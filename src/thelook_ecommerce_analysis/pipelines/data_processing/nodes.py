import logging
from typing import cast

import polars as pl

logger = logging.getLogger(__name__)

TYPE_MAPPING = {
    # Inteiros
    "UInt8": pl.UInt8,
    "UInt16": pl.UInt16,
    "UInt32": pl.UInt32,
    "UInt64": pl.UInt64,
    "Int8": pl.Int8,
    "Int16": pl.Int16,
    "Int32": pl.Int32,
    "Int64": pl.Int64,
    # Flutuantes
    "Float32": pl.Float32,
    "Float64": pl.Float64,
    # Textos e Categorias
    "String": pl.String,
    "Categorical": pl.Categorical,
    # Tempo
    "Date": pl.Date,
    "Datetime": pl.Datetime,  # Padrão (microssegundos)
    "Boolean": pl.Boolean,
}


def _get_polars_type(type_str: str) -> pl.DataType:
    """
    Resolve a string do YAML para um tipo Polars usando mapeamento direto.

    Args:
        type_str (str): String do tipo Polars esperado.

    Returns:
        pl.DataType: Tipo Polars convertido.
    """
    clean_str = type_str.strip()

    # 1. Busca Direta
    if clean_str in TYPE_MAPPING:
        return cast("pl.DataType", TYPE_MAPPING[clean_str])

    # 2. Caso Especial: Quando há argumentos
    if clean_str.startswith("Decimal"):
        try:
            content = clean_str.split("(")[1].split(")")[0]
            precision, scale = map(int, content.split(","))

            # Validação
            if precision < scale:
                msg = (
                    f"Precisão ({precision}) não pode ser menor que a Escala ({scale})."
                )
                logger.error(msg)
                raise ValueError(msg)

            return pl.Decimal(precision, scale)
        except Exception as e:
            logger.error(f"Erro ao parsear Decimal '{clean_str}' : {e}")
            raise ValueError(
                f"Decimal inválido: {clean_str}. Use formato 'Decimal(P, S)'."
            ) from e

    raise ValueError(f"Tipo desconhecido ou não permitido: '{clean_str}'.")


def process_table(
    df: pl.LazyFrame, target_schema: dict[str, str], table_name: str
) -> pl.LazyFrame:
    """
    Aplica limpeza e tipagem baseada em schema externo.

    Args:
        df (pl.LazyFrame): LazyFrame da camada Raw.
        target_schema (dict[str, str]): Dicionário do parameters.yml contendo o schema.
        table_name (str): Nome da tabela.

    Returns:
        pl.LazyFrame: Dataset processado.

    Raises:
        ValueError: Levanta erro se a coluna esperada no schema não for encontrada na tabela.
    """
    logger.info(f"Processando '{table_name}'...")

    expressions = []

    for col_name, type_str in target_schema.items():
        # 1. Validação de Existência (Fail Fast)
        if col_name not in df.collect_schema().names():
            msg = f"SCHEMA ERROR: Coluna '{col_name}' não encontrada na tabela '{table_name}'."
            logger.error(msg)
            raise ValueError(msg)

        # 2. Resolução do Tipo
        try:
            dtype = _get_polars_type(type_str)
        except ValueError as e:
            logger.error(f"Configuração inválida em '{table_name}.{col_name}': {e}")
            raise e

        # 3. Construção da Expressão
        expr = pl.col(col_name)

        # Lógicas específicas de Cast
        if dtype == pl.Categorical:
            expr = expr.str.strip_chars().cast(dtype)

        # O Polars trata Datetime diferente dependendo da entrada, strict=False é seguro
        elif isinstance(dtype, pl.Datetime) or dtype == pl.Datetime:
            expr = expr.cast(dtype, strict=False)

        else:
            expr = expr.cast(dtype)

        expressions.append(expr)

    # 4. Projeção e Deduplicação
    df_clean = df.select(expressions)

    if "id" in df.collect_schema().names():
        df_clean = df_clean.unique(subset=["id"], keep="any")
    else:
        df_clean = df_clean.unique()

    return df_clean
