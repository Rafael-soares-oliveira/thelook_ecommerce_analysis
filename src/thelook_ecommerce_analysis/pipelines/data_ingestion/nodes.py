import logging
import re
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import sqlalchemy as sa
from google.cloud import bigquery
from google.oauth2 import service_account

logger = logging.getLogger(__name__)


def _get_bq_client(key_filepath: str) -> bigquery.Client:
    """
    Registrar as credenciais no BigQuery Client.

    Args:
        key_filepath (str): Diretório onde está o arquivo JSON com as credenciais.

    Returns:
        bigquery.Client: Cliente com as credenciais
    """
    key_path = Path(key_filepath)
    if not key_path.exists():
        raise FileNotFoundError(f"Chave GCP não encontrada: {key_path}.")

    creds = service_account.Credentials.from_service_account_file(key_path)
    return bigquery.Client(credentials=creds, project=creds.project_id)


def _validate_table_name(table_name: str):
    """Validação de Segurança. Impede SQL Injection rejeitando nomes com caracteres especiais."""
    if not re.fullmatch(r"^[a-zA-Z0-9_]+$", table_name):
        raise ValueError(
            f"Nome de tabela inválido/inseguro: '{table_name}'. "
            "Use apenas letras, números e sublinhados."
        )


# Node 1: Extração Incremental
def extract_incremental_data(
    table_name: str,
    date_col: str,
    key_filepath: str,
    start_date: str,
    lookback_days: int = 2,
) -> pl.DataFrame:
    """
    Extrai apenas o delta de dados baseado em um janela de tempo.

    Args:
        table_name (str): Nome da tabela no BigQuery.
        date_col (str): Nome da coluna de data que será utilizado como filtro.
        key_filepath (str): Diretório onde está o arquivo JSON com as credenciais do GCP.
        start_date (str): Data de ínício dos dados extraídos.
        lookback_days (int): Define o limite da data para hoje - lookback_days.

    Returns:
        pl.DataFrame: DataFrame com dados extraídos
    """
    # Validar table_name
    _validate_table_name(table_name)

    client = _get_bq_client(key_filepath)

    # Lógica Temporal
    today = datetime.now()
    end_date = (today - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    logger.info(f"Ingestão Incremental: '{table_name}' | {start_date} -> {end_date}")

    # Nome da tabela completo
    full_table_id = f"`bigquery-public-data.thelook_ecommerce.{table_name}`"

    # SELECT * FROM table
    stmt = sa.select(sa.literal_column("*")).select_from(sa.text(full_table_id))

    # Criamos um objeto Coluna para usar o WHERE
    target_col = sa.column(date_col)

    # 2. Construção da query
    # Adiciona filtros
    stmt = stmt.where(target_col >= sa.text("@start_date"))
    stmt = stmt.where(target_col < sa.text("@end_date"))

    # 3. Compilação
    # Compilamos o objeto para string
    query_str = str(stmt.compile(compile_kwargs={"literal_binds": True}))

    # 4. Execução
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ]
    )

    try:
        job = client.query(query_str, job_config=job_config)
        arrow_table = job.to_arrow()
        df = pl.from_arrow(arrow_table)

        if isinstance(df, pl.Series):
            logger.warning(
                "A extração retornou uma Series. Convertendo para DataFrame."
            )
            df = df.to_frame()

        logger.info(f"Incremental '{table_name}': {df.height} linhas.")
        return df
    except Exception as e:
        logger.error(f"Erro SQL Gerado: {query_str}")
        logger.error(f"Falha: {e}")
        raise e


def extract_snapshot_data(
    table_name: str, key_filepath: str, safety_limit: int = 100_000
) -> pl.DataFrame:
    """
    Extrai toda a tabela quando a tabela não é temporal.

    Args:
        table_name (str): Nome da tabela no BigQuery.
        key_filepath (str): Diretório onde está o arquivo JSON com as credenciais do GCP.
        safety_limit (int): Quantidade máxima de linhas esperada.

    Returns:
        pl.DataFrame: DataFrame com dados extraídos
    """
    # Valida table_name
    _validate_table_name(table_name)

    client = _get_bq_client(key_filepath)
    logger.info(f"Ingestão Snapshot: '{table_name}'")

    full_table_id = f"`bigquery-public-data.thelook_ecommerce.{table_name}`"

    # 1. Verificar tamanho da tabela
    count_stmt = sa.select(sa.func.count()).select_from(sa.text(full_table_id))
    count_sql = str(count_stmt.compile(compile_kwargs={"literal_binds": True}))

    try:
        count_job = client.query(count_sql)
        total_rows = list(count_job.result())[0][0]

        logger.info(f"Tabela '{table_name}' possui {total_rows} linhas.")

        if total_rows > safety_limit:
            raise ValueError(
                f"Tabela '{table_name}' é muito grande para Snapshot ({total_rows}). "
                f"Limite de segurança: {safety_limit}. Use ingestão incremental se for tabela temporal ou aumente o limite."
            )

    except Exception as e:
        logger.error(f"Falha ao contar linhas de '{table_name}': {e}")
        raise e

    # 2. Extração REAL
    logger.info(f"Volume seguro. Iniciando download de '{table_name}'...")

    stmt = sa.select(sa.literal_column("*")).select_from(sa.text(full_table_id))

    query_str = str(stmt.compile(compile_kwargs={"literal_binds": True}))

    try:
        job = client.query(query_str)
        arrow_table = job.to_arrow()
        df = pl.from_arrow(arrow_table)

        if isinstance(df, pl.Series):
            logger.warning(
                "A extração retornou uma Series. Convertendo para DataFrame."
            )
            df = df.to_frame()

        logger.info(f"Snapshot '{table_name}': {df.height} linhas.")

        return df

    except Exception as e:
        logger.error(f"Falha Snapshot: {e}")
        raise e
