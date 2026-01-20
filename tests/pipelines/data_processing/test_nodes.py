import polars as pl
import pytest

from thelook_ecommerce_analysis.pipelines.data_processing.nodes import process_table


@pytest.fixture
def dummy_lazy_df() -> pl.LazyFrame:
    """Cria um LazyFrame de exemplo para testes."""
    data = {
        "id": [1, 2, 2, 3],  # IDs duplicados para testar deduplicação
        "amount": ["10.50", "20.00", "20.00", "30.00"],
        "cat_col": [" A ", "B", "B", "C"],  # Espaços para testar Trim
        "date_col": ["2026-01-01", "invalid", "2026-01-01", "2026-01-02"],
    }

    return pl.LazyFrame(data)


def test_process_table_types(dummy_lazy_df: pl.LazyFrame):
    """Testa se os tipos do dicionário são aplicados corretamente."""
    schema = {"id": "UInt32", "amount": "Float64", "cat_col": "Categorical"}

    res = process_table(dummy_lazy_df, schema, "test").collect_schema()

    assert res["id"] == pl.UInt32
    assert res["amount"] == pl.Float64
    assert res["cat_col"] == pl.Categorical


def test_decimal_parsing(dummy_lazy_df: pl.LazyFrame):
    """Testa se interpreta Decimal corretamente."""
    schema = {
        "id": "UInt32",
        "amount": "Decimal(10, 2)",
        "cat_col": "String",
        "date_col": "Datetime",
    }

    res = process_table(dummy_lazy_df, schema, "test").collect_schema()

    # Garantir para o type hint que é um Decimal
    assert isinstance(res["amount"], pl.Decimal)
    assert res["amount"].precision == 10
    assert res["amount"].scale == 2
    assert res["cat_col"], pl.String
    assert res["date_col"], pl.Datetime


def test_invalid_type_raises_error(dummy_lazy_df: pl.LazyFrame):
    """Testa se o tipo inválido falha."""
    schema = {"id": "Floater"}

    with pytest.raises(ValueError, match="Tipo desconhecido"):
        process_table(dummy_lazy_df, schema, "test")


def test_col_name_not_in_schema_names(dummy_lazy_df: pl.LazyFrame):
    """Testa se levanta erro quando é informado no schema uma tabela que não existe."""
    schema = {"id_not": "UInt32"}

    with pytest.raises(ValueError, match="Coluna 'id_not' não encontrada"):
        process_table(dummy_lazy_df, schema, "test")


def test_if_drop_duplicate_id(dummy_lazy_df: pl.LazyFrame):
    """Testa se deduplica tabela com coluna id."""
    schema = {
        "id": "UInt32",
        "amount": "Decimal(10, 2)",
        "cat_col": "String",
        "date_col": "Datetime",
    }

    res = process_table(dummy_lazy_df, schema, "test").collect()

    assert isinstance(res, pl.DataFrame)
    assert res.height == 3
    assert res.select("id").n_unique() == 3
    assert sorted(res["id"].to_list()) == [1, 2, 3]


def test_deduplication_logic_without_id_column():
    """
    Valida: else: df_clean = df_clean.unique()
    Cenário: Tabela NÃO tem 'id'. Deve deduplicar apenas se a linha INTEIRA for igual.
    """
    # Setup:
    # Linha 1 e 2 são idênticas -> Deve virar 1 linha.
    # Linha 3 é diferente -> Mantém.
    df_raw = pl.LazyFrame({"nome": ["Ana", "Ana", "Bia"], "idade": [30, 30, 25]})

    schema = {"nome": "String", "idade": "Int64"}

    # Execução
    df_result = process_table(df_raw, schema, "test_no_id").collect()

    # Asserções
    assert df_result.height == 2  # Ana (duplicada) vira 1 + Bia = 2 linhas
    assert "Ana" in df_result["nome"]
    assert "Bia" in df_result["nome"]
