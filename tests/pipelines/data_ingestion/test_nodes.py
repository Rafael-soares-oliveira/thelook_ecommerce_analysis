import logging
from unittest.mock import MagicMock

import polars as pl
import pytest
from pytest_mock import MockerFixture

from thelook_ecommerce_analysis.pipelines.data_ingestion.nodes import (
    extract_incremental_data,
    extract_snapshot_data,
)


@pytest.fixture
def mock_bq_client(mocker: MockerFixture) -> MagicMock:
    """
    Mock de toda a cadeia de conexão do BigQuery:
        Path -> Credentials -> Client -> QueryJob -> Arrow Table
    """
    # 1. Mock do arquivo e credenciais
    mocker.patch("pathlib.Path.exists", return_value=True)
    mocker.patch("google.oauth2.service_account.Credentials.from_service_account_file")

    # 2. Mock do Client
    mock_client_cls = mocker.patch(
        "thelook_ecommerce_analysis.pipelines.data_ingestion.nodes.bigquery.Client"
    )
    mock_client_instance = mock_client_cls.return_value

    # 3. Mock da Execução da Query
    mock_job = MagicMock()
    mock_client_instance.query_return = mock_job

    # .to_arrow() retorna Unknown, precisa garantir que será um DataFrame
    mock_job.to_arrow().return_value = "pyarrow_table.fake"

    # 4. Mock do Polars
    mock_polars_df = MagicMock(spec=pl.DataFrame)
    mock_polars_df.height = 100

    mocker.patch(
        "thelook_ecommerce_analysis.pipelines.data_ingestion.nodes.pl.from_arrow",
        return_value=mock_polars_df,
    )

    return mock_client_instance


# Testes de Segurança
def test_validation_rejects_bad_table_name_incremental(mock_bq_client: MagicMock):
    """Testa a validação de regex."""
    with pytest.raises(ValueError, match="Nome de tabela inválido."):
        extract_incremental_data(
            'orders"; DROP --', "date_col", "key.json", "2025-01-01"
        )

    mock_bq_client.query.assert_not_called()


def test_validation_rejects_bad_table_name_snapshot(mock_bq_client: MagicMock):
    """Testa a validação de regex."""
    with pytest.raises(ValueError, match="Nome de tabela inválido."):
        extract_snapshot_data('product"; DROP --', "key.json")

    mock_bq_client.query.assert_not_called()


def test_incremental_query_parameterization_security(mock_bq_client: MagicMock):
    """Testa se os valores (datas) estão sendo passados via parâmetros do BigQuery e não concatenando na string."""
    input_date = "2025-01-01"

    extract_incremental_data(
        table_name="orders",
        date_col="created_at",
        key_filepath="dummy.json",
        start_date=input_date,
        lookback_days=2,
    )

    # Recuperar os argumentos passados para o client.query(sql, job_config)
    args, kwargs = mock_bq_client.query.call_args
    sql_generated = args[0]
    job_config = kwargs["job_config"]

    # 1. Validação da String SQL
    # Deve conter o placeholder @start_date
    assert "@start_date" in sql_generated

    # Não deve conter o valor literal "2025-01-01"
    assert input_date not in sql_generated, "Valor injetado diretamente na string SQL"

    # 2. Validação dos Parâmetros
    # Extrai os parâmetros configurados no job
    params = {p.name: p.value for p in job_config.query_parameters}

    assert "start_date" in params
    assert params["start_date"] == input_date
    assert params["start_date"] != f"'{input_date}'"


def test_incremental_column_identifier_safety(mock_bq_client: MagicMock):
    """Testa a segurança no nome da coluna."""
    malicious_col = "created_at OR 1=1"

    extract_incremental_data(
        table_name="orders",
        date_col=malicious_col,
        key_filepath="dummy.json",
        start_date="2025-01-01",
    )

    args, _ = mock_bq_client.query.call_args
    assert "@start_date" in args[0]


def test_extract_missing_key_file(mocker: MockerFixture):
    """Testa se apresenta erro quando não fornecido o diretório do JSON com as credenciais."""
    mocker.patch("pathlib.Path.exists", return_value=False)
    with pytest.raises(FileNotFoundError, match="Chave GCP não encontrada"):
        extract_incremental_data("orders", "col", "fake.json", "2025-01-01")


def test_conversion_series_to_dataframe(
    mock_bq_client: MagicMock, caplog: pytest.LogCaptureFixture
):
    """Testa se converte de Series para DataFrame, quando necessário."""
    # 1. Mock um Series
    mock_series = MagicMock(spec=pl.Series)
    mock_series.to_frame.return_value = MagicMock(spec=pl.DataFrame, height=50)

    # 2. Injeta este mock específico
    with pytest.MonkeyPatch().context() as mp:
        mp.setattr(
            "thelook_ecommerce_analysis.pipelines.data_ingestion.nodes.pl.from_arrow",
            lambda x: mock_series,
        )

        with caplog.at_level(logging.WARNING):
            df = extract_incremental_data("orders", "date", "k.json", "2025-01-01")  # noqa: F841

            mock_series.to_frame.assert_called_once()
            assert "A extração retornou uma Series" in caplog.text


def test_snapshot_fails_fast_if_too_big(
    mock_bq_client: MagicMock, caplog: pytest.LogCaptureFixture
):
    """Testa se a função aborta e levanta erro se o COUNT(*) > safety_limit."""
    limit = 1_000
    actual_size = 5_000

    mock_count_job = MagicMock()
    mock_count_job.result.return_value = [
        [actual_size]
    ]  # Lista de list/tupla simulando Row

    mock_bq_client.query.side_effect = [mock_count_job]

    # Verifica se levanta erro
    with pytest.raises(ValueError, match=f"muito grande.*{actual_size}"):
        extract_snapshot_data("products", "key.json", safety_limit=limit)

    # Verifica logs
    assert f"possui {actual_size} linhas" in caplog.text

    # Garante que executou COUNT(*) apenas uma vez
    assert mock_bq_client.query.call_count == 1

    # Verifica se a query executada é apenas COUNT
    assert "count" in mock_bq_client.query.call_args[0][0]


def test_snapshot_if_size_ok(
    mock_bq_client: MagicMock, caplog: pytest.LogCaptureFixture
):
    """Testa se a função executa o Download se o COUNT(*) <= safety_limit."""
    limit = 1_000
    actual_size = 500

    # 1. Job de Count
    mock_count_job = MagicMock()
    mock_count_job.result.return_value = [[actual_size]]

    # 2. Job de Download (Fetch)
    mock_fetch_job = MagicMock()
    mock_fetch_job.to_arrow.return_value = MagicMock()

    mock_bq_client.query.side_effect = [mock_count_job, mock_fetch_job]

    with caplog.at_level(logging.INFO):
        extract_snapshot_data("products", "key.json", safety_limit=limit)

    # Deve ter chamado Count e Fetch
    assert mock_bq_client.query.call_count == 2

    args_list = mock_bq_client.query.call_args_list
    assert "count" in args_list[0][0][0]
    assert "SELECT" in args_list[1][0][0]
