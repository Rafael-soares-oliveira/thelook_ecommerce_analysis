import logging
from typing import cast
from unittest.mock import MagicMock

import pytest
from kedro.io import DataCatalog
from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node
from pytest_mock import MockerFixture

from thelook_ecommerce_analysis.hooks import ResourceMonitoringHook


# Fixtures
@pytest.fixture
def hook() -> ResourceMonitoringHook:
    """Retorna uma instância limpa do Hook."""
    return ResourceMonitoringHook()


@pytest.fixture
def mock_node() -> Node:
    """Cria um Node falso do Kedro."""
    node = MagicMock(spec=Node)
    node.name = "test_node"
    return node


@pytest.fixture
def mock_catalog() -> DataCatalog:
    """Cria um DataCatalog falso."""
    return MagicMock(spec=DataCatalog)


@pytest.fixture
def mock_pipeline() -> Pipeline:
    """Cria um Pipeline falso."""
    return MagicMock(spec=Pipeline)


# Testes
def test_hook_initialization_defaults(hook: ResourceMonitoringHook):
    """Veerifica se o hook inicia com valores seguros."""
    assert hook._memory_threshold == 1000  # Valor default


def test_before_pipeline_run_loads_parameters(
    hook: ResourceMonitoringHook,
    mock_catalog: DataCatalog,
    mock_pipeline: Pipeline,
    caplog: pytest.LogCaptureFixture,
):
    """Testa se o hook lê corretamente o parameters.yml do catálogo e atualiza o limite de memória."""
    mock_params = {"monitoring": {"memory_alert_threshold_mb": 500}}
    cast("MagicMock", mock_catalog.load).return_value = mock_params

    with caplog.at_level(logging.INFO):
        hook.before_pipeline_run(
            run_params={"pipeline_name": "test_pipe"},
            pipeline=mock_pipeline,
            catalog=mock_catalog,
        )

    # Verificações
    assert hook._memory_threshold == 500
    assert "Configuração de Monitoramento carregada" in caplog.text
    assert "500MB" in caplog.text


def test_before_pipeline_run_handles_missing_params(
    hook: ResourceMonitoringHook,
    mock_catalog: DataCatalog,
    mock_pipeline: Pipeline,
    caplog: pytest.LogCaptureFixture,
):
    """Testa se o hook falha graciosamente (mantém default) se não conseguir ler parâmetros."""
    # Simula erro ao carregar parâmetros
    cast("MagicMock", mock_catalog.load).side_effect = Exception("Dataset not found")

    with caplog.at_level(logging.WARNING):
        hook.before_pipeline_run({}, mock_pipeline, mock_catalog)

    # Verificações
    assert hook._memory_threshold == 1000  # Default
    assert "Não foi possível ler parâmetros" in caplog.text


# Testes de Monitoramento de Nodes
def test_node_execution_logging_normal(
    hook: ResourceMonitoringHook,
    mock_node: Node,
    mocker: MockerFixture,
    caplog: pytest.LogCaptureFixture,
):
    """Simula uma execução de nó com consumo de memória normal."""
    # 1. Mock do Tempo (Start=100s, End=105s -> Duração=5s)
    mocker.patch("time.time", side_effect=[100.0, 105.0])

    # 2. Mock da Memória
    mb = 1024 * 1024
    mock_process = mocker.patch("psutil.Process")
    mock_process.return_value.memory_info.side_effect = [
        MagicMock(rss=100 * mb),  # before_node_run
        MagicMock(rss=150 * mb),  # after_node_run
    ]

    # Execução
    with caplog.at_level(logging.INFO):
        hook.before_node_run(mock_node)
        hook.after_node_run(mock_node, {}, {})

    # Verificação da Mensagem de Log
    log_msg = caplog.text
    assert "test_node" in log_msg  # Nome Node
    assert "5.00s" in log_msg  # Duração
    assert "150.0MB" in log_msg  # Memória Final
    assert "50.0MB" in log_msg  # Delta Memória
    assert "HIGH MEMORY" not in log_msg  # Sem alerta


def test_node_execution_logging_high_memory(
    hook: ResourceMonitoringHook,
    mock_node: Node,
    mocker: MockerFixture,
    caplog: pytest.LogCaptureFixture,
):
    """Simula uma execução de nó com consumo de memória ALTO."""
    # Define threshold baixo para forçar o alerta
    hook._memory_threshold = 200

    # 1. Mock do Tempo
    mocker.patch("time.time", side_effect=[100.0, 101.0])

    # 2. Mock da Memória
    # Start=100MB, End=500MB -> Delta=400MB (Acima do threshold)
    mb = 1024 * 1024
    mock_process = mocker.patch("psutil.Process")
    mock_process.return_value.memory_info.side_effect = [
        MagicMock(rss=100 * mb),
        MagicMock(rss=500 * mb),
    ]

    # Execução
    with caplog.at_level(logging.INFO):
        hook.before_node_run(mock_node)
        hook.after_node_run(mock_node, {}, {})

    # Verificação
    assert "HIGH MEMORY" in caplog.text


# Teste de Erro
def test_on_pipeline_error_logs_details(
    hook: ResourceMonitoringHook,
    mock_pipeline: Pipeline,
    mock_catalog: DataCatalog,
    mocker: MockerFixture,
    caplog: pytest.LogCaptureFixture,
):
    """Verifica se erros fatais são logados com o tempo de execução."""
    mocker.patch("time.time", side_effect=[100.0, 120.0])

    # Simula início para setar o start_time
    hook.before_pipeline_run({}, mock_pipeline, mock_catalog)

    with caplog.at_level(logging.ERROR):
        hook.on_pipeline_error(
            ValueError("Erro simulado"), {}, mock_pipeline, mock_catalog
        )

    assert "FALHA CRÍTICA" in caplog.text
    assert "20.00s" in caplog.text
    assert "Erro simulado" in caplog.text
