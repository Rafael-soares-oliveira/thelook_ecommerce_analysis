from typing import Any

import pytest
from kedro.pipeline import Pipeline
from pytest_mock import MockerFixture

from thelook_ecommerce_analysis.pipelines.data_ingestion import create_pipeline

# Mock da configuração que vem do parameters.yml
MOCK_INGESTION_CONFIG = {
    "incremental_tables": {"orders": "created_at", "users": "created_at"},
    "snapshot_tables": ["products", "distribution_centers"],
    "gcp_service_account": "path/to/key.json",
    "start_date": "2025-01-01",
}


@pytest.fixture
def mock_get_params(mocker: MockerFixture) -> dict[str, Any]:
    """Substitui a função get_params para retornar dicionário de testes em vez de ler o arquivo YAML real."""
    return mocker.patch(
        "thelook_ecommerce_analysis.pipelines.data_ingestion.pipeline.get_params",
        return_value=MOCK_INGESTION_CONFIG,
    )


def test_pipeline_structure_generation(mock_get_params: dict[str, Any]):
    """Testa se o pipeline gera o número correto de nós baseados na config."""
    pipeline = create_pipeline()

    assert isinstance(pipeline, Pipeline)

    # Verifica se há 4 nodes (2 incremental + 2 snapshot)
    assert len(pipeline.nodes) == 4


def test_incremental_node_attributes(mock_get_params: dict[str, Any]):
    """Testa se os nós incrementais estão com os inputs/ouputs corretos."""
    pipeline = create_pipeline()

    # Pegar o node 'orders'
    orders_node = next(
        node for node in pipeline.nodes if node.name == "extract_orders_node"
    )

    # 1. Verifica Inputs
    # Utiliza _inputs para acessar o dicionário original e não a conversão para lista
    assert (
        orders_node._inputs["date_col"] == "params:ingestion.incremental_tables.orders"
    )
    assert orders_node._inputs["start_date"] == "params:ingestion.start_date"

    # Verifica Output
    assert orders_node.outputs == ["ingestion_raw_orders"]

    # 3, Verifica Tags
    assert "incremental" in orders_node.tags
    assert "orders" in orders_node.tags

    # 4. Verifica se a função correta está sendo usada, preservada com create_node_func
    assert orders_node.func.__name__ == "extract_incremental_data"


def test_pipeline_empty_config(mocker: MockerFixture):
    """Testa se o pipeline lida bem com configurações vazias ou parciais."""
    partial_config = {
        "snapshot_tables": ["products"],
    }

    mocker.patch(
        "thelook_ecommerce_analysis.pipelines.data_ingestion.pipeline.get_params",
        return_value=partial_config,
    )

    pipeline = create_pipeline()

    assert len(pipeline.nodes) == 1
    assert pipeline.nodes[0].name == "extract_products_node"


def test_pipeline_missing_keys_raises_error(mocker: MockerFixture):
    """Testa se ocorre erro se chaves obrigatórias faltarem."""
    mocker.patch(
        "thelook_ecommerce_analysis.pipelines.data_ingestion.pipeline.get_params",
        return_value={},
    )

    with pytest.raises(KeyError):
        create_pipeline()
