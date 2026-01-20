import pytest
from kedro.pipeline import Pipeline
from pytest_mock import MockerFixture

from thelook_ecommerce_analysis.pipelines.data_processing.pipeline import (
    create_pipeline,
)

# Configuração simulada do parameters.yml
MOCK_PROCESSING_CONFIG = {
    "schemas": {
        "orders": {"id": "UInt32"},
        "products": {"id": "UInt32", "price": "Decimal(10, 2)"},
    }
}

# Definir tipo de MOCK_PROCESSING_CONFIG
type Params = dict[str, dict[str, str]]


@pytest.fixture
def mock_get_params(mocker: MockerFixture) -> Params:
    """Mock do get_params para não ler arquivos reais."""
    return mocker.patch(
        "thelook_ecommerce_analysis.pipelines.data_processing.pipeline.get_params",
        return_value=MOCK_PROCESSING_CONFIG,
    )


def test_pipeline_structure(mock_get_params: Params):
    """Verifica se cria 1 nó para cada tabela no schema."""
    pipeline = create_pipeline()

    assert isinstance(pipeline, Pipeline)
    assert len(pipeline.nodes) == 2


def test_node_naming_convention(mock_get_params: Params):
    """Testa os inputs, outputs e tags dos nós gerados."""
    pipeline = create_pipeline()

    orders_node = next(n for n in pipeline.nodes if n.name == "process_orders_node")

    # Verifica Inputs
    assert orders_node._inputs["df"] == "ingestion_raw_orders"
    assert orders_node._inputs["target_schema"] == "params:processing.schemas.orders"

    # Verifica Outputs
    assert orders_node.outputs == ["processing_intermediate_orders"]

    # Verifica Tags
    assert "processing" in orders_node.tags
    assert "orders" in orders_node.tags
