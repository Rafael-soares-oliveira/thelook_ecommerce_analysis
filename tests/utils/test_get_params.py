from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from thelook_ecommerce_analysis.utils.get_params import get_params

# Simula a estrutura de um parameters.yml
MOCK_FULL_PARAMS = {
    "ingestion": {"start_date": "2025-01-01", "tables": ["orders", "users"]},
    "processing": {"layer": "intermediate"},
}


@pytest.fixture
def mock_config_loader(mocker: MockerFixture) -> MagicMock:
    """Mock do OmegaConfigLoader para não precisar ler arquivos."""
    # 1. Mock do settings
    mocker.patch(
        "thelook_ecommerce_analysis.utils.get_params.settings", CONF_SOURCE="conf"
    )

    # 2. Mock da Classe OmegaConfigLoader
    mock_cls = mocker.patch(
        "thelook_ecommerce_analysis.utils.get_params.OmegaConfigLoader"
    )

    # 3. Configura a instância que a classe retorna
    mock_instance = mock_cls.return_value
    mock_instance.__getitem__.side_effect = (
        lambda key: MOCK_FULL_PARAMS if key == "parameters" else {}
    )

    return mock_cls


def test_get_params_returns_correct_section(mock_config_loader: MagicMock):
    """Testa se a função extrai apenas a sub-seção solicitada."""
    result = get_params("ingestion")

    # Validação
    assert result["start_date"] == "2025-01-01"
    assert "tables" in result
    assert "processing" not in result


def test_get_params_returns_empty_dict_if_missing(
    mock_config_loader: MagicMock,
):
    """Testa o comportamento de fallback se a chave não existir."""
    result = get_params("not_exist")

    # Validação
    assert result == {}
    assert isinstance(result, dict)


def test_get_params_initializes_loader_correctly(mock_config_loader: MagicMock):
    """Testa se o OmegaConfigLoader foi instanciado com os argumentos certos (base_env='base', default_run_env='local')."""
    params = get_params("ingestion")  # noqa

    # Verifica a chamada do construtor
    mock_config_loader.assert_called_once_with(
        conf_source="conf", base_env="base", default_run_env="local"
    )
