from kedro.config import OmegaConfigLoader
from kedro.framework.project import settings


def get_params(param: str) -> dict:
    """
    Helper interno para carregar a configuração de parameters.yml antes do pipeline executar.

    Args:
        params (str): Chave do parameters.yml que deseja extrair.

    Returns:
        dict: Dicionário com os parâmetros
    """
    # Carrega configurações base e local para pegar credencias ou overrides
    conf_loader = OmegaConfigLoader(
        conf_source=settings.CONF_SOURCE, base_env="base", default_run_env="local"
    )
    params = conf_loader["parameters"]

    return params.get(param, {})
