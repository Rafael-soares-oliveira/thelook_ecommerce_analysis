from thelook_ecommerce_analysis import settings
from thelook_ecommerce_analysis.hooks import ResourceMonitoringHook


def test_hooks_registration():
    """Garante que o Hook de Monitoramento de Recursos está registrado."""
    hooks = settings.HOOKS

    # Verifica se é uma tupla ou lista
    assert isinstance(hooks, (list, tuple)), "HOOKS deve ser uma lista ou tupla"

    # Verifica se o hook está registrado
    has_monitoring_hook = any(isinstance(h, ResourceMonitoringHook) for h in hooks)
    assert has_monitoring_hook, "O ResourceMonitoringHook não está registrado em HOOKS"


def test_config_loader_args_structure():
    """Valida o 'CONFIG_LOADER_ARGS'. Garante que o projeto sempre busca configs em 'base' e 'local' por padrão."""
    config_args = settings.CONFIG_LOADER_ARGS

    assert isinstance(config_args, dict), "CONFIG_LOADER_ARGS deve ser um dicionário."

    # 1. Validação de Integridade
    assert config_args.get("base_env") == "base", (
        "O ambiente base deve ser 'base' para garantir acesso ao catalog/parameters."
    )

    # 2. Validação de Ambiente de Execução (local). Verifica se acessa conf/local
    assert config_args.get("default_run_env") == "local", (
        "O ambiente padrão deve ser 'local' para carregar credentials.yml corretamente."
    )
