import logging

import pytest


@pytest.fixture(autouse=True)
def force_log_propagation():
    """
    Garante que os logs do projeto propaguem para o root logger, permitindo que o 'caplog' do pytest capture as mensagens.

    A ideia é resolver o conflito causado pela inicialização do settings em outros testes.
    """
    project_logger = logging.getLogger("thelook_ecommerce_analysis")

    original_propagate = project_logger.propagate

    # Força True para o teste funcionar
    project_logger.propagate = True

    yield

    # Restaura o estado original
    project_logger.propagate = original_propagate
