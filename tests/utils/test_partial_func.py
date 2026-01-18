import functools

from thelook_ecommerce_analysis.utils.partial_func import create_node_func


def dummy_function(a: int, b: int) -> int:
    """Docstring original da função."""
    return a + b


def test_create_node_func_execution():
    """Testa se a função parcial executa a lógica corretamente fixando um argumento."""
    partial = create_node_func(dummy_function, a=10)

    assert isinstance(partial, functools.partial)

    result = partial(b=5)

    assert result == 15


def test_create_node_func_preserves_metadata():
    """Testa se o nome e a docstring foram preservados."""
    partial = create_node_func(dummy_function, a=10)

    assert isinstance(partial, functools.partial)

    assert partial.__name__ == "dummy_function"  # type: ignore
    assert partial.__doc__ == "Docstring original da função."


def test_create_node_fun_overrides():
    """Testa se ainda é possível sobrescrever o argumento fixado se necessário."""
    partial = create_node_func(dummy_function, a=10)

    result = partial(a=20, b=5)
    assert result == 25
