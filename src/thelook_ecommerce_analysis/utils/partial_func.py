import functools
from collections.abc import Callable
from typing import Any


def create_node_func(func: Callable, **kwargs: Any) -> Callable[..., Any]:
    """
    Cria uma função parcial (com argumentos pré-fixados), mas preserva o nome e a documentação da função original para garantir logs legíveis no Kedro.

    Args:
        func (Callable): A função original.
        **kwargs: Argumentos que queremos fixar.
    """
    # Cria a partial
    partial_func = functools.partial(func, **kwargs)

    # Copia metadados  (name, doc, module) da função original para a parcial
    functools.update_wrapper(partial_func, func)
    return partial_func
