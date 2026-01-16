from collections.abc import Generator
from pathlib import Path
from typing import Any

import psycopg
import pytest
import yaml
from psycopg import Connection


# Fixtures
@pytest.fixture(scope="module")
def db_credentials() -> dict[str, Any]:
    """
    Carrega as credenciais do arquivo local. Pula os testes se o arquivo não existir.
    """
    creds_path = Path("conf/local/credentials.yml")
    if not creds_path.exists():
        pytest.skip("Arquivo conf/local/credentials.yml não encontrado.")

    with open(creds_path) as f:
        data = yaml.safe_load(f)

    # Valida se a chave existe
    if "postgres" not in data:
        pytest.fail("Chave 'postgres' não encontrada em credentials.yml")

    return data["postgres"]


@pytest.fixture(scope="module")
def db_connection(db_credentials: dict[str, Any]) -> Generator[Connection]:
    """Abre uma conexão com o banco e a encerra automaticamente após os testes."""
    # Monta conn string (formato libpq)
    conn_info = (
        f"host={db_credentials['host']} "
        f"port={db_credentials['port']} "
        f"dbname={db_credentials['dbname']} "
        f"user={db_credentials['user']} "
        f"password={db_credentials['password']}"
    )

    with psycopg.connect(conn_info) as conn:
        yield conn


# Testes
def test_database_connection_is_alive(db_connection: Connection):
    """Teste básico de conectividade."""
    try:
        with db_connection.cursor() as cur:
            cur.execute("SELECT 1;")
            row = cur.fetchone()

            assert row is not None, "A query retornou vazio, esperava (1,)"
            assert row[0] == 1

    except Exception as e:
        pytest.fail(f"Erro ao executar query simples: {e}")
