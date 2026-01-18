"""
This is a boilerplate pipeline 'data_ingestion'
generated using Kedro 1.1.1
"""

from kedro.pipeline import Node, Pipeline

from thelook_ecommerce_analysis.pipelines.data_ingestion.nodes import (
    extract_incremental_data,
    extract_snapshot_data,
)
from thelook_ecommerce_analysis.utils.get_params import get_params
from thelook_ecommerce_analysis.utils.partial_func import create_node_func


def create_pipeline(**kwargs) -> Pipeline:
    # 1. Leitura Dinâmica
    config = get_params("ingestion")

    # Extrai as chaves do dicionário
    incremental_tables = list(config.get("incremental_tables", {}).keys())
    snapshot_tables: list[str] = config["snapshot_tables"]

    nodes = []

    # 2. Pipeline Factory
    # 2.1 Tabelas Incrementais
    for table in incremental_tables:
        nodes.append(
            Node(
                func=create_node_func(extract_incremental_data, table_name=table),
                inputs={
                    "date_col": f"params:ingestion.incremental_tables.{table}",
                    "key_filepath": "params:ingestion.gcp_service_account",
                    "start_date": "params:ingestion.start_date",
                },
                outputs=f"ingestion_raw_{table}",
                name=f"extract_{table}_node",
                tags=["ingestion", "incremental", table],
            )
        )

    # 2.1 Tabelas Incrementais
    for table in snapshot_tables:
        nodes.append(
            Node(
                func=create_node_func(extract_snapshot_data, table_name=table),
                inputs={
                    "key_filepath": "params:ingestion.gcp_service_account",
                    "safety_lmit": "params:ingestion.safety_limit",
                },
                outputs=f"ingestion_raw_{table}",
                name=f"extract_{table}_node",
                tags=["ingestion", "snapshot", table],
            )
        )

    return Pipeline(nodes)
