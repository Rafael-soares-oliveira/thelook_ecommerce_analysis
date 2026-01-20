from kedro.pipeline import Node, Pipeline

from thelook_ecommerce_analysis.pipelines.data_processing.nodes import process_table
from thelook_ecommerce_analysis.utils.get_params import get_params
from thelook_ecommerce_analysis.utils.partial_func import create_node_func


def create_pipeline(**kwargs) -> Pipeline:
    # 1. Obter o parâmetro 'data_processing'
    config = get_params("processing")

    # 2. Extrair o nome das tabelas
    tables = list(config.get("schemas", {}).keys())

    nodes = []

    # 3. Pipeline Factory
    for table in tables:
        nodes.append(
            Node(
                func=create_node_func(process_table, table_name=table),
                inputs={
                    "df": f"ingestion_raw_{table}",
                    "target_schema": f"params:processing.schemas.{table}",
                },
                outputs=f"processing_intermediate_{table}",
                name=f"process_{table}_node",
                tags=["processing", table],
            )
        )

    return Pipeline(nodes)
