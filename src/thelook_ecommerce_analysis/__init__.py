"""thelook_ecommerce_analysis"""

__version__ = "0.1"

import os

# Aplicado para a tabela 'distribution_datacenters' nas colunas 'latitude' e 'longitude'
os.environ["POLARS_UNKNOWN_EXTENSION_TYPE_BEHAVIOR"] = "load_as_storage"
