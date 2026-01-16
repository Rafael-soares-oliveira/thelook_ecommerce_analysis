FROM pgvector/pgvector:pg18

USER root

# Instala PostGIS para Postgres 18
# clean e rm -rf garante que não sobrem arquivos temporários aumentando a imagem
RUN apt-get update && \
    apt-get install -y postgresql-18-postgis-3 postgresql-18-postgis-3-scripts && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
