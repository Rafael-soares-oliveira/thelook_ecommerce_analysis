-- Habilita extensão de vetores
CREATE EXTENSION IF NOT EXISTS vector;

-- Habilita PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- Configurações de performance para sessão -> ajustar
ALTER SYSTEM SET work_mem = '64MB';
ALTER SYSTEM SET maintenance_work_mem = '256MB';
