# Ecommerce MLOps & GenAI Pipeline

[![Powered by Kedro](https://img.shields.io/badge/powered_by-kedro-ffc900?logo=kedro)](https://kedro.org)
[![Python](https://img.shields.io/badge/Python-3.13%2B-blue?logo=Python)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-blue?logo=Docker)](https://www.docker.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-docker-green?logo=Postgresql)](https://www.postgresql.org/)
[![Ollama](https://img.shields.io/badge/Ollama-local-green?logo=Ollama)](https://ollama.com/)
[![Ollama](https://img.shields.io/badge/theLook_eCommerce-dataset-blue?logo=GoogleCloud)](https://console.cloud.google.com/marketplace/product/bigquery-public-data/thelook-ecommerce?project=bigquery-484420)

Este projeto implementa um pipeline completo de Engenharia de Dados e Inteligência Artificial.

## Arquitetura

O sistema segue o padrão **Lakehouse** com enriquecimento semântico:

1. **Ingestão**: Extração incremental do BigQuery (`thelook_ecommerce`).
2. **Processamento**: Limpeza e modelagem de dados usando **Polars** (Lazy Execution).
3. **Vector Store**: Armazenamento de embeddings de produtos no **PostgreSQL (pgvector)**.
4. **AI Enrichment**: Geração de resumos diários usando **DeepSeek (via Ollama)**.
5. **Viz**: Dashboard interativo em **Streamlit** com Chatbot RAG.

## Tech Stack

- **Gerenciamento**: `uv` (Astral)
- **Orquestração**: Kedro + Kedro-Viz
- **Qualidade de Código**: Ruff (Lint), Ty (Typing), Pytest (Testes)
- **Banco de Dados**: PostgreSQL + pgvector
- **Modelos AI**:
  - *Embedding*: `all_MiniLM-L6-v2`
  - *SLM*: `deepseek-r1:1.5b` (via Ollama)

## Estrutura do Repositório

``` plaintext
thelook_ecommerce_analysis/
├── .github/                        # Workflows de CI/CD
├── conf/
│   └── base/
│   │   ├── catalog.yml             # Definição de DataSets
│   │   ├── logging.yml             # Configuração de Logs
│   │   └── parameters.yml          # Hiperparâmetros (Prompts, Model Names)
│   │   
│   └── local/
│       └── credentials.yml         # Credenciais. Ignorado no Git
│
├── data/                           # Ignorado no Git (apenas estrutura)
│   ├── 01_raw/
│   ├── 02_intermediate/
│   ├── 03_primary/
│   └── 04_feature/
│
├── docs/                           # Documentação adicional (arquitetura, diagramas)
├── notebooks/                      # Jupyter Notebooks para exploração
├── src/                            # Código Fonte do Kedro
│   └── thelook_ecommerce_analysis/
│       ├── pipelines/
│       │   ├── data_ingestion/
│       │   ├── transformation/
│       │   ├── ai_embeddings/
│       │   └── ai_slm_enrichment/
│       ├── settings.py
│       └── __init__.py
│
├── tests/                          # Testes Automatizados
│   └── pipelines/                  # Testes dos pipelines, dividir em pastas
│       ├── __init__.py
│       ├── test_nodes.py
│       └── test_pipelines.py
│   └── kedro_settings/
│       ├── __init__.py
│       ├── test_hooks.py           # Testes do hooks Kedro
│       └── test_settings.py        # Testes das configurações do Kedro
│
├── .dockerignore
├── .gitignore
├── docker-compose.yml       # Infraestrutura (Postgres + Ollama + Streamlit)
├── pyproject.toml           # Configuração Central (UV, Ruff, Ty, Pytest)
├── README.md                # Documentação Principal
└── uv.lock                  # Lockfile do UV (garantia de reprodutibilidade)
```

## Como Executar

### Pré-requisitos

- Docker e Docker Compose
- Python 3.13 com `uv` instalado (imagem oficial da Astral)

## Roadmap de Implementação (Planejamento)

Este planejamento foca nas entregas lógicas, sem datas fixas.

### Fase 1: Fundação & Infraestrutura

- [X] Configurar repositório Git com `.gitignore` e `pyproject.toml`.
- [ ] Criar `docker-compose.yml` com PostgreSQL (imagem `pgvector/pgvector`) e Ollama.
- [ ] Validar conexão local com o Banco de Dados.
- [ ] Baixar e testar modelos locais (Pull do DeepSeek no Ollama).

### Fase 2: Core Engineering (Kedro ETL)

- [X] Inicializar projeto Kedro (`kedro new`).
- [ ] Configurar `crendentials.yml` e `parameters.yml`.
- [ ] Registrar datasets no `catalog.yml`.
- [ ] Implementar **Pipeline de Ingestão**:
  - [ ] Conector BigQuery.
  - [ ] Lógica de Watermark (Incremental Load) lendo do Postgres.
- [ ] Implementar **Pipeline de Transformação**:
  - [ ] Limpeza com Polars (Lazy).
  - [ ] Criação de tabelas Fato/Dimensão.

### Fase 3: AI & Vetores

- [ ] Implementar **Pipeline de Embeddings**:
  - [ ] Node para gerar vetores de descrições de produtos.
  - [ ] Carga no Postgres com tipo `VECTOR`.
- [ ] Implementar **Pipeline de SLM Batch**:
  - [ ] Node que agrega métricas diárias.
  - [ ] Integração com API do Ollama para gerar resumos textuais.

### Fase 4: Consumo e Visualização

- [ ] Criar Dashboard básico no **Streamlit**.
- [ ] Conectar Streamlit ao Postgres para ler métricas.
- [ ] Implementar Chatbot RAG no Streamlit:
  - [ ] Lógica de busca semântica (Query -> Embedding -> Select Postgres).
  - [ ] Envio de contexto para o SLM.

### Fase 5: Observabilidade e Qualidade

- [ ] Configurar `conf/base/logging.yml` para salvar logs em arquivo.
- [ ] Configurar Hooks do Kedro para logging.
- [ ] Escrever testes unitários para os Nodes principais (mockando BigQuery e Ollama).
- [ ] Configurar pipeline de CI (GitHub Actions) para rodar `ruff`, `ty` e `pytest`.
