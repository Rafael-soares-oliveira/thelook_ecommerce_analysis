# Ecommerce MLOps & GenAI Pipeline

[![Powered by Kedro](https://img.shields.io/badge/powered_by-kedro-ffc900?logo=kedro)](https://kedro.org)
[![Python](https://img.shields.io/badge/Python-3.13%2B-blue?logo=Python)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-blue?logo=Docker)](https://www.docker.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-docker-green?logo=Postgresql)](https://www.postgresql.org/)
[![Ollama](https://img.shields.io/badge/Ollama-local-green?logo=Ollama)](https://ollama.com/)
[![Ollama](https://img.shields.io/badge/theLook_eCommerce-dataset-blue?logo=GoogleCloud)](https://console.cloud.google.com/marketplace/product/bigquery-public-data/thelook-ecommerce?project=bigquery-484420)

Este projeto implementa um pipeline completo de Engenharia de Dados e Inteligência Artificial.

## Destaque da Implementação Atual

### Ingestão (Raw)

* **Performance**: Uso da **BigQuery Storage API** (stream Arrow) reduzindo IO em 50%.
* **Segurança**: Blindagem contra SQL Injection via sanitização de identificadores e parametrização.
* **Fail Fast**: Verificação de volume (`COUNT`) antes de downloads snapshot.

### Processamento (Intermediate)

* **Metadata Driven Transformation**: Lógica de limpeza 100% controlada via `parameters.yml`. O código é genérico, as regras são configuráveis.
* **Tipagem Estrita & Financeira**:
  * Suporte a tipos complexos como `Decimal(P, S)` para precisão monetária, evitando erros de ponto flutuante.
  * Otimização de memória com conversão automática para `Categorical`.
* **Resiliência**:
  * **Schema Check**: O pipeline aborta imediatamente se uma coluna esperada não existir a origem.
  * **Deduplicação**: Remove duplicatas baseadas em ID (se existir) ou na linha completa.
* **Lazy Execution**: Utiliza `polars.LazyFrame` para otimização de plano de execução, só materializando dados na escrita.

## Arquitetura

O sistema segue o padrão **Lakehouse** com enriquecimento semântico:

1. **Ingestion**: Extração incremental do BigQuery (`thelook_ecommerce`).
2. **Processing**: Limpeza e modelagem de dados usando **Polars** (Lazy Execution).
3. **Loading**: Injestão dos dados no PostgreSQL.
3. **Vector Store**: Armazenamento de embeddings de produtos no **PostgreSQL (pgvector)**.
4. **AI Enrichment**: Geração de resumos diários usando **DeepSeek (via Ollama)**.
5. **Viz**: Dashboard interativo em **Streamlit** com Chatbot RAG.

## Tech Stack

- **Gerenciamento**: `uv` (Astral)
- **Orquestração**: Kedro + Kedro-Viz
- **Processamento**: Polars + PyArrow
- **Cloud/Data**: Google BigQuery + BigQuery Storage API
- **Qualidade de Código**: Ruff (Lint), Ty (Typing), Pytest (Testes)
- **Banco de Dados**: PostgreSQL + pgvector (via Docker)
- **Modelos AI**:
  - *Embedding*: `all_MiniLM-L6-v2` (local)
  - *SLM*: `deepseek-r1:1.5b` (via Ollama) (local)

## Estrutura do Repositório

``` plaintext
thelook_ecommerce_analysis/
├── .github/                        # Workflows de CI/CD
├── conf/
│   ├── base/                       # Configurações Base (Logging, Catalog, Params)
│   ├── local/                      # Credenciais (Ignorado pelo Git)
│   └── README.md                   # Documentação específica das configurações
│
├── data/                           # Ignorado no Git (apenas estrutura)
│
├── src
│   └── thelook_ecommerce_analysis
│        ├── hooks.py               # Hooks de monitoramento de memória/tempo
│        ├── pipeline_registry.py   
│        ├── pipelines              # Pipelines
│        │   └── data_ingestion
│        ├── settings.py            # Configurações do Kedro
│        └── utils/                 # Scripts auxiliares
│
├── tests/                          # Testes Automatizados
│   ├── pipelines/                  # Testes dos pipelines
│   ├── integration/                # Teste de integração com PostgreSQL (Docker)
│   ├── utils/                      # Teste dos scripts auxiliares
│   └── kedro_settings/             # Testes das configurações do Kedro
│
├── .gitignore                      # Arquivos para serem ignorados no Git
├── docker-compose.yml              # Infraestrutura (Postgres + PgAdmin)
├── pyproject.toml                  # Configuração Central (UV, Ruff, Ty, Pytest)
├── README.md                       # Documentação Principal
└── uv.lock                         # Lockfile do UV
```

## Como Executar

### Pré-requisitos

- Docker e Docker Compose
- Python 3.13 com `uv` instalado (imagem oficial da Astral)
- Service Account do Google Cloud (JSON) com permissão de leitura no BigQuery.

## Tabelas de Métricas

- **Métricas de Vendas e Receita**: Focada no desempenho financeiro.
  - **Tabelas Fonte**: `order_items`, `orders`, `products`.
  - **Métricas**:
    - **GMV (Gross Merchandise Value)**: Soma total do valor das vendas (`sale_price`).
    - **Ticket Médio (AOV)**: Média de gasto por pedido.
    - **Taxa de Cancelamento**: % de pedidos com status `Cancelled`.
- **Métricas de Clientes (CRM e Retenção)**: Focada no comportamento e valor do usuário ao longo do tempo.
  - **Tabelas Fonte**: `users`, `orders`.
  - **Métricas**:
    - **LTV (Lifetime Value)**: Valor total gasto por usuário desde o cadastro.
    - **Análise de Cohort**: Retenção de usuário agrupados pelo mês de aquisição (safra).
    - **RFM (Recência, Frequência, Monetário)**: Segmentação de clientes para marketing.
    - **Novos vs. Recorrentes**: Proporção de vendas de primeira compra vs. recompra.
- **Métricas de Produto e Estoque**: Focada na logística e atratividade do item.
  - **Tabelas Fonte**: `inventory_items`, `products`, `order_items`, `distribution_center`.
  - **Métricas**:
    - **Taxa de Devolução**: % de itens com status `Returned`.
    - **Tempo de Envio**: Diferença entre `created_at` e `shipped_at`.
    - **Margem de Produto**: Diferença entre `sale_price` e `cost`.
    - **Aging do Estoque**: Tempo que os itens ficam no inventário antes da venda.
- **Métricas de Navegação (Web Analytics)**: Focada no funil de conversão no site.
  - **Tabelas Fonte**: `events`.
  - **Métricas Possíveis**:
    - **Taxa de Conversão de Sessão**: Visitantes únicos que compram / Total de visitantes.
    - **Abandono de Carrinho**: Usuários que adicionam ao carrinho (`cart`) mas não compram (`purchase`).
    - **Origem de Tráfego**: Análise da coluna `traffic_source`.

## Roadmap de Implementação (Planejamento)

Este planejamento foca nas entregas lógicas, sem datas fixas.

### Fase 1: Fundação & Infraestrutura

- [X] Configurar repositório Git com `.gitignore` e `pyproject.toml`.
- [X] Criar `docker-compose.yml` com PostgreSQL (imagem `pgvector/pgvector`).
- [X] Validar conexão local com o Banco de Dados.
- [ ] Baixar e testar modelo de embedding.
- [ ] Baixar e testar modelos locais (Pull do DeepSeek no Ollama).

### Fase 2: Core Engineering (Kedro ETL)

- [X] Inicializar projeto Kedro (`kedro new`).
- [X] Configurar `crendentials.yml` e `parameters.yml`.
- [X] Registrar datasets no `catalog.yml`.
- [X] Implementar **Pipeline de Ingestão**:
  - [X] Conector BigQuery.
  - [X] Lógica de Watermark (Incremental Load) lendo do Postgres.
- [X] Implementar **Pipeline de Transformação**:
  - [X] Limpeza com Polars (Lazy).
- [ ] Implementar **Pipeline de Carga**:
  - [ ] Criação de tabelas Fato/Dimensão.
  - [ ] Realizar carga dos dados de modo incremental, caso já exista as tabelas.

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

- [X] Configurar `conf/base/logging.yml` e configurar Hooks do Kedro.
- [X] Criar testes unitários para testar hooks.py e settings.py
- [X] Criar testes para pipeline Data Ingestion.
- [X] Criar testes para pipeline Data Processing.
- [ ] Criar testes para pipeline Data Loading.
- [ ] Criar testes para pipeline Vector Store.
- [ ] Criar testes para pipeline AI Enrichment.
- [ ] Escrever testes unitários para os Nodes principais (mockando BigQuery e Ollama).
- [ ] Configurar pipeline de CI (GitHub Actions) para rodar `ruff`, `ty` e `pytest`.
