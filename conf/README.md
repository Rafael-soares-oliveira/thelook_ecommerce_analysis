# Configuração do Projeto

Este diretório contém os arquivos YAML que controlam o comportamento, conexões de dados e parâmetros de execução do pipeline.

## 1. logging.yml

Define como os logs da aplicação são formatados e armazenados.

* **Handlers**:
  * **console**: Saída padrão (stdout) simplificada para acompanhamento em tempo real.
  * **info_file_handler**: Salva logs gerais em `logs/info.log` com rotação automática.
  * **erro_file_handlers**: Segrega apenas errors em `logs/errors.log` para facilitar o debug.
* **Integração com Hooks**:
  * O projeto utiliza Hooks que dependem destes loggers para registrar métricas de memória e tempo de execução de cada nó.
  * A configuração `propagate: no` evita duplicidade de mensagens no terminal.

## 2. Catalog.yml

O catálogo de dados utiliza recursos avançados do Kedro e Polars para garantir escalabilidade e reduzir código repetitivo.

### Principais Conceitos Utilizados:

* **Dataset Factory**:
  * Utiliza padrões como `{namespace}_raw_{table}`.
  * Isso elimina a necessidade de registrar cada tabela manualmente. Se o pipeline gerar um dataset chamado `ingestion_raw_orders`, o catálogo aplica automaticamente as configurações definidas neste padrão.
* **YAML Anchors & Aliases**:
  * Definido `_parquet_settings` (**Anchor**) uma única vez e é reutilizado em todas as camadas (**Alias**). Isso garante consistência nos argumentos de salvamento (ex: compressão `zstd`).
* **Lazy Execution**:
  * Utiliza o `polars.LazyPolarsDataset`. Isso significa que os dados não são carregados na memória RAM imediatamente. O Polars constrói um plano de execução e só processa os dados quando uma ação (collect/fetch) é explicitamente chamada, otimizando drasticamente o uso de memória.

## 3. parameters.yml:

Contém os parâmetros que controlam a lógica de negócio e a construção dinâmica do pipeline (Pipeline Factory).

### Estrutura:

* **monitoring**: Define os limites para alertar de uso de memória RAM nos Hooks.
* **ingestion**: Controla a extração do BigQuery.
  * **gcp_service_account**: Caminho para o arquivo JSON de credenciais (String). Arquivo obtido na GCP.
  * **start_date**: Data inicial da extração dos dados.
  * **safety_limit**: Quantidade máxima de linhas esperada para tabelas **snapshot**.
  * **incremental_tables**: Dicionário `tabela: coluna_data`. O pipeline usa isso para gerar queries com filtros temporais (`WHERE data >= start_date`).
  * **snapshot_tables**: Lista de tabelas dimensionais (**full_load**). O pipeline adiciona automaticamente uma verificação de segurança (`COUNT`) antes de baixar.

## 4. local/credentials.yml

Armazena segredos e credenciais sensíveis.
> **⚠️ IMPORTANTE**: Este arquivo é ignorado pelo Git (`.gitignore`) para segurança. Este arquivo deve ser criado manualmente na pasta `conf/local`.

### Exemplo de estrutura:

```
# Exemplo para conexões de banco de dados
postgres:
  host: localhost
  port: 5432
  dbname: thelook_db
  user: admin
  password: admin_password
  driver: psycopg
```
