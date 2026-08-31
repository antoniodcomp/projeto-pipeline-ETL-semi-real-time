# Guia de Desenvolvimento: Fase 4 - Modelagem e Carga no Data Warehouse

Este guia detalha o passo a passo para implementar a **Fase 4** do pipeline ELT semi-real-time. O objetivo desta etapa é capturar os dados armazenados no Data Lake (MinIO) no formato Parquet (originários do Kafka processados pelo Spark) e carregá-los em uma tabela de Staging no nosso Data Warehouse (PostgreSQL), preparando o terreno para transformações futuras via dbt.

---

### Esboço de Organização de Pastas para a Fase 4

Esta estrutura separa os scripts de DDL (Data Definition Language) do script Python responsável pela extração e carga, mantendo o projeto organizado para futura orquestração:

```text
src/
└── data_loader/
    ├── __init__.py
    ├── config.py           # Conexões com o MinIO (S3) e PostgreSQL (Engine)
    ├── extract.py          # Lógica para ler os arquivos Parquet do MinIO particionados
    ├── load.py             # Lógica para inserção no PostgreSQL (to_sql ou COPY) garantindo idempotência
    └── main.py             # Script principal que orquestra extract -> load

infra/
└── sql/
    └── staging_ddl.sql     # Script SQL com o CREATE TABLE da staging layer no Postgres
```

---

## 1. Provisionamento e Configuração do PostgreSQL

O PostgreSQL servirá como nosso Data Warehouse. Precisamos garantir que ele está sendo executado no nosso ambiente Docker com os recursos necessários.

**Passo a Passo:**
1. **Configurar o `docker-compose.yaml` (se ainda não existir):**
   Adicione um serviço para o PostgreSQL, garantindo que você possua variáveis de ambiente configuradas para o usuário, senha e banco de dados (ex: `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`). Mapeie a porta `5432` e crie um volume nomeado do Docker para persistência dos dados (evita a perda de dados se o container cair).
2. **Criação do Schema:**
   Crie um schema lógico dedicado para a camada *Staging* (ex: `bronze` ou `staging`), separando logica e visualmente esses dados brutos de tabelas dimensionais ou fatos que serão geradas posteriormente (camadas *Silver* e *Gold*).

---

## 2. Modelagem Relacional (Tabela de Staging)

A tabela de *Staging* deve refletir fielmente a estrutura dos dados de origem, agindo como um espelho relacional dos arquivos Parquet gerados na Fase 3, sem grandes transformações estruturais nesta etapa.

**Passo a Passo:**
1. **Criar a DDL (Data Definition Language):**
   Escreva um script SQL para criar a tabela. Lembre-se de usar os tipos de dados apropriados do Postgres que se alinhem com o seu dataset (UCI Energy Dataset).
   *Exemplo de modelagem para seu dataset:*
   ```sql
   CREATE TABLE staging.sensor_data (
       house_id VARCHAR(50),
       current_timestamp TIMESTAMP,
       original_datetime TIMESTAMP,
       global_active_power NUMERIC,
       global_reactive_power NUMERIC,
       voltage NUMERIC,
       global_intensity NUMERIC,
       sub_metering_1 NUMERIC,
       sub_metering_2 NUMERIC,
       sub_metering_3 NUMERIC,
       year INT,
       month INT
   );
   ```
2. **Executar a DDL no banco:**
   Aplique o comando no PostgreSQL conectando-se localmente (via ferramentas como DBeaver, pgAdmin) ou através de um script de inicialização do próprio Docker (`/docker-entrypoint-initdb.d/`).

---

## 3. Desenvolvimento do Script de Carga (Python)

A lógica central desta fase é o processo de extração dos dados do MinIO e inserção no Postgres. Como estamos simulando um processo ELT (Extract, Load, Transform), faremos apenas a movimentação de dados (Extração e Carga), deixando transformações de regras de negócio para a ferramenta dbt na próxima fase.

**Passo a Passo:**
1. **Conexão com MinIO (Extract):**
   Utilize bibliotecas como `boto3`, `s3fs` ou `pyarrow` no Python. Certifique-se de configurar o cliente S3 para apontar para o `endpoint_url` do seu container MinIO local (ex: `http://localhost:9000`), fornecendo `aws_access_key_id` e `aws_secret_access_key`.
2. **Leitura dos Arquivos Parquet:**
   Leia os arquivos no bucket do MinIO. Lembre-se que eles foram particionados por `year` e `month`. Seu script precisa iterar sobre os diretórios para buscar novos micro-batches ou ler os prefixos desejados utilizando `pandas.read_parquet`.
3. **Conexão com PostgreSQL:**
   Utilize a biblioteca `SQLAlchemy` (geralmente juntamente com `psycopg2`) para criar uma `engine` de conexão (ex: `postgresql://user:pass@localhost:5432/dbname`).
4. **Inserção de Dados (Load):**
   Despeje o DataFrame lido no MinIO diretamente para o Postgres. Utilize o método `to_sql()` do Pandas ou, para uma inserção em altíssima performance, utilize o suporte a `COPY` do Postgres através da função `execute_values` do psycopg2.

---

## 4. Garantia de Idempotência

Idempotência significa que o script pode rodar 1 ou 1000 vezes para os mesmos dados e o resultado no banco será rigorosamente igual (sem inserções duplicadas). Isso é crítico em Engenharia de Dados para lidar com falhas e reprocessamentos seguros.

**Estratégias para implementar no script de carga:**
- **Opção A (Delete-and-Load):** Para o período de dados que está sendo ingerido (ex: todos os registros do dia ou hora atual), primeiro faça um `DELETE FROM staging.sensor_data WHERE ...` para limpar aquela "janela de tempo". Só então insira os registros. Assim, re-executar a carga apenas sobrepõe a janela, mantendo o controle limpo.
- **Opção B (Upsert / ON CONFLICT):** Crie uma chave primária composta na tabela (ex: `house_id` + `original_datetime`). O script de carga tentará um insert, e caso encontre um conflito de chave primária, executa a regra de negócio para ignorar os duplicados ou atualizá-los (`INSERT ... ON CONFLICT DO NOTHING`).

---

## 5. Preparação para Orquestração (Airflow)

Esta Fase 4 sugere scripts de carga ou uso do **Airflow**. Caso você inicie o script via Python puro agora, prepare-o para o Airflow no futuro:
1. **Modularização:** Separe tudo em funções bem definidas (`extract_minio()`, `load_postgres()`), permitindo que cada função se torne uma Task de uma DAG depois.
2. **Parametrização de Tempo:** Evite que o script tente "ler tudo" o tempo todo. Faça-o receber parâmetros de data (ex: janela de execução), algo que o Airflow passa dinamicamente (`{{ ds }}`) durante os agendamentos.
3. **Logging Robustez:** Adicione `try/except` com logs para rastrear erros, pois o Airflow usa isso para falhar tarefas e enviar alertas (retries).

---

### Resultado Esperado
Ao concluir esses passos, ao gerar novos eventos através da API/Kafka/Spark, o acionamento do script Python da Fase 4 fará com que esses registros populam com sucesso e idempotência a tabela relacional do PostgreSQL, ficando disponíveis para consultas SQL brutas e modelagens analíticas de Business Intelligence.
