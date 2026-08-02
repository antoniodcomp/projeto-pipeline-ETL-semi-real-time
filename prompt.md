# Prompt Aprimorado

Quero que você atue como um **Arquiteto de Dados Sênior**, **Engenheiro de Dados Sênior** e **Tech Lead** responsável por planejar um projeto de nível profissional para portfólio.

Meu objetivo é desenvolver um **pipeline ELT semi-real-time** completo utilizando tecnologias modernas **gratuitas e open source** do ecossistema de Data Engineering. O documento deverá servir como um guia completo de estudos e implementação, permitindo que eu execute o projeto do início ao fim sem depender de outro roteiro.

O projeto deverá ser executado inteiramente em ambiente local utilizando **Docker Compose**, permitindo que qualquer pessoa consiga reproduzi-lo sem custos de infraestrutura em nuvem.

---

# Projeto

## Objetivo

Construir um pipeline ELT semi-real-time capaz de ingerir dados provenientes de IoT, logs ou APIs, armazená-los em um Data Lake, transformá-los incrementalmente e disponibilizá-los em um Data Warehouse para consultas analíticas.

---

# Stack Tecnológica

## Fonte de Dados

* Apache Kafka (preferencialmente)
* API REST simulada utilizando FastAPI
* Gerador de dados IoT/Logs em Python
* Dados enviados para o Data Lake (Raw Zone)

---

## Processamento

* Apache Spark Structured Streaming (preferencialmente)
* Apache Flink (comparação conceitual)

---

## Orquestração

* Apache Airflow (preferencialmente)
* Dagster (comparação conceitual)

---

## Data Lake

* MinIO (substituindo Amazon S3)
* Arquivos Parquet
* Compressão Snappy
* Particionamento por:

  * Ano
  * Mês
  * Dia
  * Hora

---

## Catálogo de Dados

* Hive Metastore
* (Opcional) Project Nessie

---

## Query Engine

* Trino (preferencialmente)
* DuckDB (comparação)
* Spark SQL (comparação)

Explique como essas ferramentas substituem o Redshift Spectrum permitindo consultas diretamente sobre arquivos Parquet armazenados no MinIO.

---

## Data Warehouse

* PostgreSQL (preferencialmente)
* Supabase PostgreSQL (opcional)

O projeto deverá utilizar:

* Staging
* Data Warehouse
* Data Marts

Explique por que PostgreSQL foi escolhido em vez de Redshift, BigQuery ou Snowflake.

---

## Transformações

* dbt Core
* Models
* Sources
* Seeds
* Snapshots
* Incremental Models
* Macros
* Documentation
* Exposures
* Tests

---

## Qualidade dos Dados

* dbt Tests
* Great Expectations
* (Opcional) Pandera

---

## Visualização

Escolha uma das ferramentas abaixo:

* Apache Superset
* Metabase

Explique a arquitetura e as vantagens da ferramenta escolhida.

---

## Monitoramento

* Prometheus
* Grafana
* Monitoramento do Airflow
* Logs centralizados

---

## Containers

* Docker
* Docker Compose

Todos os serviços deverão executar localmente.

---

## CI/CD

* GitHub Actions

---

## Infraestrutura como Código

* Terraform (Docker Provider)

ou

* Docker Compose (infraestrutura local)

Explique como o Terraform pode ser utilizado mesmo sem AWS para provisionar containers e infraestrutura local.

---

# O documento deverá ser entregue em formato PDF e possuir uma estrutura semelhante à documentação de um projeto corporativo.

## Estrutura esperada

### 1. Introdução

* Objetivos do projeto
* Problema resolvido
* Motivação
* Cenário de negócio
* Arquitetura geral

---

### 2. Arquitetura

Apresente:

* Arquitetura completa
* Fluxograma
* Componentes
* Comunicação entre serviços
* Responsabilidade de cada tecnologia
* Justificativa da escolha de cada ferramenta

Explique detalhadamente por que cada tecnologia foi escolhida.

Por exemplo:

* Spark vs Flink
* Airflow vs Dagster
* PostgreSQL vs ClickHouse
* MinIO vs Amazon S3
* Trino vs Redshift Spectrum
* Superset vs Metabase

---

### 3. Roadmap de Desenvolvimento

Divida o projeto em fases.

Para cada fase apresente:

* Objetivo
* Entregáveis
* Tecnologias utilizadas
* Pré-requisitos
* Conceitos necessários
* Tempo estimado
* Dificuldade
* Resultado esperado

---

### 4. Plano de Estudos

Para cada tecnologia, explique em detalhes:

## O que estudar antes de implementar

Exemplo:

### Apache Kafka

* Conceitos fundamentais
* Topics
* Producers
* Consumers
* Consumer Groups
* Partitions
* Replication
* Offset
* Exactly Once
* At Least Once
* At Most Once
* Schema Registry
* Avro
* Serialização

Depois explique profundamente cada conceito.

Faça isso para TODAS as tecnologias do projeto.

---

### 5. Ordem Ideal dos Estudos

Monte uma trilha completa.

Exemplo:

1. Linux
2. Docker
3. Docker Compose
4. Git
5. SQL Avançado
6. PostgreSQL
7. Python
8. FastAPI
9. Apache Kafka
10. Spark Structured Streaming
11. MinIO
12. Trino
13. dbt Core
14. Airflow
15. Terraform
16. GitHub Actions
17. Great Expectations
18. Prometheus
19. Grafana
20. Apache Superset

Explique por que essa ordem é recomendada.

---

### 6. Fluxo de Desenvolvimento

Para cada etapa, apresente:

* Objetivo
* O que será desenvolvido
* Quais arquivos serão criados
* Estrutura de diretórios
* Comandos utilizados
* Boas práticas
* Possíveis erros
* Como testar
* Critérios para considerar a etapa concluída

---

### 7. Desenvolvimento Passo a Passo

Explique detalhadamente como desenvolver cada componente.

Inclua:

#### FastAPI

* API REST
* Simulador de IoT
* Gerador de eventos

#### Kafka

* Producer
* Consumer

#### Spark Structured Streaming

* Leitura do Kafka
* Processamento
* Escrita no MinIO

#### MinIO

* Buckets
* Organização do Data Lake
* Particionamento

#### Trino

* Configuração
* Conectores
* Consulta ao MinIO

#### PostgreSQL

* Schemas
* Tabelas
* Data Warehouse
* Data Marts

#### dbt Core

* Sources
* Models
* Incremental Models
* Snapshots
* Tests
* Documentation

#### Airflow

* DAGs
* Operators
* Scheduling
* Monitoramento

#### Terraform

* Provisionamento dos containers
* Organização dos módulos
* Variáveis
* State

#### GitHub Actions

* Build
* Testes
* Deploy

---

### 8. Organização do Repositório

Mostre uma estrutura profissional de diretórios explicando cada pasta e arquivo.

---

### 9. Testes

Explique como validar cada etapa.

Inclua:

* Testes unitários
* Testes de integração
* Testes do dbt
* Testes de qualidade
* Observabilidade
* Monitoramento

---

### 10. Monitoramento

Explique como monitorar:

* Kafka
* Spark
* Airflow
* PostgreSQL
* MinIO
* Trino
* Docker
* Performance
* Latência
* Throughput
* Qualidade dos dados

---

### 11. Boas Práticas

Inclua recomendações sobre:

* Arquitetura
* Versionamento
* Organização do código
* Segurança
* Particionamento
* Governança
* Modelagem de Dados
* Performance
* Otimização de consultas
* Organização do Data Lake
* Convenções de nomenclatura

---

### 12. Cronograma

Monte um cronograma semanal completo.

Cada semana deve conter:

* Estudos
* Implementação
* Testes
* Entregáveis

---

### 13. Checklist Final

Crie uma checklist para validar que o projeto está concluído.

---

### 14. Evoluções Futuras

Sugira melhorias para transformar o projeto em uma arquitetura Enterprise.

Inclua:

* Apache Iceberg
* Delta Lake
* Apache Hudi
* Data Mesh
* Lakehouse
* CDC
* Debezium
* Kubernetes
* Grafana
* Prometheus
* OpenMetadata
* Apache Atlas
* ClickHouse
* Apache Pinot
* Apache Druid
* Apache Pulsar
* Apache NiFi

Explique quando e por que cada tecnologia seria incorporada.

---

# Fluxo esperado

FastAPI / Simulador IoT

↓

Apache Kafka

↓

Spark Structured Streaming

↓

MinIO (Raw Zone)

↓

dbt Core

↓

PostgreSQL (Staging)

↓

PostgreSQL (Data Warehouse)

↓

Data Marts

↓

Apache Superset ou Metabase

↓

Monitoramento (Prometheus + Grafana)

↓

Alertas

---

O documento deve ser extremamente detalhado, com linguagem técnica, explicações didáticas, diagramas em ASCII quando necessário, tabelas comparativas, exemplos de código, decisões arquiteturais, boas práticas de mercado e justificativas para todas as escolhas. O resultado deve ter qualidade equivalente à documentação técnica utilizada por empresas de tecnologia para orientar engenheiros de dados na construção de pipelines modernos utilizando exclusivamente tecnologias gratuitas e open source.


Se não conseguir gerar o pdf, salve a resposta em um arquivo chamado 'documento_projeto.md'