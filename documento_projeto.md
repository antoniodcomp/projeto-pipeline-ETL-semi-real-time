# 📊 Projeto de Pipeline ELT Semi-Real-Time

> **Documento Técnico Completo** — Guia de Estudos e Implementação  
> **Data:** 02 de Agosto de 2026  
> **Stack:** FastAPI · Kafka · Spark · MinIO · Trino · PostgreSQL · dbt · Airflow · Superset · Prometheus · Grafana  
> **Ambiente:** Docker Compose (100% local, gratuito, open source)

---

## Sumário

1. [SEÇÃO 1 - Introdução](#seção-1---introdução)
2. [SEÇÃO 2 - Arquitetura](#seção-2---arquitetura)
3. [SEÇÃO 3 - Roadmap de Desenvolvimento](#seção-3---roadmap-de-desenvolvimento)
4. [SEÇÃO 4 - Plano de Estudos](#seção-4---plano-de-estudos)
5. [SEÇÃO 5 - Ordem Ideal dos Estudos](#seção-5---ordem-ideal-dos-estudos)
6. [SEÇÃO 6 - Fluxo de Desenvolvimento](#seção-6---fluxo-de-desenvolvimento)
7. [SEÇÃO 7 - Desenvolvimento Passo a Passo](#seção-7---desenvolvimento-passo-a-passo)
8. [SEÇÃO 8 - Organização do Repositório](#seção-8---organização-do-repositório)
9. [SEÇÃO 9 - Testes](#seção-9---testes)
10. [SEÇÃO 10 - Monitoramento](#seção-10---monitoramento)
11. [SEÇÃO 11 - Boas Práticas](#seção-11---boas-práticas)
12. [SEÇÃO 12 - Cronograma de Implementação](#seção-12---cronograma-de-implementação-12-semanas)
13. [SEÇÃO 13 - Checklist Final](#seção-13---checklist-final-de-projeto)
14. [SEÇÃO 14 - Evoluções Futuras](#seção-14---evoluções-futuras-enterprise-architecture)

---

## SEÇÃO 1 - INTRODUÇÃO

### Objetivos do Projeto
O objetivo central deste projeto é projetar e implementar um pipeline de dados ELT (Extract, Load, Transform) orientado a eventos em regime semi-real-time (micro-batching). A arquitetura é construída inteiramente utilizando tecnologias gratuitas e de código aberto (open source), sendo executada de forma autossuficiente em um ambiente local via Docker Compose. Este pipeline deverá ser capaz de ingerir alto volume de dados telemétricos, armazená-los em um Data Lake, transformá-los de forma escalável e disponibilizá-los em um Data Warehouse (DW) otimizado para Business Intelligence e Analytics.

### Problema Resolvido
Em cenários industriais modernos, o maquinário gera dados continuamente a altas frequências (séries temporais, métricas de saúde dos equipamentos, variáveis ambientais). Arquiteturas tradicionais baseadas em processamento *batch* (D+1) são insuficientes para detectar anomalias rapidamente. Por outro lado, arquiteturas *hard real-time* completas (com latências de milissegundos ponta a ponta) costumam ter alto custo de infraestrutura e complexidade de manutenção. 

Este projeto resolve essa lacuna implementando uma abordagem **semi-real-time** (latência na casa de segundos a poucos minutos). Ele garante:
1. Desacoplamento entre produtores e consumidores de dados.
2. Ingestão resiliente a picos de tráfego (buffering).
3. Armazenamento persistente de baixo custo (Data Lake) para auditoria e retreinamento de modelos de Machine Learning.
4. Transformações SQL padronizadas e versionadas no Data Warehouse.
5. Visibilidade total do estado da infraestrutura (Monitoramento e Observabilidade).

### Motivação
A principal motivação é capacitar equipes de engenharia de dados, arquitetos e cientistas de dados com um laboratório prático, robusto e replicável que mimetiza um ambiente corporativo de grande escala. Evitamos dependências (lock-in) de provedores de nuvem (AWS, GCP, Azure), utilizando alternativas equivalentes em código aberto que podem ser executadas localmente ou implantadas em ambientes *on-premise*.

### Cenário de Negócio: Indústria 4.0 (Monitoramento de Turbinas Eólicas)
Imagine um parque eólico operado por uma empresa de energia (WindTech Analytics). O parque possui dezenas de turbinas eólicas conectadas via IoT. Cada turbina envia telemetria a cada 5 segundos contendo:
- `turbine_id`: Identificador da turbina.
- `timestamp`: Momento da medição.
- `wind_speed`: Velocidade do vento (m/s).
- `rotor_speed`: Rotação do rotor (RPM).
- `power_output`: Geração de energia (kW).
- `temperature`: Temperatura da caixa de engrenagens (Celsius).
- `vibration_level`: Nível de vibração.

**Necessidade do Negócio:**
A equipe de manutenção preventiva precisa de painéis atualizados em minutos para identificar turbinas com superaquecimento ou vibração excessiva. Paralelamente, a equipe financeira precisa de agregações diárias e mensais para faturamento.

---

## SEÇÃO 2 - ARQUITETURA

### Arquitetura Geral e Fluxograma

A arquitetura adota o paradigma **Medallion Architecture** integrado a um fluxo de streaming de eventos, culminando em um Data Warehouse relacional.

```text
+-------------------+        +-------------------+         +------------------------+
|   DATA SOURCES    |        |  MESSAGE BROKER   |         |    STREAM PROCESSING   |
|                   |        |                   |         |                        |
|  +-------------+  | REST   |   +-----------+   | Consume |   +----------------+   |
|  | IoT Sensors |  +------->|   |   Kafka   |   +-------->|   | Apache Spark   |   |
|  | (FastAPI)   |  | (JSON) |   | (Topics)  |   | (Kafka) |   | Struct. Stream |   |
|  +-------------+  |        |   +-----------+   |         |   +----------------+   |
+-------------------+        +-------------------+         +-----------+------------+
                                                                       |
                                                                       | Write (Parquet/Delta)
                                                                       v
                                                           +------------------------+
                                                           |       DATA LAKE        |
                                                           |                        |
                                                           |    +--------------+    |
                                                           |    |    MinIO     |    |
                                                           |    | (Raw Zone)   |    |
                                                           |    +--------------+    |
                                                           +-----------+------------+
                                                                       |
                                                                       | Load & Transform
+-------------------+        +-------------------+         +-----------+------------+
|  DATA VISUAL.     |        |  DATA WAREHOUSE   |         |    DATA TRANSFORMATION |
|                   |        |                   |         |                        |
|  +-------------+  | Query  |  +-------------+  | Execute |   +----------------+   |
|  | Apache      |  |<-------+  | PostgreSQL  |  |<--------+   |    dbt Core    |   |
|  | Superset    |  |        |  | (Staging,   |  | (SQL)   |   |    (Airflow)   |   |
|  +-------------+  |        |  |  Marts)     |  |         |   +----------------+   |
+-------------------+        +-------------------+         +------------------------+

                           =========================================
                           |   OBSERVABILITY & ALERTING            |
                           |   [Prometheus] -> [Grafana]           |
                           =========================================
```

### Componentes e Responsabilidade

1. **Simulador IoT (FastAPI):** Aplicação Python responsável por gerar dados de telemetria falsos de forma contínua e enviá-los via API/Websockets para o Message Broker.
2. **Message Broker (Apache Kafka):** Atua como o buffer de ingestão central. Recebe as mensagens JSON, organiza-as em tópicos particionados e permite que múltiplos consumidores leiam os mesmos dados sem impacto na performance de escrita.
3. **Stream Processing (Apache Spark Structured Streaming):** Consome os dados do Kafka em micro-batches (ex: a cada 10-30 segundos). Aplica validações de schema, converte os dados JSON para formatos colunares (Parquet/Delta) e salva no Data Lake.
4. **Data Lake (MinIO):** Object Storage compatível com a API do S3. Armazena os dados brutos (Raw/Bronze Zone) recebidos do Spark de forma imutável e de longo prazo.
5. **Transformação de Dados (dbt Core):** Ferramenta baseada em SQL que orquestra e executa as transformações dos dados. Extrai dados do MinIO (ou atua diretamente pós-carga) para popular o DW. Aplica qualidade de dados e testes.
6. **Data Warehouse (PostgreSQL):** Banco de dados relacional que atua como Serving Layer (Silver e Gold Zones). Armazena tabelas Staging, Dimensões e Fatos (Data Marts) prontas para consumo analítico.
7. **Business Intelligence (Apache Superset ou Metabase):** Plataforma de visualização conectada ao DW para criar os dashboards de monitoramento de saúde das turbinas e agregações de geração de energia.
8. **Observabilidade (Prometheus + Grafana):** O Prometheus coleta métricas de saúde dos containers (CPU, memória) e métricas JMX do Kafka/Spark. O Grafana provê os dashboards operacionais da plataforma.

### Justificativas de Arquitetura e Tabelas Comparativas

#### 1. Engine de Processamento: Apache Spark vs Apache Flink
Optou-se pelo **Apache Spark (Structured Streaming)** por oferecer uma abstração (DataFrame API) familiar a profissionais de dados e excelente integração em cenários micro-batching.

| Critério | Apache Spark (Structured Streaming) | Apache Flink | Vencedor no Projeto |
| :--- | :--- | :--- | :--- |
| **Paradigma** | Micro-batching (Simula streaming via pequenos batches) | True Streaming (Processamento evento a evento) | Spark (Suficiente p/ semi-real-time) |
| **Latência** | Segundos a Minutos | Sub-milissegundos | Flink |
| **Curva de Aprendizado** | Média (Usa SQL e DataFrames muito populares) | Alta (Conceitos complexos de state e watermarks) | Spark |
| **Ecossistema Batch** | Excelente (Dominante no mercado) | Bom (Crescendo em batch unificado) | Spark |
| **Adequação ao Projeto**| Ideal para escrever Parquet periodicamente no MinIO | Mais adequado para alertas instantâneos | **Spark** |

#### 2. Orquestração: Apache Airflow vs Dagster
Embora o processamento principal seja streaming, orquestramos dbt e rotinas complementares.

| Critério | Apache Airflow | Dagster | Vencedor no Projeto |
| :--- | :--- | :--- | :--- |
| **Modelo Mental** | Focado na execução de Tarefas (Task-oriented) | Focado nos Dados (Asset-oriented / Data-Aware) | Empate (depende do perfil) |
| **Maturidade e Comunidade**| Altíssima (Padrão de mercado, vastos providers) | Crescente, excelente para ecossistema moderno | Airflow |
| **Curva de Aprendizado** | Alta (Conceitos de DAGs, Operators, XComs) | Média/Alta (Conceitos de software-defined assets) | Airflow |
| **Monitoramento** | Excelente UI, mas menos visibilidade do dado em si | Visualização direta da linhagem do dado e do estado | **Airflow** (para portabilidade corporativa) |

#### 3. Data Warehouse / Serving Layer: PostgreSQL vs ClickHouse
No cenário open source local, optamos pelo **PostgreSQL**.

| Critério | PostgreSQL | ClickHouse | Vencedor no Projeto |
| :--- | :--- | :--- | :--- |
| **Arquitetura** | Orientado a Linhas (OLTP/HTAP) | Orientado a Colunas (OLAP) | ClickHouse (para analytics) |
| **Performance (Agregações)**| Moderada (pode degradar em tabelas massivas sem índices) | Extremamente Rápida (projetado para bilhões de linhas) | ClickHouse |
| **Facilidade de Setup (Docker)**| Altíssima, pouco uso de memória em repouso | Requer tunning de memória e Zookeeper/Keeper | PostgreSQL |
| **Integração com dbt** | Suporte nativo Tier 1 maduro | Suporte comunitário bom, mas requer adaptações | PostgreSQL |
| **Decisão** | Excelente para simulação acadêmica/POC de até ~10GB | Overkill para volumes pequenos em um único container | **PostgreSQL** |

#### 4. Object Storage: MinIO vs Amazon S3
| Critério | MinIO | Amazon S3 | Vencedor no Projeto |
| :--- | :--- | :--- | :--- |
| **Ambiente** | Local / On-premise (Dockerizado) | Nuvem Pública (AWS) | MinIO |
| **Custo** | Gratuito (Custo apenas da infra subjacente) | Pay-per-use (Armazenamento e Transferência) | MinIO |
| **API** | 100% S3 Compatible | Padrão Proprietário | MinIO |
| **Motivação** | Permite desenvolver offline sem custos e sem risco de vazamento | Padrão ouro corporativo | **MinIO** (Simula perfeitamente o S3) |

#### 5. Visualização: Apache Superset vs Metabase
| Critério | Apache Superset | Metabase | Vencedor no Projeto |
| :--- | :--- | :--- | :--- |
| **Público-Alvo** | Engenheiros/Analistas de Dados avançados | Usuários de Negócio / Product Managers | Superset |
| **Capacidade Gráfica** | Muito rica, extensa variedade de charts nativos | Limpa e focada, menos opções complexas | Superset |
| **Controle de Acessos** | Muito granular (Roles, Row-Level Security nativo) | Bom, mas recursos avançados são Enterprise | Superset |
| **Linguagem / Stack** | Python (Fácil extensão para Engenheiros de Dados) | Clojure / Java | Superset |
| **Decisão** | Alinha-se melhor com projetos de Engenharia puros | Ótimo para self-service de negócios rápido | **Apache Superset** |

---

## SEÇÃO 3 - ROADMAP DE DESENVOLVIMENTO

O desenvolvimento foi estruturado de forma incremental e modular.

### Fase 1: Infraestrutura Base e Simulador IoT
- **Objetivo:** Estabelecer o alicerce do projeto (containers) e criar a fonte produtora de dados.
- **Entregáveis:** Arquivo `docker-compose.yml` base, API FastAPI do gerador de telemetria em container separado, rede virtual configurada.
- **Tecnologias:** Docker, Docker Compose, Python (FastAPI, Faker, Pydantic).
- **Pré-requisitos:** Docker instalado e conhecimentos básicos de redes Docker.
- **Conceitos:** Containerização, APIs REST, Geração de Séries Temporais.
- **Tempo Estimado:** 1 a 2 dias.
- **Dificuldade:** Fácil.
- **Resultado Esperado:** Um endpoint rodando localmente capaz de cuspir JSONs de turbinas eólicas a cada poucos segundos.

### Fase 2: Mensageria com Kafka
- **Objetivo:** Ingerir os eventos gerados para um Message Broker persistente e escalável.
- **Entregáveis:** Kafka e Zookeeper (ou KRaft) configurados via Docker Compose, script Python (Producer) integrado ao FastAPI para envio aos tópicos do Kafka.
- **Tecnologias:** Apache Kafka, python-kafka / confluent-kafka.
- **Pré-requisitos:** Fase 1 concluída.
- **Conceitos:** Tópicos, Partições, Producers, Offsets.
- **Tempo Estimado:** 2 a 3 dias.
- **Dificuldade:** Médio.
- **Resultado Esperado:** API enviando dados pro Kafka com sucesso e utilitário de CLI de console mostrando as mensagens em tempo real.

### Fase 3: Processamento Semi-Real-Time com Spark
- **Objetivo:** Consumir os dados do Kafka, aplicar esquema (schema on read) e gravá-los no Data Lake.
- **Entregáveis:** MinIO configurado com buckets criados, Job Spark em PySpark utilizando Structured Streaming.
- **Tecnologias:** Apache Spark (PySpark), MinIO, bibliotecas hadoop-aws.
- **Pré-requisitos:** Fase 2 e conceitos básicos de processamento distribuído.
- **Conceitos:** Micro-batching, DataFrames, Parquet, Checkpointing em Streaming.
- **Tempo Estimado:** 4 a 5 dias.
- **Dificuldade:** Difícil (lidar com dependências de JARs para S3/Minio e Kafka no PySpark costuma gerar atritos).
- **Resultado Esperado:** MinIO sendo populado com arquivos Parquet particionados por data, refletindo os dados do Kafka.

### Fase 4: Modelagem e Carga no Data Warehouse
- **Objetivo:** Preparar a base de consumo analítico.
- **Entregáveis:** PostgreSQL provisionado, scripts em Python ou ferramenta de orquestração (Airflow) para puxar os dados do MinIO e carregar em uma tabela de Staging no Postgres (extensão do ELT).
- **Tecnologias:** PostgreSQL, Pandas/Psycopg2 (para scripts de carga) ou Airflow Operators.
- **Pré-requisitos:** Conhecimentos de Modelagem Relacional e SQL.
- **Conceitos:** Staging Tables, Copy commands, Idempotência.
- **Tempo Estimado:** 3 dias.
- **Dificuldade:** Médio.
- **Resultado Esperado:** Dados fluindo do MinIO (Raw) para a tabela de staging (Bronze/Silver) no DW.

### Fase 5: Transformações com dbt (Data Build Tool)
- **Objetivo:** Aplicar regras de negócio e criar Data Marts agregados dentro do PostgreSQL utilizando dbt.
- **Entregáveis:** Projeto dbt inicializado, modelos staging (limpeza de tipos), modelos dimensionais (Dimensões de turbina) e modelos Fato (Fato_Geracao). Testes de schema configurados.
- **Tecnologias:** dbt-core, dbt-postgres.
- **Pré-requisitos:** PostgreSQL rodando com dados na staging.
- **Conceitos:** Jinja Templating, DAGs, Materializações (Table, View, Incremental), Testes de Qualidade.
- **Tempo Estimado:** 3 a 4 dias.
- **Dificuldade:** Médio.
- **Resultado Esperado:** Linhagem de dados completa no `dbt docs`. Tabelas agregadas por hora prontas para BI.

### Fase 6: Visualização e Monitoramento
- **Objetivo:** Consumir os dados transformados e garantir a saúde do sistema.
- **Entregáveis:** Apache Superset provisionado via Docker, Dashboards de geração de energia criados. Prometheus coletando métricas do Kafka/Spark e visualização no Grafana.
- **Tecnologias:** Apache Superset, Prometheus, Grafana.
- **Pré-requisitos:** Data Marts populados.
- **Conceitos:** Data Storytelling, Observabilidade de Pipelines, Exporters JMX.
- **Tempo Estimado:** 4 dias.
- **Dificuldade:** Médio/Difícil (Devido à configuração de rede e JMX exporters do Prometheus para o Spark).
- **Resultado Esperado:** Duas portas expostas em `localhost`: Uma com dashboards analíticos (Superset) e outra com dashboards operacionais (Grafana).
# SEÇÃO 4 - PLANO DE ESTUDOS

Para construir um pipeline ELT semi-real-time com maestria, é essencial compreender profundamente as bases de cada tecnologia envolvida. A seguir, apresentamos um detalhamento avançado e conceitual das tecnologias que compõem nossa stack.

## 1. Apache Kafka
O Kafka atua como o backbone de mensageria e streaming de eventos em tempo real do nosso pipeline. Compreender sua mecânica é vital para garantir alta disponibilidade e baixa latência.

**Conceitos Fundamentais:**
*   **Topics:** Categorias ou feeds onde os registros são publicados. São os canais lógicos de dados.
*   **Producers & Consumers:** Aplicações que escrevem dados nos tópicos (Producers) e aplicações que leem dados (Consumers).
*   **Consumer Groups:** Conjuntos de Consumers que cooperam para consumir mensagens de um tópico. Cada partição de um tópico é consumida por apenas um membro do grupo, permitindo paralelismo e escalabilidade horizontal.
*   **Partitions:** A unidade fundamental de escalabilidade do Kafka. Um tópico é dividido em partições, permitindo que os dados sejam distribuídos através de múltiplos brokers no cluster.
*   **Replication:** O processo de copiar dados de partições entre diferentes brokers para garantir tolerância a falhas.
*   **Offset:** Um identificador sequencial único atribuído a cada registro dentro de uma partição, usado pelos consumers para rastrear até onde a leitura foi efetuada.
*   **Semânticas de Entrega (At Most Once, At Least Once, Exactly Once):** Define as garantias de entrega de mensagens. *At Most Once* (pode perder, nunca duplica), *At Least Once* (nunca perde, pode duplicar) e *Exactly Once* (nunca perde, nunca duplica, mais complexo e requer suporte transacional).
*   **Schema Registry, Avro & Serialização:** O Schema Registry centraliza a governança dos formatos dos dados (schemas). Avro é um formato de serialização binária compacto e eficiente que, em conjunto com o Registry, garante a evolução segura de schemas sem quebrar a compatibilidade entre producers e consumers.

## 2. Apache Spark Structured Streaming
Motor de processamento distribuído responsável por ingerir fluxos contínuos do Kafka, transformar e descarregar no Data Lake (MinIO).

**Conceitos Fundamentais:**
*   **SparkSession:** O ponto de entrada unificado para ler dados, criar DataFrames e executar consultas SQL no Spark.
*   **DataFrame API:** Uma abstração de dados em formato tabular, similar a tabelas em um banco de dados relacional, que suporta operações de manipulação (select, filter, groupBy) otimizadas pelo motor Catalyst.
*   **Structured Streaming:** Motor de processamento de fluxo escalável e tolerante a falhas construído sobre o motor Spark SQL, tratando fluxos de dados em tempo real de forma análoga a tabelas não limitadas (unbounded tables).
*   **Watermarks:** Mecanismo crucial para lidar com dados atrasados (late data). Define um limite temporal sobre o quão antigo o dado pode ser para ainda ser considerado na agregação stateful, permitindo que o Spark descarte estados velhos e evite estouro de memória.
*   **Triggers:** Determinam o intervalo no qual as operações de streaming devem processar os novos dados disponíveis, como micro-batches de X segundos ou processamento contínuo.
*   **OutputMode:** Define como as atualizações das tabelas de resultados são escritas no sink (Append, Complete ou Update).
*   **Checkpointing:** O processo de salvar o estado e os metadados das queries de streaming em armazenamento durável (ex: MinIO/HDFS), permitindo recuperação exata de falhas.
*   **Sinks & Sources:** *Sources* são as fontes de dados de entrada (ex: Kafka) e *Sinks* são os destinos onde os resultados processados são escritos (ex: diretórios Delta/Parquet no MinIO).

## 3. MinIO / Object Storage
O repositório central do nosso Data Lake, fornecendo armazenamento escalável e de alto desempenho projetado para grandes volumes de dados não estruturados ou arquivos colunares.

**Conceitos Fundamentais:**
*   **Buckets:** Contêineres lógicos de alto nível para agrupar objetos, servindo como o namespace primário (similar a um diretório raiz).
*   **Objects:** Os dados armazenados em si, que consistem no arquivo, seus metadados estruturados e um identificador global (chave).
*   **Policies:** Políticas baseadas em JSON que definem regras granulares de controle de acesso (IAM) sobre buckets e objetos (quem pode ler, escrever, deletar).
*   **Versionamento:** Capacidade de preservar, recuperar e restaurar cada versão de cada objeto armazenado num bucket, protegendo contra exclusão ou sobrescrita acidental.
*   **Lifecycle:** Regras automatizadas para o gerenciamento do ciclo de vida dos dados, como expiração (exclusão automática após X dias) ou transição para tiers de armazenamento mais baratos.
*   **S3 API Compatibility:** A garantia de que o MinIO responde perfeitamente às mesmas chamadas de API do Amazon S3, permitindo o uso do vasto ecossistema de ferramentas AWS (como Boto3) sem alterações no código.

## 4. Trino
Motor de consulta SQL distribuído de altíssimo desempenho. Ele não armazena dados; ele consulta dados onde residem (federação de dados).

**Conceitos Fundamentais:**
*   **Arquitetura distribuída:** Composta por um Coordinator (analisa, planeja e escalona as queries) e múltiplos Workers (executam as tarefas e processam os dados em paralelo).
*   **Conectores:** Plugins que permitem ao Trino se conectar a diferentes fontes de dados (MinIO/Iceberg, PostgreSQL, Kafka) usando uma interface padronizada.
*   **Catálogos:** Definição de uma configuração de conector (ex: catálogo `lakehouse` conectando ao MinIO, catálogo `rds` conectando ao PostgreSQL). Os catálogos contêm esquemas, que por sua vez contêm tabelas.
*   **SQL Analytics:** Capacidade de executar consultas ANSI SQL complexas, joins, agregações de janela (window functions) sobre petabytes de dados em várias origens simultaneamente.
*   **Performance Tuning:** Técnicas para otimização de consultas, que englobam particionamento adequado, formatos de arquivo (Parquet/ORC), alocação de memória (query.max-memory) e otimização de custo (Cost-Based Optimizer).

## 5. PostgreSQL
O banco de dados relacional, utilizado no nosso cenário para armazenamento de metadados, ou possivelmente como banco de dados transacional para ser ingerido e cruzado com dados do Data Lake.

**Conceitos Fundamentais:**
*   **Schemas:** Namespaces que contêm tabelas, views e funções, permitindo organizar dados e gerenciar permissões no banco.
*   **Indexes:** Estruturas de dados (B-Tree, Hash, GIN) que melhoram drasticamente a velocidade de recuperação de dados em colunas frequentemente consultadas.
*   **Partitioning:** Divisão de grandes tabelas lógicas em pedaços físicos menores, melhorando o desempenho de queries e facilitando manutenção (ex: drop de partições antigas em vez de DELETE).
*   **VACUUM:** Processo crítico de manutenção (garbage collection) que recupera espaço ocupado por tuplas "mortas" (atualizadas/deletadas) e atualiza as estatísticas para o otimizador de query.
*   **WAL (Write-Ahead Logging):** Mecanismo padrão para garantir a integridade dos dados, escrevendo mudanças num log antes de aplicá-las no disco, essencial para crash recovery e replicação.
*   **Replication:** Cópia dos dados de um servidor (Master) para outros (Replicas) para alta disponibilidade (HA) e escalabilidade de leitura (Streaming Replication, Logical Replication).
*   **Roles & Performance:** Gerenciamento robusto de acesso via papéis e otimização geral de parâmetros como `shared_buffers`, `work_mem` e `maintenance_work_mem`.

## 6. dbt Core
Ferramenta para modelagem de dados no Data Warehouse / Data Lake (o "T" do ELT). Ele trata as transformações SQL como engenharia de software (com versionamento e testes).

**Conceitos Fundamentais:**
*   **Models:** Arquivos `.sql` que contêm uma única instrução `SELECT`. O dbt se encarrega de compilar isso em materializações no banco (Views, Tables).
*   **Sources & Seeds:** *Sources* mapeiam tabelas brutas existentes no banco (definidas em YAML). *Seeds* são pequenos arquivos CSV estáticos carregados diretamente via dbt.
*   **Snapshots:** Captura do estado de dados mutáveis ao longo do tempo (Type 2 Slowly Changing Dimensions - SCD2) em uma tabela temporal.
*   **Incremental Models:** Modelos que, em vez de recriar a tabela do zero (Full Refresh), processam e inserem apenas os dados novos, economizando tempo de computação e dinheiro.
*   **Macros & Jinja:** Macros são blocos de código reutilizáveis escritos com a linguagem de template Jinja. Permitem criar lógica condicional, loops e abstrações SQL dinâmicas (ex: `{{ config(...) }}`).
*   **ref() & source():** As funções Jinja essenciais. `source()` referencia tabelas brutas; `ref()` referencia outros modelos. Ambas criam automaticamente a linhagem de dados e gerenciam as dependências de execução (DAG).
*   **Tests, Documentation & Exposures:** Testes validam pressupostos (ex: not_null, unique). A Documentação é auto-gerada com dicionários de dados. Exposures documentam os usos finais (dashboards) que dependem dos modelos.

## 7. Apache Airflow
O orquestrador central que comanda quando e como as etapas do pipeline ocorrem.

**Conceitos Fundamentais:**
*   **DAGs (Directed Acyclic Graphs):** Uma coleção de todas as tarefas que você quer executar, organizadas de forma a refletir seus relacionamentos e dependências (sem ciclos).
*   **Operators:** Representam uma única tarefa no DAG (ex: `PythonOperator`, `BashOperator`, `TrinoOperator`).
*   **Sensors:** Um tipo especial de Operator que fica aguardando (polling) em intervalos regulares para que um evento aconteça (ex: um arquivo chegar num bucket) antes de prosseguir com a DAG.
*   **XCom (Cross-Communication):** Mecanismo nativo que permite às tarefas trocarem pequenas mensagens/dados entre si.
*   **Connections & Variables:** Armazenamento centralizado e seguro para gerenciar senhas, chaves de API, URLs de bancos de dados (Connections) e configurações gerais (Variables).
*   **Pools:** Mecanismo para limitar o paralelismo e evitar que DAGs sobrecarreguem recursos externos (ex: limitar a 5 conexões simultâneas num DB).
*   **Scheduling & Executor Types:** Define a cronologia das execuções (cron). Executores (LocalExecutor, CeleryExecutor, KubernetesExecutor) definem como as tarefas são alocadas e executadas fisicamente.

## 8. FastAPI
Framework web em Python moderno e de alto desempenho, utilizado na camada de ingestão para receber dados de sistemas externos e publicá-los no Kafka.

**Conceitos Fundamentais:**
*   **Async/Await:** Utiliza as bibliotecas assíncronas do Python (asyncio) nativamente, o que o torna incrivelmente rápido para cargas baseadas em I/O, como chamadas de rede ou escritas no Kafka.
*   **Pydantic:** Uma biblioteca de validação e serialização de dados baseada em type hints do Python. FastAPI a utiliza intensamente para validar o payload das requisições JSON.
*   **Endpoints:** As rotas de API (ex: `POST /ingest/sales`) definidas usando decoradores do Python de maneira limpa.
*   **Middleware:** Lógica interceptadora que é executada antes que uma requisição chegue ao endpoint ou antes que a resposta seja enviada ao cliente (útil para logs, métricas, CORS).
*   **Dependencies:** Um sistema robusto de Injeção de Dependências, facilitando o compartilhamento de lógicas (como conexão a banco de dados ou autenticação) através das rotas de forma limpa.
*   **Background Tasks:** Permite delegar processamento longo (como envio de e-mails ou uploads complexos) para rodar de forma assíncrona após a API já ter retornado uma resposta ao usuário.

## 9. Docker & Docker Compose
A base da infraestrutura local, garantindo que o pipeline funcione independentemente do sistema operacional base através de contêineres.

**Conceitos Fundamentais:**
*   **Images & Containers:** *Images* são templates imutáveis de leitura com as dependências da aplicação. *Containers* são as instâncias em execução (runtime) dessas imagens.
*   **Volumes:** Mecanismo para persistência de dados fora do ciclo de vida efêmero do contêiner, garantindo que dados de bancos (Postgres, MinIO) não se percam quando o contêiner for destruído.
*   **Networks:** Redes virtuais que permitem a comunicação isolada e segura entre contêineres utilizando DNS (nome do serviço) ao invés de IPs fixos.
*   **Multi-stage builds:** Técnica no `Dockerfile` de usar múltiplas imagens base sequenciais para compilar o código num estágio e apenas copiar os binários finais para uma imagem menor e segura no estágio final.
*   **Health checks:** Comandos de diagnóstico executados periodicamente pelo Docker para verificar se a aplicação dentro do contêiner está verdadeiramente pronta (ex: Postgres aceitando conexões).
*   **Compose services:** Definição da arquitetura multi-container num arquivo `docker-compose.yml`, configurando serviços, dependências, volumes e redes de forma declarativa e orquestrada localmente.

## 10. Terraform
A ferramenta de Infraestrutura como Código (IaC). No cenário local, usaremos para provisionar e gerenciar configurações (ex: criar buckets MinIO via código, configurar usuários no Grafana).

**Conceitos Fundamentais:**
*   **Providers:** Plugins do Terraform (ex: AWS, Docker, MinIO) que interagem e entendem as APIs das tecnologias finais para provisionar a infraestrutura.
*   **Resources:** Os blocos de construção fundamentais do Terraform. Cada bloco descreve um objeto de infraestrutura, como um bucket no MinIO ou um banco de dados.
*   **Variables & Outputs:** *Variables* permitem parametrizar as configurações do Terraform. *Outputs* extraem e expõem propriedades da infraestrutura criada, como IPs ou ARNs.
*   **State:** Arquivo (`terraform.tfstate`) que o Terraform usa para mapear os recursos definidos no código para os recursos reais criados. É a fonte de verdade.
*   **Modules:** Conjuntos reutilizáveis de recursos e arquivos do Terraform empacotados juntos, melhorando a modularidade e reuso de código de infraestrutura.
*   **Plan / Apply:** Ciclo de vida da execução: `terraform plan` verifica e descreve o que mudará; `terraform apply` executa efetivamente as mudanças contra a API dos provedores.

## 11. GitHub Actions
Nossa ferramenta de CI/CD (Continuous Integration / Continuous Deployment) para testar, fazer linting do código e validar as infraestruturas, garantindo a governança do código do pipeline.

**Conceitos Fundamentais:**
*   **Workflows:** Processos automatizados globais compostos de um ou mais jobs e que são ativados por eventos (push, pull request, schedule). Definidos em YAML na pasta `.github/workflows`.
*   **Jobs:** Conjuntos de etapas (steps) executadas na mesma máquina (runner). Jobs podem rodar em paralelo ou de forma sequencial com dependências.
*   **Steps:** Tarefas individuais dentro de um job (executar um comando shell ou usar uma "Action" pré-fabricada).
*   **Runners:** Os servidores virtuais (Ubuntu, Windows) que efetivamente executam os workflows. Podem ser hospedados no GitHub ou Self-hosted.
*   **Secrets:** Variáveis criptografadas e injetadas no ambiente durante a execução (como tokens, senhas de banco), nunca expostas nos logs.
*   **Artifacts & Matrix builds:** *Artifacts* permitem compartilhar arquivos ou pacotes compilados entre jobs ou salvar logs após a execução. *Matrix builds* permitem rodar jobs repetidos com variações de variáveis (ex: testar o código Python nas versões 3.9, 3.10 e 3.11 simultaneamente).

## 12. Great Expectations
Framework avançado de testes de qualidade de dados. Garante que os dados no Data Lake são precisos, íntegros e atendem a regras de negócio estritas.

**Conceitos Fundamentais:**
*   **Expectations:** Asserções verificáveis (testes unitários para os dados). Ex: `expect_column_values_to_not_be_null("id")`.
*   **Suites:** Um agrupamento de Expectativas voltado para um ativo de dados específico (uma tabela) ou processo de negócio.
*   **Datasources:** Configurações que informam à ferramenta onde os dados residem (Trino, Postgres, Spark, MinIO) e por qual motor computacional serão validados.
*   **Checkpoints:** O componente executor. É ele que é invocado pelo Airflow em tempo de execução para pegar um lote de dados (Batch), rodar a Suite de Expectativas e gerar um resultado (Success/Failure).
*   **Data Docs:** Documentação de qualidade de dados auto-gerada a partir das Expectativas em formato HTML interativo, provendo um relatório compreensível do estado dos dados.

## 13. Prometheus & Grafana
Nossa stack de Observabilidade. O Prometheus coleta as métricas e o Grafana fornece os painéis visuais. É imperativo para manter a saúde do pipeline em semi-real-time.

**Conceitos Fundamentais:**
*   **Métricas (Prometheus):** Dados temporais quantitativos (Counters, Gauges, Histograms). Ex: `kafka_messages_in_total`.
*   **Exporters:** Pequenos agentes responsáveis por traduzir métricas de sistemas terceiros (Postgres, Kafka via JMX) para o formato que o Prometheus entende via endpoint HTTP.
*   **PromQL:** Linguagem de consulta do Prometheus poderosa, permitindo extrair agregações temporais, médias, e percentis complexos.
*   **Data Sources (Grafana):** A conexão de onde o Grafana vai extrair dados para visualização, geralmente apontando para o servidor Prometheus.
*   **Dashboards & Panels (Grafana):** Telas compostas por diversos gráficos, tabelas e medidores visualizando queries PromQL de forma consolidada e atraente.
*   **Alertas:** Regras definidas que monitoram métricas em tempo real (ex: latência de consumo do Kafka > 10s) e engatilham notificações (Slack, Email, PagerDuty).

## 14. Apache Superset
A camada final de BI (Business Intelligence) open source. Consumirá do Trino ou do PostgreSQL.

**Conceitos Fundamentais:**
*   **Datasources & Datasets:** Conexões apontando fisicamente para bancos de dados via SQLAlchemy e mapeamento lógico das tabelas ou queries para as visualizações.
*   **SQL Lab:** IDE SQL interativa integrada no Superset para que analistas possam explorar os dados livremente antes de transformá-los em gráficos.
*   **Charts:** Representação visual atômica de dados gerados através da interface de exploração.
*   **Dashboards:** Agrupamento de vários Charts interativos numa única interface gerenciável, suportando filtros contextuais e cross-filtering.
*   **Roles & Security:** Gerenciamento rígido de quem tem acesso à leitura, construção ou administração do sistema.
*   **Caching:** Mecanismo vital para escalar a ferramenta de BI. Ele guarda os resultados de queries pesadas em Redis/Memcached para re-renderização imediata dos painéis.

---

# SEÇÃO 5 - ORDEM IDEAL DOS ESTUDOS

Para construir um sistema dessa complexidade, é absolutamente crucial seguir uma lógica evolutiva e incremental. Estudar uma tecnologia fora de ordem causará confusões conceituais, pois as camadas superiores dependem fortemente do comportamento das camadas inferiores de infraestrutura e processamento. 

Abaixo apresentamos a trilha sequencial projetada para proporcionar a curva de aprendizado mais suave e eficaz, simulando a construção natural do pipeline a partir do alicerce.

### Trilha de Conhecimento e Implementação

**Nível 1: O Alicerce de Infraestrutura**
*   **1. Docker & Docker Compose:** O alicerce principal. Absolutamente tudo no projeto rodará contêinerizado. Você precisa saber subir, conectar (networks) e destruir os componentes isoladamente sem quebrar sua máquina física.
*   **2. PostgreSQL:** Compreenda o relacional básico. Ele será a fundação (Metadata Store) para diversas outras ferramentas da stack (Airflow, Superset, etc). 

**Nível 2: Ingestão e Mensageria (A Entrada do Pipeline)**
*   **3. Python & FastAPI:** A camada de API. Aqui os dados sintéticos (falsos eventos transacionais) serão gerados, injetados via requisições assíncronas para simular nosso "real-time". Requer domínio robusto em Python.
*   **4. Apache Kafka:** Esta é a ponte. Após a FastAPI validar as mensagens, elas são despejadas no Kafka. O estudo profundo do Kafka é necessário aqui, focado na abstração de produtores de alto rendimento. Entenda partições e latência.

**Nível 3: Armazenamento e Processamento do Data Lake (O Meio do Pipeline)**
*   **5. MinIO (Object Storage):** Agora o foco muda para o destino. Onde salvaremos grandes volumes de dados brutos e processados de forma permanente. Entenda buckets S3, políticas de acesso antes de gravar dados lá.
*   **6. Apache Spark Structured Streaming:** O elo que liga a Mensageria ao Armazenamento. O Spark vai se conectar ao Kafka (tópicos em real-time), realizar transformações em streaming ou micro-batch e gravar os dados em partições no MinIO no formato parquet/delta. 

**Nível 4: A Camada Analítica e de Transformações (O "T" do ELT)**
*   **7. Trino:** Os dados estão agora gravados no MinIO. Eles não são facilmente legíveis diretamente. O Trino atua como o motor SQL federado (via catálogos) permitindo que você enxergue esses arquivos dispersos no MinIO como tabelas de bancos relacionais com altíssima velocidade.
*   **8. dbt Core:** Com o Trino fornecendo o poder computacional SQL, entra o dbt Core. O dbt usará o Trino para refinar os dados brutos (Bronze), filtrá-los, uni-los e salvar agregados (Prata e Ouro) construindo o Data Warehouse no topo do Data Lake.

**Nível 5: Orquestração e Qualidade (Governança e Confiabilidade)**
*   **9. Apache Airflow:** O regente da orquestra. Ele comandará e coordenará o acionamento dos scripts Spark (batch jobs), execuções diárias/horárias das pipelines dbt e monitoramento de falhas. 
*   **10. Great Expectations:** Adicionaremos checagens de sanidade automatizadas nas DAGs do Airflow. Aqui as expectativas garantirão que dados corrompidos parem o processo ao invés de poluírem o Data Lake final.

**Nível 6: O Produto Final (A Saída do Pipeline)**
*   **11. Apache Superset:** O ápice da jornada dos dados. Aqui os dados refinados no nível Ouro (acessados via Trino) são disponibilizados em visualizações de negócios (Dashboards) para a tomada de decisão da empresa.

**Nível 7: Gestão, Maturidade e Monitoramento (Nível Produção)**
*   **12. Terraform:** Refatoração de maturidade. Neste estágio, substitui-se processos manuais (como criar buckets no MinIO e painéis) por IaC, definindo toda a infraestrutura adjacente de forma declarativa e versionada.
*   **13. GitHub Actions:** Implementação do CI/CD, aplicando lints no Python, testando os códigos das dags do Airflow, e validando os planos do Terraform automaticamente através do GitHub.
*   **14. Prometheus & Grafana:** O estado final da arte, focando na observabilidade. Adicionaremos telemetria a todas as etapas (Kafka JMX, latência da FastAPI, uso de RAM do Spark) finalizando o projeto como uma solução de nível empresarial corporativo ("production ready").

Essa ordem é planejada para que nenhum componente requeira conhecimentos de um componente subsequente, gerando uma fluidez perfeita de desenvolvimento incremental. Você começa sem depender de nada, e termina coordenando um ecossistema inteiro de altíssima escala.
# SEÇÃO 6 - FLUXO DE DESENVOLVIMENTO

O desenvolvimento de um pipeline de dados moderno exige um método estruturado e iterativo. Adotaremos uma abordagem *bottom-up* com validações contínuas, garantindo que cada componente seja testado isoladamente antes da integração.

Abaixo, detalhamos as etapas de implementação desta arquitetura ELT semi-real-time.

---

## 6.1 Setup Inicial e Infraestrutura Base (Docker & Terraform)

*   **Objetivo:** Estabelecer a fundação de infraestrutura local reproduzível.
*   **O que será desenvolvido:** Configuração de IaC (Infrastructure as Code) usando Terraform para orquestrar os containers Docker.
*   **Arquivos criados:** `docker-compose.yaml`, `main.tf`, `variables.tf`, `.env`.
*   **Estrutura de diretórios:**
    ```text
    projeto/
    ├── infra/
    │   ├── terraform/
    │   └── docker/
    ```
*   **Comandos:** `terraform init`, `terraform apply -auto-approve`, `docker-compose up -d`.
*   **Boas práticas:** Utilizar arquivos `.env` para credenciais; fixar versões de imagens Docker; manter o estado do Terraform fora do controle de versão (`.gitignore`).
*   **Erros comuns:** Conflitos de portas (ex: 5432, 8080 em uso). *Solução:* Mapear portas alternativas no host ou finalizar processos concorrentes.
*   **Como testar:** `docker ps` para verificar a integridade dos containers.
*   **Critério de conclusão:** Todos os serviços base subindo com `status: healthy`.

---

## 6.2 Ingestão: FastAPI e IoT Simulator

*   **Objetivo:** Simular o fluxo contínuo de dados na origem.
*   **O que será desenvolvido:** Uma API REST e um worker que gera e envia dados sintéticos de telemetria.
*   **Arquivos criados:** `app/main.py`, `app/simulator.py`, `app/models.py`, `app/requirements.txt`.
*   **Estrutura de diretórios:**
    ```text
    projeto/
    ├── src/
    │   ├── api/
    ```
*   **Comandos:** `uvicorn app.main:app --reload`.
*   **Boas práticas:** Utilizar Pydantic para validação rígida de schemas; implementar tratamento de exceções global; não bloquear o *event loop*.
*   **Erros comuns:** Falha na validação de schema. *Solução:* Garantir que o gerador tipifica os dados exatamente como esperado pelo Pydantic.
*   **Como testar:** Fazer requisições ao endpoint `/docs` (Swagger UI) e inspecionar os payloads gerados.
*   **Critério de conclusão:** API respondendo HTTP 200 e logs exibindo dados gerados a cada X segundos.

---

## 6.3 Mensageria: Apache Kafka

*   **Objetivo:** Desacoplar a origem do processamento, atuando como um *buffer* tolerante a falhas.
*   **O que será desenvolvido:** Tópicos do Kafka e a integração (Producer) com o FastAPI.
*   **Arquivos criados:** `src/api/kafka_producer.py`.
*   **Estrutura de diretórios:** O Kafka rodará via Docker, a integração via código fica junto à API.
*   **Comandos:** `kafka-topics.sh --create --topic telemetry --bootstrap-server kafka:9092`.
*   **Boas práticas:** Configurar *retries* no producer; usar chaves de partição (ex: `device_id`) para garantir a ordem das mensagens do mesmo dispositivo.
*   **Erros comuns:** `LeaderNotAvailableException`. *Solução:* Aguardar o Zookeeper/Kraft inicializar completamente antes de produzir mensagens.
*   **Como testar:** Consumir manualmente usando `kafka-console-consumer.sh`.
*   **Critério de conclusão:** Mensagens geradas pelo FastAPI sendo persistidas no tópico `telemetry`.

---

## 6.4 Processamento Streaming: Apache Spark

*   **Objetivo:** Consumir os dados em tempo real e gravá-los no Data Lake.
*   **O que será desenvolvido:** Job PySpark (Structured Streaming).
*   **Arquivos criados:** `src/spark/stream_processor.py`.
*   **Estrutura de diretórios:**
    ```text
    projeto/
    ├── src/
    │   ├── spark/
    ```
*   **Comandos:** `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 stream_processor.py`.
*   **Boas práticas:** Configurar *checkpointing* em armazenamento confiável; lidar com late data através de *watermarking*.
*   **Erros comuns:** Falta de pacotes JDBC/Kafka. *Solução:* Passar as dependências corretas em `--packages`.
*   **Como testar:** Verificar os logs do Spark buscando por *micro-batches* sendo processados e o throughput (records/sec).
*   **Critério de conclusão:** Job rodando sem falhas e processando dados do Kafka continuamente.

---

## 6.5 Data Lake (Raw Zone): MinIO

*   **Objetivo:** Armazenamento distribuído S3-compatible para a camada Raw.
*   **O que será desenvolvido:** Buckets e políticas de acesso.
*   **Arquivos criados:** Scripts de inicialização (`create_buckets.sh`).
*   **Comandos:** `mc alias set myminio http://localhost:9000 admin password; mc mb myminio/raw-zone`.
*   **Boas práticas:** Particionar dados no S3 no formato `year=YY/month=MM/day=DD/hour=HH` para otimizar queries; comprimir arquivos (Snappy/ZSTD).
*   **Erros comuns:** Spark não consegue escrever por erro de ACL/S3A credentials. *Solução:* Configurar corretamente `fs.s3a.access.key` no `SparkConf`.
*   **Como testar:** Acessar a interface Web do MinIO no `localhost:9001` e verificar os arquivos Parquet.
*   **Critério de conclusão:** Arquivos Parquet sendo gravados de forma contínua pelo Spark com a estrutura de pastas particionada.

---

## 6.6 Motor SQL & Virtualização: Trino

*   **Objetivo:** Fornecer acesso SQL ANSI direto aos arquivos no MinIO.
*   **O que será desenvolvido:** Catálogos (MinIO, Postgres).
*   **Arquivos criados:** `etc/catalog/minio.properties`, `etc/catalog/postgres.properties`.
*   **Comandos:** `trino --server localhost:8080 --catalog minio --schema raw`.
*   **Boas práticas:** Utilizar o Hive Metastore (HMS) para gerenciar as tabelas sobre o MinIO.
*   **Erros comuns:** O Trino não encontra as partições novas. *Solução:* Rodar `CALL system.sync_partition_metadata('schema', 'table', 'FULL')`.
*   **Como testar:** Rodar `SELECT * FROM minio.raw.telemetry LIMIT 10;`.
*   **Critério de conclusão:** Consultas SQL retornando resultados lidos diretamente do Data Lake via Trino.

---

## 6.7 Data Warehouse: PostgreSQL

*   **Objetivo:** Armazenar dados tabulares nas camadas Staging, DWH e Marts.
*   **O que será desenvolvido:** Estrutura DDL, roles e schemas.
*   **Arquivos criados:** `infra/db/init.sql`.
*   **Comandos:** `psql -h localhost -U admin -d data_warehouse -f init.sql`.
*   **Boas práticas:** Separar fisicamente ou logicamente schemas (`raw`, `staging`, `marts`); indexar as chaves estrangeiras.
*   **Erros comuns:** Permissões negadas para o usuário do dbt. *Solução:* Conceder permissões de `USAGE`, `CREATE` no schema adequado.
*   **Como testar:** Conectar via DBeaver/DataGrip e validar as schemas criadas.
*   **Critério de conclusão:** Banco respondendo a conexões com todos os schemas base criados.

---

## 6.8 Transformações ELT: dbt Core

*   **Objetivo:** Transformar, testar e documentar os dados dentro do DWH.
*   **O que será desenvolvido:** Projeto dbt, models, tests, macros.
*   **Arquivos criados:** `dbt_project.yml`, `models/staging/*`, `models/marts/*`, `tests/*`.
*   **Estrutura de diretórios:**
    ```text
    projeto/
    ├── dbt_analytics/
    ```
*   **Comandos:** `dbt debug`, `dbt run`, `dbt test`, `dbt docs generate`.
*   **Boas práticas:** Usar materialized='incremental' para tabelas de fatos grandes; documentar todas as colunas; padronizar nomenclatura (ex: `stg_`, `fct_`, `dim_`).
*   **Erros comuns:** Referências circulares ou tabelas não encontradas. *Solução:* Usar sempre a função `{{ ref('model') }}` e nunca *hard-code* nomes de tabelas.
*   **Como testar:** Rodar `dbt test` para validar uniqueness, not_null e referências.
*   **Critério de conclusão:** `dbt run` completando com sucesso criando todas as views/tables.

---

## 6.9 Orquestração: Apache Airflow

*   **Objetivo:** Agendar e gerenciar a dependência lógica entre os jobs batch/ELT.
*   **O que será desenvolvido:** DAGs em Python.
*   **Arquivos criados:** `dags/dbt_run_dag.py`.
*   **Comandos:** `airflow dags trigger dbt_daily_run`.
*   **Boas práticas:** Tornar as tasks idempotentes; não colocar lógica pesada de processamento na DAG (o Airflow orquestra, não processa).
*   **Erros comuns:** Sensores (Sensors) bloqueando workers (deadlock). *Solução:* Usar `mode='reschedule'` nos sensores.
*   **Como testar:** Acessar Webserver (`localhost:8080`), habilitar a DAG e validar visualmente o sucesso de cada *task*.
*   **Critério de conclusão:** DAG completa (do trigger até o término do dbt run) verde.

---

## 6.10 CI/CD: GitHub Actions

*   **Objetivo:** Automação de testes e deploy da infraestrutura/código.
*   **O que será desenvolvido:** Workflows de pull request e merge.
*   **Arquivos criados:** `.github/workflows/ci.yml`.
*   **Comandos:** N/A (Acionado via Git Push).
*   **Boas práticas:** Fail-fast (rodar linters primeiro); cachear dependências do Python para velocidade; testar modelos do dbt em ambiente *ephemeral* (schema temporário).
*   **Erros comuns:** Falta de secrets no repositório. *Solução:* Configurar via painel do GitHub Settings -> Secrets.
*   **Como testar:** Abrir um Pull Request no repositório e verificar os *checks*.
*   **Critério de conclusão:** Actions completando sem falhas (green checkmark).

<br>

# SEÇÃO 7 - DESENVOLVIMENTO PASSO A PASSO

Nesta seção, dissecamos o código e a configuração real (production-like) que fazem esta arquitetura funcionar, fornecendo as implementações completas.

## 7.1 Ingestão: FastAPI e IoT Simulator

Criaremos uma API que não apenas expõe endpoints, mas inicia uma thread em *background* gerando dezenas de eventos por segundo para simular o IoT.

**`src/api/main.py`**
```python
import asyncio
import json
import random
from datetime import datetime
from fastapi import FastAPI
from pydantic import BaseModel
from kafka import KafkaProducer
import threading
import time

app = FastAPI(title="IoT Telemetry API")

# Modelo de Dados (Schema)
class SensorData(BaseModel):
    device_id: str
    temperature: float
    humidity: float
    pressure: float
    timestamp: str

# Configuração Kafka Producer
def get_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8'),
        retries=3
    )

producer = None

@app.on_event("startup")
async def startup_event():
    global producer
    # Tenta conectar ao Kafka de forma resiliente
    for _ in range(5):
        try:
            producer = get_kafka_producer()
            break
        except Exception:
            time.sleep(5)
    
    # Inicia a thread de simulação
    threading.Thread(target=simulate_iot_devices, daemon=True).start()

def generate_telemetry():
    return {
        "device_id": f"sensor-{random.randint(1, 50)}",
        "temperature": round(random.uniform(15.0, 35.0), 2),
        "humidity": round(random.uniform(40.0, 90.0), 2),
        "pressure": round(random.uniform(980.0, 1050.0), 2),
        "timestamp": datetime.utcnow().isoformat()
    }

def simulate_iot_devices():
    """Worker rodando em background enviando 5 msgs/sec para o Kafka"""
    while True:
        if producer:
            data = generate_telemetry()
            # O device_id é usado como KEY para garantir a ordem no Kafka
            producer.send(
                topic='telemetry_raw',
                key=data["device_id"],
                value=data
            )
        time.sleep(0.2)

@app.post("/ingest")
async def manual_ingest(data: SensorData):
    """Endpoint para injeção manual de dados"""
    if producer:
        producer.send('telemetry_raw', key=data.device_id, value=data.dict())
    return {"status": "success", "message": "Data published"}
```

## 7.2 Mensageria: Kafka no Docker

O Kafka atuará como *backbone*. Usaremos a imagem Kraft (dispensa o ZooKeeper, simplificando a infra).

**Trecho do `docker-compose.yaml` (Kafka & Init):**
```yaml
version: '3.8'
services:
  kafka:
    image: confluentinc/cp-kafka:7.4.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092'
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka:29093'
      KAFKA_LISTENERS: 'PLAINTEXT://kafka:29092,CONTROLLER://kafka:29093,PLAINTEXT_HOST://0.0.0.0:9092'
      KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qk'

  kafka-setup:
    image: confluentinc/cp-kafka:7.4.0
    depends_on:
      - kafka
    entrypoint: [ "bash", "-c" ]
    command: >
      "
      sleep 10 &&
      kafka-topics --create --if-not-exists --bootstrap-server kafka:29092 --partitions 3 --replication-factor 1 --topic telemetry_raw
      "
```

## 7.3 Spark Structured Streaming

Este job ficará escutando o Kafka, interpretando o JSON, adicionando colunas de partição temporal, e gravando no formato Parquet dentro do MinIO S3 de forma *append-only*.

**`src/spark/streaming_job.py`**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, year, month, dayofmonth, hour
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Configurar Spark com S3A e Kafka
spark = SparkSession.builder \
    .appName("IoT_Telemetry_Stream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.hadoop:hadoop-aws:3.3.2") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Definir Schema
schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# 3. Ler Streaming do Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "telemetry_raw") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Transformações
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Parsing de tempo e criação de colunas de particionamento (importante para Data Lake)
enriched_df = parsed_df.withColumn("event_time", to_timestamp(col("timestamp"))) \
    .withColumn("year", year("event_time")) \
    .withColumn("month", month("event_time")) \
    .withColumn("day", dayofmonth("event_time")) \
    .withColumn("hour", hour("event_time"))

# 5. Escrita no MinIO (S3) particionado em Parquet
query = enriched_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .partitionBy("year", "month", "day", "hour") \
    .option("path", "s3a://raw-zone/telemetry/") \
    .option("checkpointLocation", "s3a://raw-zone/checkpoints/telemetry/") \
    .start()

query.awaitTermination()
```

## 7.4 Banco de Dados: PostgreSQL (Staging e DWH)

O Airflow (ou conectores como o Trino) fará a ingestão consolidada de lotes do MinIO para o Postgres. Vamos criar a arquitetura de schemas para o dbt operar.

**`infra/postgres/init.sql`**
```sql
CREATE DATABASE analytics;
\c analytics;

-- Schemas
CREATE SCHEMA raw;     -- Dados espelhados do data lake (se necessário ingestão EL)
CREATE SCHEMA staging; -- Camada transitória para limpeza (dbt)
CREATE SCHEMA marts;   -- Data Warehouse e Modelos Analíticos

-- Criação do usuário do dbt
CREATE USER dbt_user WITH PASSWORD 'dbt_pass';
GRANT ALL ON SCHEMA raw TO dbt_user;
GRANT ALL ON SCHEMA staging TO dbt_user;
GRANT ALL ON SCHEMA marts TO dbt_user;
```

## 7.5 Transformação e Modelagem: dbt Core

O dbt irá materializar tabelas no PostgreSQL criando o Data Warehouse (Dimensional).

**Estrutura de `dbt_project.yml`**
```yaml
name: 'iot_analytics'
version: '1.0.0'
config-version: 2
profile: 'iot_profile'

models:
  iot_analytics:
    staging:
      +materialized: view
      +schema: staging
    marts:
      +materialized: table
      +schema: marts
      core:
        +materialized: incremental
```

**Modelo Incremental (Staging -> Fato) `models/marts/core/fct_telemetry.sql`**
Este modelo demonstra uma carga incremental moderna: apenas os novos registros são processados e inseridos, economizando processamento de banco de dados.

```sql
{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='delete+insert'
    )
}}

WITH source AS (
    SELECT 
        -- Gerando um ID único sintético se não existir
        md5(device_id || timestamp::text) as event_id,
        device_id,
        temperature,
        humidity,
        pressure,
        timestamp as event_time
    FROM {{ source('raw', 'telemetry') }}
)

SELECT *
FROM source

{% if is_incremental() %}
    -- Onde processa apenas os dados mais novos que os existentes na tabela destino
    WHERE event_time > (SELECT MAX(event_time) FROM {{ this }})
{% endif %}
```

## 7.6 Orquestração Batch: Apache Airflow

O Airflow irá acionar o Trino (se usarmos Trino -> Postgres via query) ou acionar o `dbt run` em lotes diários ou horários. Abaixo a DAG de disparo do dbt.

**`dags/dbt_pipeline.py`**
```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dbt_hourly_transformations',
    default_args=default_args,
    description='Executa as transformações do dbt a cada hora',
    schedule_interval='0 * * * *', # Roda a cada hora
    start_date=days_ago(1),
    catchup=False,
    tags=['dbt', 'elt', 'marts'],
) as dag:

    # 1. Executar os modelos staging
    dbt_run_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command='dbt run --select staging --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt',
    )

    # 2. Executar os modelos de fatos (incremental)
    dbt_run_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command='dbt run --select marts --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt',
    )

    # 3. Executar testes de qualidade (data contracts)
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='dbt test --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt',
    )

    # Orquestração das dependências
    dbt_run_staging >> dbt_run_marts >> dbt_test
```

## 7.7 CI/CD e Terraform

### Terraform (Main Provider & Resources)
Usando o provider local Docker para gerenciar containers se necessário, mas na maioria dos cenários reais, usaríamos AWS/GCP. Aqui exemplificamos a sintaxe.

**`infra/terraform/main.tf`**
```hcl
terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0.1"
    }
  }
}

provider "docker" {}

resource "docker_network" "data_network" {
  name = "data_net"
}

# Exemplo de Resource instanciando o Postgres
resource "docker_image" "postgres" {
  name         = "postgres:15-alpine"
  keep_locally = false
}

resource "docker_container" "postgres_db" {
  image = docker_image.postgres.image_id
  name  = "postgres-dwh"
  env   = [
    "POSTGRES_USER=admin",
    "POSTGRES_PASSWORD=admin",
    "POSTGRES_DB=analytics"
  ]
  ports {
    internal = 5432
    external = 5432
  }
  networks_advanced {
    name = docker_network.data_network.name
  }
}
```

### GitHub Actions (Validação de PR)
Este workflow YAML garante que o código SQL do dbt está formatado e não quebra compilação antes de autorizar o Merge para a `main`.

**`.github/workflows/dbt-ci.yml`**
```yaml
name: dbt CI/CD

on:
  pull_request:
    branches:
      - main
    paths:
      - 'dbt_analytics/**'

jobs:
  dbt-tests:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout Repository
        uses: actions/checkout@v3
        
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
          
      - name: Install dependencies
        run: |
          pip install dbt-postgres sqlfluff
          
      - name: SQL Linting (SQLFluff)
        run: sqlfluff lint dbt_analytics/models
        
      - name: dbt Compile
        working-directory: ./dbt_analytics
        run: dbt compile --target dev
```

---
*Este documento reflete a base técnica completa do projeto. A combinação destes componentes garante processamento tolerante a falhas, escalável e com estrito controle de versionamento e governança de dados.*
---

## SEÇÃO 8 - ORGANIZAÇÃO DO REPOSITÓRIO

Para garantir manutenibilidade e escalabilidade, estruturamos o repositório utilizando padrões de projetos de Engenharia de Dados corporativos. A separação clara de responsabilidades (Separation of Concerns) facilita o trabalho de múltiplas equipes no mesmo repositório (monorepo).

```text
projeto-pipeline-etl-semi-real-time/
├── .github/                      # CI/CD: Configurações do GitHub Actions
│   └── workflows/
│       ├── ci.yml                # Workflow de Continuous Integration (lint, testes)
│       └── cd.yml                # Workflow de Continuous Deployment (infra, dbt, airflow)
├── infra/                        # Infraestrutura como Código (IaC) e Containerização
│   ├── docker/                   # Arquivos relacionados ao Docker
│   │   ├── docker-compose.yml    # Orquestração local de todos os serviços
│   │   └── Dockerfile.*          # Dockerfiles customizados (ex: Airflow, Spark)
│   └── terraform/                # Definição de recursos na nuvem (se aplicável)
│       ├── main.tf
│       └── variables.tf
├── src/                          # Código fonte principal do pipeline
│   ├── api/                      # Produtor: Aplicação FastAPI
│   │   ├── main.py               # Ponto de entrada da API
│   │   ├── models/               # Modelos de dados (Pydantic)
│   │   └── routers/              # Rotas da API
│   ├── streaming/                # Consumidor: Scripts Apache Spark Structured Streaming
│   │   ├── jobs/                 # Jobs de ingestão (Kafka -> MinIO Bronze)
│   │   └── utils/                # Funções auxiliares de transformação e conexão
│   └── orchestration/            # Apache Airflow: DAGs e plugins
│       ├── dags/                 # Definição das DAGs de orquestração
│       └── plugins/              # Operadores e hooks customizados
├── transformations/              # Transformações de Dados (dbt Core)
│   ├── dbt_project.yml           # Configuração raiz do projeto dbt
│   ├── models/                   # Modelos SQL (Bronze -> Silver -> Gold)
│   │   ├── staging/              # Camada Staging (views sobre a Bronze)
│   │   ├── marts/                # Modelos de negócios (Silver e Gold)
│   │   └── schema.yml            # Testes e documentação dos modelos
│   ├── macros/                   # Macros Jinja reutilizáveis
│   └── tests/                    # Testes singulares (custom SQL)
├── tests/                        # Testes automatizados (pytest e Great Expectations)
│   ├── unit/                     # Testes unitários para API e Spark
│   ├── integration/              # Testes de integração (API -> Kafka, Spark -> MinIO)
│   └── gx/                       # Great Expectations (Quality checks)
│       └── expectations/         # Suítes de validação de dados
├── monitoring/                   # Observabilidade (Prometheus, Grafana)
│   ├── prometheus/               # Configurações de coleta de métricas
│   │   └── prometheus.yml
│   └── grafana/                  # Dashboards exportados (JSON) e datasources
├── scripts/                      # Scripts utilitários
│   ├── setup_local.sh            # Script de inicialização do ambiente local
│   └── seed_data.py              # Script para popular dados de teste na API
├── .env.example                  # Template de variáveis de ambiente
├── .gitignore                    # Arquivos ignorados pelo Git
├── pyproject.toml                # Dependências Python e config do linting (Poetry/Ruff)
├── README.md                     # Documentação principal de onboarding
└── Makefile                      # Comandos atalho (make up, make test, make lint)
```

**Descrição dos Diretórios:**
- **infra/**: Centraliza tudo que diz respeito a recursos de máquina e deploy local.
- **src/**: Código de engenharia de software aplicado à engenharia de dados (API produtora e Jobs Spark).
- **transformations/**: Focado inteiramente na lógica analítica usando dbt. Em organizações maiores, isso pode ser movido para um repositório separado de Analytics Engineering.
- **tests/** e **monitoring/**: Garantem a estabilidade e visibilidade do sistema (DataOps).

---

## SEÇÃO 9 - TESTES

A qualidade dos dados e a estabilidade do código são mantidas por múltiplas camadas de testes.

### Testes Unitários e de Integração (pytest)
Garantem que pequenos blocos de código funcionem isoladamente e em conjunto.

*Exemplo de Teste Unitário (FastAPI Pydantic Model):*
```python
# tests/unit/test_api_models.py
from src.api.models import TransactionEvent
from pydantic import ValidationError
import pytest

def test_valid_transaction_event():
    event = TransactionEvent(
        transaction_id="123e4567-e89b-12d3-a456-426614174000",
        user_id=999,
        amount=150.50,
        timestamp="2023-10-27T10:00:00Z"
    )
    assert event.amount == 150.50

def test_invalid_negative_amount():
    with pytest.raises(ValidationError):
        TransactionEvent(
            transaction_id="123",
            user_id=999,
            amount=-50.00, # Expected to fail
            timestamp="2023-10-27T10:00:00Z"
        )
```

### Testes do dbt (Transformações)
O dbt é utilizado para testar a integridade relacional e lógicas de negócios nas camadas Silver e Gold.

- **Testes Genéricos (Schema Tests):** Definidos no `schema.yml`. Verificam propriedades fundamentais como `unique`, `not_null`, `accepted_values` e `relationships`.
- **Testes Singulares:** Scripts SQL no diretório `tests/` que devem retornar 0 linhas se o teste passar (ex: verificar se o saldo total não excede o limite).

### Testes de Qualidade com Great Expectations
Focado em Data Quality (DQ). O GX verifica a distribuição estatística e regras de negócio avançadas diretamente nos dados (profiling).
- **Validação na Borda:** Rodado via Spark antes de escrever na Bronze para rejeitar dados corrompidos.
- **Validação Analítica:** Rodado no Airflow após as transformações do dbt para garantir que os relatórios na Gold são confiáveis.
- Exemplo de expectation: `expect_column_values_to_be_between(column="amount", min_value=0, max_value=10000)`

---

## SEÇÃO 10 - MONITORAMENTO

Uma stack de DataOps robusta exige observabilidade total do pipeline, alcançada através do Prometheus (coleta) e Grafana (visualização).

### O Que e Como Monitorar

| Componente | Métricas Chave | Ferramenta / Método |
|------------|----------------|----------------------|
| **Kafka** | Consumer Lag, Throughput (msg/sec), Bytes In/Out, Under Replicated Partitions | JMX Exporter + Prometheus |
| **Spark Streaming** | Input Rate, Processing Rate, Batch Duration, Checkpoint Size, Executor Memory | Spark Metrics HTTP + Prometheus |
| **Airflow** | Task Duration, DAG Failure Rate, Running Tasks, Active DAGs | StatsD / Prometheus Exporter |
| **PostgreSQL** | Active Connections, Slow Queries, Deadlocks, Cache Hit Ratio | PostgreSQL Exporter |
| **MinIO** | Storage Used, HTTP 5xx Errors, S3 Requests/sec | MinIO Prometheus Endpoint |
| **Trino** | Queued Queries, Running Queries, Worker Memory, CPU Time | Trino JMX / Prometheus |
| **Docker/Host** | CPU, Memória, Disco I/O, Network Tx/Rx | Node Exporter + cAdvisor |

### Exemplo de Configuração `prometheus.yml`

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'kafka'
    static_configs:
      - targets: ['kafka-jmx-exporter:7071']

  - job_name: 'spark-streaming'
    metrics_path: '/metrics/prometheus'
    static_configs:
      - targets: ['spark-master:8080']

  - job_name: 'airflow'
    static_configs:
      - targets: ['airflow-statsd:9102']

  - job_name: 'minio'
    metrics_path: '/minio/v2/metrics/cluster'
    static_configs:
      - targets: ['minio:9000']
```

### Métricas de Negócio (Pipeline)
- **Latência End-to-End:** Tempo entre a geração do evento na API e sua disponibilidade na tabela Gold.
- **Throughput do Pipeline:** Volume de dados (GB/hora) ou eventos por segundo processados com sucesso.
- **Qualidade dos Dados:** Porcentagem de registros que falharam nas regras do Great Expectations.

---

## SEÇÃO 11 - BOAS PRÁTICAS

### Arquitetura e Engenharia
- **Idempotência:** Todo job (Spark ou DAG do Airflow) deve poder ser reexecutado múltiplas vezes de forma segura sem duplicar dados. No dbt isso é resolvido usando materializações adequadas (incremental, table). No Spark, pelo uso correto de checkpoints.
- **Separation of Concerns (SoC):** A API apenas gera dados; o Kafka apenas transporta; o Spark apenas ingere; o dbt apenas transforma. Evite acoplar lógica de transformação no job de ingestão.
- **Clean Code e SOLID:** Aplique princípios de engenharia de software na engenharia de dados. Funções UDF pequenas, tipagem forte em Python, e DRY (Don't Repeat Yourself) usando Macros no dbt.

### Dados e Modelagem
- **Medallion Architecture:**
  - **Bronze:** Dados crus, histórico completo, imutáveis (formato JSON ou Parquet via Kafka).
  - **Silver:** Dados limpos, filtrados, normalizados e tipados. Deduplicação e tratamento de nulos.
  - **Gold:** Dados agregados e modelados para o negócio (Star Schema). Pronto para o Superset.
- **Particionamento:** No Data Lake (MinIO), particione a camada Bronze por tempo de ingestão (ex: `ano=2023/mes=10/dia=27`). Evite *over-partitioning* (partições muito pequenas geram problemas de pequenos arquivos).
- **Kimball vs Inmon:** Na camada Gold, adote a modelagem dimensional de Ralph Kimball (Tabelas Fato e Dimensão) para facilitar a leitura por ferramentas de BI e otimizar agregações. Utilize Slowly Changing Dimensions (SCD) Tipo 2 para rastrear histórico de alterações em dimensões (suportado nativamente pelo dbt via snapshots).

### DevOps e Segurança
- **Versionamento:** Use Git Flow ou GitHub Flow. Commits devem seguir o padrão *Conventional Commits* (ex: `feat(api): add transaction validation`).
- **Secrets Management:** NUNCA faça commit de senhas. Use arquivos `.env` localmente ou o GitHub Secrets em CI/CD. Em produção, use HashiCorp Vault ou AWS Secrets Manager.

---

## SEÇÃO 12 - CRONOGRAMA DE IMPLEMENTAÇÃO (12 SEMANAS)

| Semana | Foco | Estudos Recomendados | Implementação | Entregáveis |
|---|---|---|---|---|
| **1-2** | **Infra e Ingestão (API)** | Docker, Redes Virtuais, FastAPI, Pydantic | Setup do `docker-compose.yml`, criação da API FastAPI geradora de dados (Mock). | API rodando e aceitando requisições HTTP locais. |
| **3-4** | **Message Broker** | Apache Kafka, Zookeeper/KRaft, Tópicos, Partições | Subir Kafka no Docker. Integrar a API para atuar como Producer enviando eventos JSON para o Kafka. | Eventos fluindo da API para um tópico do Kafka. |
| **5-6** | **Lake e Streaming** | MinIO (S3 compatível), Apache Spark, Structured Streaming | Subir MinIO e Spark. Criar job Spark para consumir Kafka e salvar no MinIO (Camada Bronze). | Dados brutos sendo salvos no Lake em tempo quase real. |
| **7-8** | **Engine SQL e Transformação** | Trino, dbt Core, Jinja, Modelagem Dimensional | Configurar Trino conectando ao MinIO e Postgres. Iniciar projeto dbt, criar modelos Bronze para Silver. | Camada Silver gerada via dbt executando através do Trino. |
| **9-10** | **Orquestração e Qualidade** | Apache Airflow, DAGs, Great Expectations | Subir Airflow. Criar DAGs para rodar o dbt periodicamente. Integrar GX para validação de dados. | Airflow orquestrando o pipeline batch (Silver/Gold) e report de DQ. |
| **11** | **Visualização (BI)** | Apache Superset, Conexão com Trino/Postgres | Subir Superset. Conectar à camada Gold. Criar datasets e dashboards. | Dashboards funcionais mostrando KPIs de negócios. |
| **12** | **Observabilidade e CI/CD** | Prometheus, Grafana, GitHub Actions | Configurar exporters, criar dashboards de métricas do sistema. Configurar Actions para Linting. | Pipeline totalmente monitorado e CI/CD básico rodando no repositório. |

---

## SEÇÃO 13 - CHECKLIST FINAL DE PROJETO

**Infraestrutura:**
- [ ] Todos os serviços iniciam corretamente via `docker-compose up -d`.
- [ ] Volumes do Docker mapeados corretamente (dados persistem após restart).

**Ingestão e Streaming (EL):**
- [ ] API gera e envia dados para o Kafka sem gargalos (Producer).
- [ ] Spark Streaming consome do Kafka e escreve na Bronze (Consumer).
- [ ] Tratamento de falhas (Checkpoints do Spark configurados).

**Transformação (T) - dbt & Trino:**
- [ ] Conexões do Trino com MinIO e PostgreSQL validadas.
- [ ] Modelos dbt criados para Staging (Silver) e Marts (Gold).
- [ ] Testes genéricos do dbt configurados (`not_null`, `unique`) e passando.

**Orquestração (Airflow):**
- [ ] DAGs agendadas rodando sem falhas.
- [ ] Dependências de tasks definidas corretamente.

**Qualidade e Monitoramento:**
- [ ] Great Expectations rodando e gerando Data Docs.
- [ ] Grafana exibindo painéis com métricas do Kafka, Spark e máquina host.

**Visualização (Superset):**
- [ ] Dashboards criados respondendo a perguntas de negócio com base na Gold.

**Código e Git:**
- [ ] Repositório estruturado.
- [ ] `README.md` claro ensinando como rodar o projeto do zero.
- [ ] Pipeline de CI configurado (flake8/ruff/pytest).

---

## SEÇÃO 14 - EVOLUÇÕES FUTURAS (ENTERPRISE ARCHITECTURE)

Para elevar este projeto pessoal/portfólio ao nível de arquiteturas implementadas em grandes corporações de tecnologia (Big Techs, Unicórnios), os seguintes conceitos e ferramentas devem ser considerados:

### Formatos de Tabela Abertos (Open Table Formats)
**O Que:** Apache Iceberg, Delta Lake ou Apache Hudi.
**Quando:** Assim que a camada Bronze estiver madura e você precisar de operações de UPDATE/DELETE (ACID) diretamente no Data Lake, sem depender do PostgreSQL para a Gold.
**Por Que:** Transformam o Data Lake em um **Lakehouse**, permitindo transações seguras, *time travel* (consultar dados como estavam ontem), e evolução de schema transparente.

### Data Mesh e Descentralização
**O Que:** Abordagem arquitetural e organizacional (Data Mesh).
**Quando:** O time de dados virou o gargalo da empresa e os domínios de negócio querem construir seus próprios produtos de dados.
**Por Que:** Descentraliza a posse dos dados. A engenharia central fornece a infraestrutura (plataforma), mas times como "Pagamentos" ou "Logística" criam e mantém seus próprios pipelines de ponta a ponta.

### Ingestão via Change Data Capture (CDC)
**O Que:** Debezium.
**Quando:** A fonte principal de dados não é uma API de eventos, mas sim o banco de dados transacional (ex: PostgreSQL/MySQL de um microserviço).
**Por Que:** O Debezium lê o log de transações do banco (WAL) e envia cada INSERT/UPDATE/DELETE para o Kafka em tempo real, sem onerar o banco de dados da aplicação em produção.

### Orquestração de Containers e Escalabilidade
**O Que:** Kubernetes (K8s).
**Quando:** O volume de dados ultrapassa o que uma única máquina virtual pode suportar (Docker Compose se torna insuficiente).
**Por Que:** O K8s permite escalar horizontalmente. Se houver pico de dados, ele provisiona mais Pods do Spark ou workers do Airflow dinamicamente.

### Governança, Catálogo e Linhagem de Dados
**O Que:** OpenMetadata ou Apache Atlas.
**Quando:** A empresa tem centenas de tabelas e os analistas perdem tempo perguntando "onde está o dado X?" ou "quem é o dono desta tabela?".
**Por Que:** Cria um portal unificado para descobrir dados, documentar glossários de negócios e visualizar a linhagem (Líneage) de ponta a ponta, entendendo o impacto caso um job falhe.

### Real-Time Analytics (OLAP Rápido)
**O Que:** ClickHouse, Apache Pinot ou Apache Druid.
**Quando:** O negócio exige dashboards que atualizam em tempo real (< 1 segundo de latência) com alta concorrência (milhares de usuários simultâneos no BI).
**Por Que:** Trino é excelente, mas é focado em alta flexibilidade e dados vastos (Datalake). Ferramentas como ClickHouse são bancos OLAP otimizados para ingerir diretamente do Kafka e entregar queries ultrarrápidas, ideal para *user-facing analytics*.

### Alternativas de Mensageria e Fluxo
**O Que:** Apache Pulsar ou Apache NiFi.
**Quando:** Necessidade de multi-tenancy nativo e geo-replicação complexa (Pulsar) ou necessidade de uma ferramenta visual low-code para movimentar dados entre sistemas heterogêneos rapidamente (NiFi).
**Por Que:** O Pulsar separa armazenamento e computação na mensageria, resolvendo dores de operação do Kafka em altíssima escala. O NiFi acelera a criação de fluxos de ingestão simples sem escrever código.
