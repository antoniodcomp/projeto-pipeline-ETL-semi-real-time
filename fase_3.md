# Direcionamento para a Implementação da Fase 3

O job de processamento *Spark Structured Streaming* atuará como o consumidor central da arquitetura em tempo real. Ele vai se inscrever nos tópicos do Kafka para consumir os eventos JSON contínuos gerados pelo Replayer, aplicar o *parsing* com um schema definido e limpar os dados. Por fim, fará a escrita em micro-lotes (micro-batches) de forma apensada e particionada (por ano/mês/dia/hora) no formato Parquet (com compressão Snappy) diretamente no *MinIO*, transformando-o na nossa *Raw Zone* (Data Lake) de alta performance.

---

### Esboço de Organização de Pastas para a Fase 3

Esta estrutura separa as responsabilidades do Spark, garantindo código limpo e facilidade no deploy:

```text
src/
└── spark_jobs/
    ├── __init__.py
    ├── config.py           # Instanciação do SparkSession, configurações do S3A (MinIO) e brokers Kafka
    ├── schemas.py          # Definição do StructType para validação e extração dos campos JSON
    ├── transformations.py  # Regras de negócio, limpeza e geração das colunas de particionamento de data
    └── streaming_job.py    # Fluxo principal: readStream (Kafka) -> transformações -> writeStream (MinIO)

infra/
└── docker/
    └── Dockerfile.spark    # Dockerfile customizado (ex: bitnami/spark) adicionando jars do Kafka e Hadoop-AWS
```

---

### Passo a Passo Simplificado da Fase 3

**1. Preparação da Imagem Docker (Infra)**
* Crie o `Dockerfile.spark` (por exemplo, estendendo a imagem `bitnami/spark:3`).
* Garanta que as bibliotecas e Jars essenciais para o conector Kafka (`spark-sql-kafka-0-10`) e para o conector S3 (`hadoop-aws`) estejam listados na submissão do job ou na imagem.

**2. Configuração do MinIO (Storage)**
* No seu `docker-compose.yml`, suba o serviço do MinIO.
* Acesse a interface dele e crie um *bucket* chamado `raw-zone`.
* Garanta que o Kafka (da Fase 2) está rodando e recebendo os dados do seu Replayer FastAPI.

**3. Leitura dos Dados em Streaming (Read)**
* Em `config.py`, instancie a `SparkSession` e declare as configurações da AWS (`fs.s3a.endpoint`, `access.key`, `secret.key`) apontando para a porta do MinIO local.
* Em `schemas.py`, monte o `StructType` que representa exatamente a estrutura do JSON que sai do Replayer.
* Em `streaming_job.py`, inicie a leitura com `spark.readStream.format("kafka")`, se conectando ao broker e tópico corretos.

**4. Parse e Limpeza (Transform)**
* Os eventos do Kafka chegam numa coluna bruta em formato binário (`value`).
* Faça um `cast` dessa coluna para *String* e aplique a função `from_json()` passando o schema criado no passo anterior para explodir o JSON em colunas normais.
* Extraia do timestamp as colunas virtuais `year`, `month`, `day` e `hour`. Isso será vital para a próxima etapa.

**5. Escrita no Data Lake (Write)**
* Usando as transformações aplicadas, chame o `writeStream.format("parquet")`.
* Use `.partitionBy("year", "month", "day", "hour")` para que o Spark crie automaticamente subpastas baseadas nas datas (evitando gargalos de leitura no futuro).
* Defina a opção obrigatória de `checkpointLocation` (salvando o progresso da leitura em uma pasta separada, o que garante tolerância a falhas).
* Dê o comando de *start* apontando o caminho de destino (`s3a://raw-zone/telemetria/`) e aguarde com `awaitTermination()`.

**6. Validação do Sucesso**
* Sem olhar para o código: apenas abra o console web do MinIO.
* Navegue até o bucket `raw-zone`. Se você vir pastas sendo formadas em estrutura de árvore (ex: `year=2026/month=08/day=14/hour=10/`) contendo arquivos de extensão `.parquet.snappy`, a fase 3 está concluída com excelência.
