Solicitação para a Fase 1

Com base na descrição da Fase 3, elabore um guia de desenvolvimento textual detalhado explicando passo a passo tudo o que deve ser feito para atingir o objetivo desta etapa.

### Fase 4: Modelagem e Carga no Data Warehouse
- **Objetivo:** Preparar a base de consumo analítico.
- **Entregáveis:** PostgreSQL provisionado, scripts em Python ou ferramenta de orquestração (Airflow) para puxar os dados do MinIO e carregar em uma tabela de Staging no Postgres (extensão do ELT).
- **Tecnologias:** PostgreSQL, Pandas/Psycopg2 (para scripts de carga) ou Airflow Operators.
- **Pré-requisitos:** Conhecimentos de Modelagem Relacional e SQL.
- **Conceitos:** Staging Tables, Copy commands, Idempotência.
- **Tempo Estimado:** 3 dias.
- **Dificuldade:** Médio.
- **Resultado Esperado:** Dados fluindo do MinIO (Raw) para a tabela de staging (Bronze/Silver) no DW.

