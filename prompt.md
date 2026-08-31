Solicitação para a Fase 1

Com base na descrição do documento **@fase_4.md**, elabore um guia de desenvolvimento textual detalhado explicando passo a passo tudo o que deve ser feito para atingir o objetivo desta etapa.

### Fase 5: Transformações com dbt (Data Build Tool)
- **Objetivo:** Aplicar regras de negócio e criar Data Marts agregados dentro do PostgreSQL utilizando dbt.
- **Entregáveis:** Projeto dbt inicializado, modelos staging (limpeza de tipos), modelos dimensionais (Dimensões de turbina) e modelos Fato (Fato_Geracao). Testes de schema configurados.
- **Tecnologias:** dbt-core, dbt-postgres.
- **Pré-requisitos:** PostgreSQL rodando com dados na staging.
- **Conceitos:** Jinja Templating, DAGs, Materializações (Table, View, Incremental), Testes de Qualidade.
- **Tempo Estimado:** 3 a 4 dias.
- **Dificuldade:** Médio.
- **Resultado Esperado:** Linhagem de dados completa no `dbt docs`. Tabelas agregadas por hora prontas para BI.

