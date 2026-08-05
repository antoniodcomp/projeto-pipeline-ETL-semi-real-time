# Fase 1 — Infraestrutura Base e Simulador IoT

## Guia de Desenvolvimento Textual Detalhado

> **Objetivo geral da fase:** Estabelecer o alicerce de infraestrutura do projeto (containers Docker) e criar a fonte produtora de dados — uma API FastAPI configurada como um **Replayer de Dataset** que iterará sobre dados reais (dataset UCI Household Electric Power Consumption) e entregará continuamente dados JSON, simulando um ambiente IoT de medidores inteligentes (smart meters).

---

## Visão Panorâmica — Passo a Passo em Alto Nível

Antes de mergulhar nos detalhes, é fundamental compreender a sequência lógica das atividades que compõem esta fase. Cada atividade foi ordenada de modo que a anterior forneça a base necessária para a próxima:

1. **Planejar a estrutura de diretórios do projeto** — Definir a organização de pastas que abrigará todo o ecossistema.
2. **Definir o modelo de dados da telemetria** — Decidir quais campos, tipos e intervalos realistas os dados simulados terão.
3. **Criar o serviço FastAPI do simulador IoT** — Desenvolver a aplicação Python que expõe endpoints REST gerando dados de telemetria.
4. **Containerizar o serviço FastAPI** — Escrever o Dockerfile que empacota a aplicação em um container.
5. **Criar o arquivo Docker Compose base** — Orquestrar o container do simulador, configurar a rede virtual e preparar a estrutura para os futuros serviços.
6. **Testar e validar a fase** — Garantir que tudo funciona como esperado.

---

## Passo 1 — Planejamento da Estrutura de Diretórios do Projeto

### Objetivo da atividade

Criar uma organização de pastas profissional que acomode não apenas o simulador IoT, mas toda a evolução futura do projeto (Kafka, Spark, dbt, Airflow, monitoramento, etc.). Uma boa estrutura de diretórios é o primeiro ato de arquitetura do projeto.

### Por que essa atividade deve ser realizada

Sem uma estrutura pensada desde o início, o repositório cresce de forma caótica. Cada nova fase adicionará serviços, configurações e código. Se a organização não for planejada agora, o retrabalho futuro será enorme — reorganizar pastas depois que já existem referências cruzadas entre Docker Compose, volumes e imports é custoso e propenso a erros.

### Como ela se encaixa no fluxo geral do projeto

Esta é a fundação. Todos os demais artefatos (docker-compose, Dockerfiles, código, configurações de Airflow, modelos dbt, etc.) serão criados dentro desta estrutura. É o esqueleto do monorepo.

### Raciocínio por trás da decisão

Pense no projeto como um monorepo onde cada serviço vive em seu próprio diretório. Dentro da raiz do projeto, você precisará de:

- Um diretório dedicado ao serviço do simulador IoT (que conterá o código Python, o Dockerfile, o arquivo de dependências e quaisquer configurações específicas desse serviço).
- Um diretório (ou conjunto de diretórios) que no futuro abrigará os demais serviços: Kafka, Spark, dbt, Airflow, Trino, etc.
- O arquivo `docker-compose.yml` na raiz do projeto, pois ele orquestra todos os serviços.
- Diretórios auxiliares para documentação, scripts utilitários, variáveis de ambiente e arquivos de infraestrutura como código (Terraform, no futuro).

A separação por serviço (em vez de separação por tipo de arquivo) é o padrão mais comum em projetos de engenharia de dados, pois permite que cada serviço seja independente, testável isoladamente e, no futuro, potencialmente implantável de forma independente.

### Erros comuns nesta etapa e como evitá-los

- **Colocar tudo na raiz do projeto:** Misturar o Dockerfile do simulador, os scripts do Spark, os modelos do dbt e as DAGs do Airflow na raiz cria confusão imediata. Sempre isole cada serviço.
- **Nomes genéricos demais:** Evite nomes como `app/`, `src/`, `service/` sem contexto. Prefira nomes descritivos como `iot-simulator/`, `spark-streaming/`, `dbt-transform/`.
- **Não criar o `.gitignore` desde o início:** Arquivos temporários do Python (`__pycache__`, `.pyc`), ambientes virtuais (`venv/`), arquivos `.env` com segredos e dados gerados devem ser ignorados pelo Git desde o primeiro commit.
- **Esquecer o arquivo `.env`:** Variáveis de ambiente (portas, credenciais, nomes de serviço) devem ficar em um arquivo `.env` na raiz, não hardcoded no docker-compose.

### Como verificar que a atividade foi concluída com sucesso

Você deve ter uma árvore de diretórios clara e compreensível. Qualquer pessoa que olhe para a raiz do projeto deve entender imediatamente que se trata de um projeto de data engineering com múltiplos serviços. Deve existir um diretório específico para o simulador IoT, com subpastas para o código-fonte e para o Dockerfile. O arquivo `docker-compose.yml` deve existir na raiz (pode estar vazio ou com a versão mínima neste ponto). Um `.gitignore` adequado deve estar presente.

---

## Passo 2 — Definição do Modelo de Dados da Telemetria (Adaptado para UCI Dataset)

### Objetivo da atividade

Projetar o schema dos dados que o simulador/replayer irá mapear e expor a partir do dataset original. Isso significa decidir como os campos lidos do dataset da UCI serão estruturados, transformados e enriquecidos antes de serem entregues no formato JSON.

### Por que essa atividade deve ser realizada

O arquivo original pode ter colunas como texto delimitado e metadados que não se encaixam perfeitamente na simulação do nosso streaming. Se o modelo de dados for mal estruturado agora, todas as etapas seguintes (ingestão no Kafka, processamento no Spark, modelagem no dbt, visualização no Superset) sofrerão. Validar e mapear o modelo antes de escrever código evita retrabalho e garante a coesão no processamento.

### Como ela se encaixa no fluxo geral do projeto

Este modelo de dados enriquecido será o contrato entre o produtor (replayer) e todos os consumidores downstream. O schema servirá como base para as tabelas no PostgreSQL e no Spark. Portanto, as decisões de tipagem tomadas aqui se refletirão por todo o pipeline.

### Raciocínio por trás da decisão

Como escolhemos utilizar o dataset da UCI sobre Consumo Elétrico Residencial, o modelo original trará os seguintes campos: `Date`, `Time`, `Global_active_power`, `Global_reactive_power`, `Voltage`, `Global_intensity`, e os medidores `Sub_metering_1`, 2 e 3.

Para adaptar ao nosso cenário IoT semi-real-time, seu schema final deve ser expandido e conter:

- **Identificação da Residência:** Um novo campo `house_id`. Como o dataset é de uma única casa, o seu *Replayer* terá a responsabilidade de clonar e injetar diversos IDs (ex: de 1 a 500) para criar o volume necessário que justifique a infraestrutura distribuída do projeto.
- **Métricas Operacionais (Mapeadas):** 
  - Os valores numéricos de potência, voltagem e intensidade descritos acima. 
  - Atenção especial ao casting correto, já que no dataset CSV/TXT muitos deles vêm como *string* e os nulos costumam ser indicados com `?`.
- **Metadados temporais:**
  - O dataset contém as medições no passado (a partir de 2006). Para simular um streaming contínuo de forma eficaz para o Spark, o Replayer deve injetar um `current_timestamp` (ISO 8601 com timezone) correspondente ao momento do disparo, enquanto o timestamp original pode ser preservado para fins comparativos.

Use o Pydantic para definir esse modelo. O Pydantic não é apenas para validação — ele força você a pensar rigorosamente sobre tipos, nulos permitidos e serialização, ajudando a limpar e estruturar o dado oriundo do arquivo original já na fonte.

### Erros comuns nesta etapa e como evitá-los

- **Esquecer de enriquecer os dados (house_id):** Se você apenas espelhar o dataset original, terá dados de apenas 1 medidor (casa). A mágica da simulação está em escalar esse único conjunto de dados para centenas de medidores simultâneos, o que mais para frente testará a resiliência do seu cluster.
- **Ignorar valores nulos do Dataset:** O dataset UCI possui cerca de 1,25% de linhas com medições faltantes (sinalizadas com `?`). Se o schema Pydantic não prever isso (com `Optional` e conversão correta), a API falhará silenciosamente no meio da execução contínua.
- **Não versionar o schema:** Documente qual é a versão 1.0 do schema. Quando evoluir os campos, o histórico ficará claro.

### Como verificar que a atividade foi concluída com sucesso

Você deve ter um modelo Pydantic escrito e documentado. Ao processar uma linha de amostra do arquivo TXT/CSV da UCI e passar ao modelo, o resultado deve ser um JSON bem tipado, contendo o `house_id` adicionado, valores nulos tratados, métricas numéricas convertidas e o `timestamp` atual simulado.

---

## Passo 3 — Criação do Serviço FastAPI do Simulador IoT

### Objetivo da atividade

Desenvolver a aplicação Python que expõe uma API REST capaz de gerar, sob demanda ou continuamente, registros de telemetria simulados no formato JSON.

### Por que essa atividade deve ser realizada

O simulador IoT é a **fonte de dados** de todo o pipeline. Sem ele, não há dados para ingerir no Kafka, processar no Spark, transformar no dbt ou visualizar no Superset. Em projetos reais, essa fonte seria um sistema SCADA, um broker MQTT ou uma API de terceiros. No nosso caso, criamos nossa própria fonte controlada, o que nos dá total flexibilidade para testar cenários específicos.

### Como ela se encaixa no fluxo geral do projeto

O simulador é o primeiro bloco do fluxo:

```
[Simulador IoT / FastAPI] → [Kafka] → [Spark Streaming] → [MinIO] → [dbt] → [PostgreSQL] → [Superset]
```

Ele será o produtor que, nas fases seguintes, publicará dados nos tópicos do Kafka. Nesta fase, ele funciona de forma standalone — respondendo a requisições HTTP. Na fase do Kafka, ele será adaptado para também publicar mensagens diretamente nos tópicos.

### Raciocínio por trás da decisão

A aplicação FastAPI deve expor pelo menos os seguintes endpoints (neste modo inicial de testes REST):

1. **Endpoint de extração/teste único:** Um endpoint que extrai e converte uma ou mais linhas recentes do dataset da UCI sob demanda, servindo como uma visão simples dos dados tipados com injeção do `house_id` para debugging rápido.
2. **Endpoint de health check:** Um endpoint simples que retorna o status do serviço.
3. **Replay contínuo (background):** Este será o coração do Replayer IoT. Trata-se de uma tarefa que, ao ser iniciada, itera sequencial e infinitamente sobre as linhas do CSV original. Para cada linha original, converte os dados, clona em múltiplos `house_id` (simulando centenas de medidores), atualiza o timestamp e joga os resultados para o log ou expõe, a uma taxa de processamento ditada via configuração (ex.: ler a próxima linha a cada segundo). No momento, ela pode apenas printar; depois integrará com o Kafka.

Sobre a organização interna do código:

- **Separe responsabilidades:** O modelo de dados (Pydantic) deve estar em um módulo separado. A lógica do "Replayer" (que lê o arquivo, controla a iteração e mapeia em memória) em outro. As rotas em si, em um terceiro. Isso permite refatorações fáceis no futuro.
- **Configure via variáveis de ambiente:** O intervalo de envio do replayer, o multiplicador de medidores residenciais simulados (`HOUSE_MULTIPLIER`), a porta do servidor e o caminho do arquivo de dataset bruto — tudo deve ser configurável no arquivo `.env`.
- **Documentação automática:** Aproveite o Swagger/OpenAPI nativo do FastAPI documentando seus endpoints de start/stop da simulação.

### Erros comuns nesta etapa e como evitá-los

- **Não usar `async`:** O FastAPI é um framework assíncrono. Se você escrever funções síncronas (`def` em vez de `async def`), elas funcionarão, mas não aproveitarão a capacidade assíncrona do framework. Para endpoints que geram dados em memória (sem I/O externo), ambos funcionam bem, mas adote `async def` como hábito.
- **Hardcodar configurações:** Colocar `port=8000`, `num_turbines=10` direto no código impede a customização via Docker Compose. Use o Pydantic Settings (BaseSettings) para carregar configurações de variáveis de ambiente com valores default.
- **Não validar a saída:** Mesmo sendo você quem gera os dados, valide-os com o modelo Pydantic antes de retorná-los. Isso garante integridade e serve como documentação viva.
- **Esquecer o CORS (Cross-Origin):** Se futuramente você quiser acessar a API de um frontend ou do Superset, o CORS deve estar configurado. Embora não seja crítico nesta fase, é uma boa prática adicioná-lo desde já.
- **Não estruturar a resposta com metadados:** Retornar apenas uma lista de dicionários é funcional, mas adicionar metadados (timestamp da geração, quantidade de registros, versão do schema) torna a API mais profissional e facilita debugging.

### Como verificar que a atividade foi concluída com sucesso

A aplicação deve iniciar localmente sem erros. Ao acessar a URL da documentação automática (Swagger UI), todos os endpoints devem estar listados com suas descrições. O endpoint de replay pontual (teste de extração) deve retornar um JSON válido com a modelagem do Pydantic perfeitamente encaixada nas colunas da base da UCI. O endpoint de health check deve retornar status 200. E o gerador contínuo em background deve conseguir varrer o dataset convertendo e multiplicando os dados sem travamentos ou out-of-memory.

---

## Passo 4 — Containerização do Serviço FastAPI

### Objetivo da atividade

Escrever o Dockerfile que empacota a aplicação FastAPI em uma imagem Docker, de modo que o serviço possa ser executado como um container isolado, reprodutível e portável.

### Por que essa atividade deve ser realizada

O Docker é a tecnologia que permite que todos os serviços do pipeline (simulador, Kafka, Spark, PostgreSQL, MinIO, etc.) coexistam no mesmo ambiente sem conflitos de dependências. Containerizar o simulador agora garante que ele funcionará exatamente da mesma forma em qualquer máquina, independentemente do sistema operacional ou das bibliotecas instaladas.

### Como ela se encaixa no fluxo geral do projeto

O container do simulador será o primeiro serviço definido no `docker-compose.yml`. Todos os demais serviços seguirão o mesmo padrão. A capacidade de subir e derrubar o simulador com um único comando do Docker Compose é o que torna o projeto reproduzível.

### Raciocínio por trás da decisão

Ao escrever o Dockerfile, considere os seguintes aspectos:

**Escolha da imagem base:** Use uma imagem Python oficial e slim (como `python:3.11-slim`). Imagens "slim" são menores e mais seguras que as completas, pois contêm apenas o necessário. Evite imagens "alpine" com Python, pois elas frequentemente causam problemas com pacotes que possuem extensões em C (como algumas dependências do uvicorn).

**Multi-stage build (opcional nesta fase, mas recomendado):** Em um multi-stage build, você teria um estágio de "build" onde instala as dependências e compila o necessário, e um estágio de "runtime" que copia apenas o resultado final. Para uma aplicação FastAPI simples, isso é opcional, mas demonstra maturidade técnica no portfólio.

**Gerenciamento de dependências:** As dependências Python devem estar listadas em um arquivo `requirements.txt` (ou, para projetos mais sofisticados, em um `pyproject.toml` com Poetry ou similar). O Dockerfile deve copiar esse arquivo e instalar as dependências antes de copiar o código-fonte. Isso aproveita o cache de camadas do Docker — se as dependências não mudarem, essa camada não será reconstruída, acelerando o build.

**Ordem das instruções no Dockerfile:** A ordem importa para cache. A sequência ideal é:
1. Definir a imagem base.
2. Definir o diretório de trabalho.
3. Copiar o arquivo de dependências.
4. Instalar as dependências.
5. Copiar o código-fonte.
6. Expor a porta.
7. Definir o comando de inicialização.

**Usuário não-root:** Por segurança, crie um usuário não privilegiado dentro do container e execute a aplicação com ele. Rodar como root é uma vulnerabilidade conhecida.

**Health check no Dockerfile:** Adicione uma instrução `HEALTHCHECK` que verifica periodicamente se a API está respondendo. Isso permite que o Docker (e o Docker Compose) saibam se o container está saudável, o que será útil para orquestração de dependências entre serviços.

### Erros comuns nesta etapa e como evitá-los

- **Copiar o código antes das dependências:** Isso invalida o cache de dependências a cada mudança no código-fonte, tornando o build desnecessariamente lento.
- **Não usar `.dockerignore`:** Sem um `.dockerignore`, o Docker copia tudo para o contexto de build — incluindo `venv/`, `__pycache__/`, `.git/` e outros diretórios irrelevantes. Isso infla o tamanho da imagem e torna o build mais lento.
- **Usar `latest` como tag da imagem base:** Isso pode causar builds irreproducíveis. Fixe a versão (ex: `python:3.11-slim` em vez de `python:slim`).
- **Não expor a porta correta:** Se a aplicação roda na porta 8000, o Dockerfile deve expor essa porta com `EXPOSE 8000`. A falta dessa instrução não impede o funcionamento, mas é uma boa prática de documentação.
- **Instalar dependências de desenvolvimento em produção:** O container não precisa de `pytest`, `black`, `flake8`. Use `--no-dev` ou separe as dependências em grupos.

### Como verificar que a atividade foi concluída com sucesso

A imagem deve ser construída sem erros. Ao executar o container isoladamente (sem Docker Compose, apenas Docker), a API deve estar acessível na porta configurada. A documentação Swagger deve abrir normalmente. O health check deve reportar o container como saudável. O tamanho da imagem deve ser razoável (menos de 200-300 MB para uma aplicação FastAPI simples).

---

## Passo 5 — Criação do Docker Compose Base

### Objetivo da atividade

Criar o arquivo `docker-compose.yml` que orquestra o container do simulador IoT, define a rede virtual compartilhada e estabelece a infraestrutura base sobre a qual todos os futuros serviços serão adicionados.

### Por que essa atividade deve ser realizada

O Docker Compose é o "maestro" que coordena todos os serviços. Sem ele, seria necessário subir cada container manualmente, configurar redes, volumes e variáveis de ambiente via linha de comando — algo impraticável com mais de 2-3 serviços. O Docker Compose permite declarar toda a infraestrutura como código (Infrastructure as Code), tornando o ambiente reproduzível com um único comando.

### Como ela se encaixa no fluxo geral do projeto

Este será o arquivo central do projeto. A cada nova fase, novos serviços serão adicionados a ele: Kafka na fase 2, Spark na fase 3, MinIO, PostgreSQL, dbt, Airflow, Trino, Superset, Prometheus, Grafana — todos orquestrados por este arquivo. Portanto, a estrutura definida agora precisa ser escalável.

### Raciocínio por trás da decisão

**Versão do Compose:** Use a especificação mais recente do Docker Compose (sem a diretiva `version:`, que foi depreciada). O Docker Compose moderno infere automaticamente a versão.

**Definição do serviço do simulador:** O serviço deve incluir:

- **Build context:** Aponte para o diretório do simulador onde está o Dockerfile.
- **Nome do container:** Defina um nome explícito (ex: `iot-simulator`) para facilitar a identificação nos logs e nas redes.
- **Portas:** Mapeie a porta interna do container para uma porta no host, permitindo acesso externo à API para testes.
- **Variáveis de ambiente:** Configure via arquivo `.env` ou diretamente no compose. Prefira o arquivo `.env` para separar configuração de definição.
- **Health check:** Referencie o health check definido no Dockerfile ou defina um no Compose. Isso garante que o Docker Compose saiba quando o serviço está pronto, o que será essencial quando outros serviços dependerem dele.
- **Restart policy:** Use `restart: unless-stopped` para que o serviço reinicie automaticamente em caso de falha, simulando um ambiente de produção.

**Definição da rede:** Crie uma rede virtual nomeada (ex: `pipeline-network`) do tipo `bridge`. Todos os serviços futuros serão conectados a esta mesma rede. Em uma rede Docker, os containers se comunicam entre si usando o nome do serviço como hostname — ou seja, o container do Kafka poderá acessar o simulador pelo hostname `iot-simulator`. Isso elimina a necessidade de IPs hardcoded.

**Definição de volumes (preparação futura):** Embora o simulador em si talvez não precise de volumes persistentes, este é o momento de criar a seção de volumes no Compose. Nas fases seguintes, o MinIO, PostgreSQL e Airflow precisarão de volumes. Deixar a estrutura pronta economiza tempo.

**Arquivo `.env`:** Crie um arquivo `.env` na raiz do projeto contendo todas as variáveis configuráveis: portas, nomes de serviços, intervalos de geração, número de turbinas. O Docker Compose lê automaticamente o arquivo `.env` da mesma pasta.

### Erros comuns nesta etapa e como evitá-los

- **Não definir redes explicitamente:** O Docker Compose cria uma rede default, mas ela tem nome gerado automaticamente (baseado no diretório do projeto). Defina uma rede nomeada para ter controle e previsibilidade.
- **Conflito de portas:** Se a porta 8000 já estiver em uso na sua máquina (por outra aplicação ou outro container), o serviço falhará ao subir. Verifique antes de definir o mapeamento de portas e use variáveis de ambiente para permitir customização.
- **Esquecer o `depends_on` com `condition`:** Quando adicionar serviços que dependem de outros (ex: o Kafka Producer depende do Kafka estar saudável), use `depends_on` com `condition: service_healthy` em vez do simples `depends_on` sem condição. Nesta fase isso ainda não é necessário (há apenas um serviço), mas a consciência dessa funcionalidade é importante.
- **Build context errado:** Se o Dockerfile estiver em `./iot-simulator/` mas o build context apontar para `./`, o Docker enviará todo o repositório como contexto, inflando o build. O build context deve ser o diretório mais específico possível.
- **Não usar `docker-compose down -v` ao limpar:** Volumes órfãos consomem disco e podem causar comportamentos inesperados. Acostume-se a limpar completamente ao reconstruir o ambiente.

### Como verificar que a atividade foi concluída com sucesso

Ao executar o Docker Compose, o serviço do simulador deve ser construído (build) e iniciado sem erros. Os logs do container devem mostrar o servidor FastAPI rodando. A API deve ser acessível na porta mapeada, tanto via navegador (documentação Swagger) quanto via ferramentas de teste de API. O container deve estar listado como "healthy". A rede nomeada deve estar visível na listagem de redes Docker.

---

## Passo 6 — Testes e Validação da Fase

### Objetivo da atividade

Executar uma bateria de verificações para garantir que todos os componentes entregues nesta fase estão funcionando corretamente e que a infraestrutura está pronta para receber os serviços das próximas fases.

### Por que essa atividade deve ser realizada

Cada fase do projeto é uma fundação para as próximas. Se o simulador IoT não estiver gerando dados corretamente, ou se a rede Docker não estiver configurada adequadamente, os problemas se propagarão e serão muito mais difíceis de diagnosticar nas fases seguintes (imagine debugar por que o Spark não recebe dados do Kafka quando, na verdade, o problema é que o simulador está produzindo JSON malformado).

### Como ela se encaixa no fluxo geral do projeto

Esta é a "porta de qualidade" da fase. Só avance para a Fase 2 (integração com Kafka) quando todas as verificações listadas aqui estiverem aprovadas. Em projetos corporativos, isso equivaleria a uma revisão de code review e a testes de aceitação antes de um merge.

### Raciocínio por trás da decisão

A validação deve cobrir múltiplas camadas:

**1. Validação da infraestrutura Docker:**
- O Docker Compose sobe sem erros?
- O container está rodando e saudável?
- A rede nomeada existe e o container está conectado a ela?
- Os logs do container não apresentam erros ou warnings críticos?
- O container reinicia corretamente após uma falha simulada (mate o processo principal e verifique se o restart policy o reinicia)?

**2. Validação da API:**
- A documentação Swagger está acessível e lista todos os endpoints?
- O endpoint de health check retorna status positivo?
- O endpoint de geração única retorna um JSON válido com todos os campos do schema?
- O endpoint de geração em lote retorna a quantidade correta de registros?
- Os dados gerados são realistas? (valores dentro das faixas esperadas, correlações entre campos respeitadas)
- Chamadas consecutivas geram dados diferentes? (não está retornando dados estáticos)
- O timestamp dos registros é atualizado a cada geração?

**3. Validação do modelo de dados:**
- Todos os campos obrigatórios estão presentes em cada registro?
- Os tipos de dados estão corretos? (números são números, strings são strings, o timestamp é ISO 8601)
- Os valores estão dentro das faixas realistas definidas?
- Os campos de identificação da turbina são variados (múltiplas turbinas, não apenas uma)?

**4. Validação de resiliência:**
- O que acontece se o container for reiniciado? Ele volta a gerar dados normalmente?
- O que acontece se a porta estiver ocupada? A mensagem de erro é clara?
- Os logs são informativos o suficiente para diagnosticar problemas?

**5. Validação da qualidade do código (preparação para o portfólio):**
- O código está organizado em módulos separados (modelos, simulador, rotas)?
- Existe um README explicando como executar o serviço?
- As variáveis de ambiente estão documentadas?
- O `.gitignore` e o `.dockerignore` estão configurados?
- A imagem Docker tem um tamanho razoável?

### Erros comuns nesta etapa e como evitá-los

- **Testar apenas o caminho feliz (happy path):** Não teste apenas se "funciona". Teste o que acontece quando dá errado — porta ocupada, parâmetros inválidos, container reiniciado.
- **Não verificar os logs:** Um container pode estar "running" mas logando erros silenciosamente. Sempre inspecione os logs após subir os serviços.
- **Não testar de dentro de outro container:** Nesta fase não há outro container, mas acostume-se a pensar em testes de conectividade inter-container. Quando o Kafka entrar, ele precisará acessar o simulador via nome de serviço na rede Docker.
- **Pular a validação por pressa:** É tentador avançar para o Kafka quando a API "parece funcionar". Invista tempo na validação agora — isso economizará horas de debugging depois.

### Como verificar que a atividade foi concluída com sucesso

Todas as verificações listadas acima devem passar. A lista de entregáveis da fase deve estar completa:

- ✅ Estrutura de diretórios criada e organizada.
- ✅ Modelo de dados da telemetria definido com Pydantic.
- ✅ API FastAPI funcionando com todos os endpoints.
- ✅ Dockerfile otimizado e construindo sem erros.
- ✅ `docker-compose.yml` subindo o serviço com rede configurada.
- ✅ Dados gerados são realistas e coerentes.
- ✅ Health check funcionando.
- ✅ Documentação mínima (README) presente.

---

## Considerações Finais sobre a Fase 1

### O que você deve saber ao final desta fase

- Como estruturar um projeto de engenharia de dados usando monorepo.
- Como modelar dados de IoT usando Pydantic.
- Como criar APIs REST com FastAPI, incluindo endpoints assíncronos e geração de dados simulados.
- Como escrever um Dockerfile otimizado, com cache de camadas, imagem slim, usuário não-root e health check.
- Como usar o Docker Compose para orquestrar serviços, definir redes e gerenciar variáveis de ambiente.
- Como validar que a infraestrutura base está pronta para as próximas fases.

### O que NÃO fazer nesta fase

- **Não se preocupe com Kafka ainda.** O simulador será adaptado para publicar no Kafka na Fase 2. Nesta fase, ele apenas responde a requisições HTTP.
- **Não complique o modelo de dados excessivamente.** 10-15 campos são suficientes. Você pode evoluir o schema em fases posteriores.
- **Não otimize prematuramente.** O foco é ter uma base funcional, limpa e bem documentada — não a aplicação mais performática possível.
- **Não pule a documentação.** Um README básico com instruções de como subir o ambiente e uma descrição dos endpoints transforma o projeto de "um exercício" em "um portfólio profissional".

### Preparação para a Fase 2

Ao concluir esta fase, você terá o núcleo do seu Replayer IoT criado, fazendo parser do dataset original da UCI e escalando-o em memória, pronto dentro do Docker Compose. Na **Fase 2**, essa engrenagem será conectada ao Apache Kafka — no lugar de (ou além de) jogar num simples log HTTP, o script passará a varrer o dataset e enviar os blocos de dados clonados diretamente como mensagens (eventos) para tópicos do Kafka, unindo a imprevisibilidade simulada à fidelidade de uma série temporal real. Portanto, ao projetar o serviço hoje, encare-o como a semente de um **Produtor (Producer)** de alto tráfego.

---

> *Este documento foi elaborado como guia textual de mentoria técnica. Ele descreve o raciocínio, a sequência de desenvolvimento e os conhecimentos necessários para implementar a Fase 1 de forma autônoma, sem fornecer código ou comandos prontos.*
