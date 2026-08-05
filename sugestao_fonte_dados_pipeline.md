# Sugestão de Fontes de Dados Reais para o Pipeline ELT

Com base na arquitetura e objetivos definidos no **documento_projeto.md**, substituir o gerador de dados sintéticos por um *dataset* real eleva significativamente o nível do projeto. Utilizar dados públicos reais traz desafios comuns do dia a dia da Engenharia de Dados, como valores nulos, *outliers*, e a necessidade de *parsing* adequado.

Abaixo, apresento 4 opções de conjuntos de dados (focados em séries temporais, IoT e eventos) que se encaixam perfeitamente na arquitetura proposta (Kafka + Spark Streaming + MinIO + dbt + Trino/PostgreSQL).

---

## 1. Wind Turbine SCADA Dataset (Kaggle)

* **Nome do dataset:** Wind Turbine Scada Dataset
* **Fonte e link oficial:** [Kaggle - Wind Turbine Scada Dataset](https://www.kaggle.com/datasets/berkerisen/wind-turbine-scada-dataset)
* **Contexto do problema:** Dados reais do sistema SCADA de uma turbina eólica operando na Turquia. O objetivo principal costuma ser a previsão de geração de energia e identificação de anomalias com base na velocidade do vento e direção.
* **Principais atributos disponíveis:** `Date/Time` (Timestamp), `LV ActivePower (kW)`, `Wind Speed (m/s)`, `Theoretical_Power_Curve (KWh)`, `Wind Direction (°)`.
* **Frequência dos dados:** A cada 10 minutos (agregações SCADA).
* **Volume aproximado de registros:** ~50.000 registros (1 ano de dados).
* **Formato dos arquivos:** CSV.
* **Licença de uso:** CC0 (Public Domain).
* **Nível de qualidade dos dados:** Alta, mas com algumas lacunas temporais, *outliers* e momentos onde a turbina não gerou a energia esperada para a velocidade do vento.
* **Vantagens e limitações:**
  * **Vantagens:** Mantém a coerência temática com o projeto inicial (telemetria de turbinas). É fácil de entender e permite cálculos analíticos diretos (ex: diferença entre energia real e teórica).
  * **Limitações:** O volume é pequeno para testar estresse de infraestrutura e a frequência de 10 minutos é um pouco baixa para simular um *streaming* de alta latência, a menos que os dados sejam "acelerados" no simulador.
* **Integração na arquitetura:** 
  * O arquivo CSV seria lido por um script Python (atuando como o *Producer* do Kafka). O script enviaria os registros para um tópico Kafka em formato JSON, simulando o comportamento de sensores em tempo real.
  * O Spark Streaming leria do Kafka e gravaria no MinIO, particionando por `Ano/Mês/Dia`.
* **Adaptações para Streaming:**
  * Será necessário criar um script (ex: em FastAPI ou script puro) que leia o CSV histórico e efetue o *replay* dos dados para o Kafka, por exemplo, enviando 1 registro por segundo, com o *timestamp* sendo atualizado para a hora atual do sistema (ou mantendo o original se o pipeline suportar processamento retroativo).

---

## 2. New York City Taxi and Limousine Commission (TLC) Trip Record Data

* **Nome do dataset:** NYC Taxi TLC Trip Record Data
* **Fonte e link oficial:** [NYC TLC - Trip Record Data](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) ou via AWS Open Data.
* **Contexto do problema:** Registros detalhados de todas as viagens de táxi (amarelo e verde) e veículos de aplicativo em Nova York. Útil para análises de tráfego, previsão de demanda, cálculos de tarifas médias por rota e análises espaço-temporais.
* **Principais atributos disponíveis:** `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `passenger_count`, `trip_distance`, `PULocationID`, `DOLocationID`, `payment_type`, `fare_amount`, `tip_amount`.
* **Frequência dos dados:** Dados gerados a cada viagem finalizada (alta cardinalidade, milhares por minuto na vida real).
* **Volume aproximado de registros:** Mais de 1 bilhão de registros (histórico de anos). Pode chegar a dezenas de GB por mês em Parquet.
* **Formato dos arquivos:** Parquet (historicamente CSV).
* **Licença de uso:** Dados públicos do governo (Open Data).
* **Nível de qualidade dos dados:** Média/Alta. Contém anomalias interessantes como viagens de 0 km, valores negativos, erros de GPS e *timestamps* absurdos.
* **Vantagens e limitações:**
  * **Vantagens:** É o "Hello World" robusto da Engenharia de Dados. Tem volume suficiente para testar limites de cluster e particionamento pesado.
  * **Limitações:** Não é puramente "IoT/Sensores", sendo mais um dataset transacional. Pode ser excessivamente grande se não for filtrado para rodar em *containers* locais.
* **Integração na arquitetura:** 
  * O arquivo Parquet pode ser consumido em lotes pelo produtor Kafka e despachado continuamente para simular eventos ocorrendo. 
  * O dbt poderia ser utilizado para criar dimensões de zonas (`PULocationID`) e fatos de faturamento.
* **Adaptações para Streaming:**
  * Criar um produtor Kafka que ordene o dataset por `pickup_datetime` e dispare eventos proporcionalmente à diferença de tempo original, ou a uma taxa fixa (ex: 500 mensagens por segundo) para estressar o Spark Streaming.

---

## 3. Individual Household Electric Power Consumption (UCI)

* **Nome do dataset:** Individual household electric power consumption Data Set
* **Fonte e link oficial:** [UCI Machine Learning Repository](https://archive.ics.uci.edu/ml/datasets/individual+household+electric+power+consumption)
* **Contexto do problema:** Medições de consumo de energia elétrica de uma residência na França coletadas ao longo de quase 4 anos. O objetivo é análise de padrões de consumo, previsão de carga e detecção de anomalias.
* **Principais atributos disponíveis:** `Date`, `Time`, `Global_active_power`, `Global_reactive_power`, `Voltage`, `Global_intensity`, `Sub_metering_1`, `Sub_metering_2`, `Sub_metering_3`.
* **Frequência dos dados:** Amostragem de 1 em 1 minuto.
* **Volume aproximado de registros:** 2.075.259 registros (~130 MB).
* **Formato dos arquivos:** Arquivo texto separado por ponto e vírgula (TXT/CSV).
* **Licença de uso:** Open Data (ODC-BY).
* **Nível de qualidade dos dados:** Alta, porém cerca de 1,25% das instâncias possuem valores faltantes, o que é ótimo para testar resiliência no pipeline (ETL).
* **Vantagens e limitações:**
  * **Vantagens:** Cenário clássico de IoT (medidores inteligentes/smart meters). Volume perfeitamente gerenciável localmente no Docker, mas grande o suficiente para justificar particionamento e *streaming*.
  * **Limitações:** Refere-se a apenas uma residência. Para tornar o projeto mais corporativo, o script gerador precisaria "clonar" e adicionar *ruído* aos dados para simular 100 ou 1000 residências.
* **Integração na arquitetura:** 
  * Similar aos anteriores, servirá como *seed* para um *Kafka Producer*.
* **Adaptações para Streaming:**
  * O produtor lerá o arquivo linha a linha. Como o original é de 1 em 1 minuto, o script de *replay* pode iterar sobre o arquivo e injetar o campo `house_id` simulando que as medições estão vindo de milhares de casas simultaneamente, multiplicando a taxa de ingestão para o Kafka.

---

## 4. NASA CMAPSS Turbofan Engine Degradation Simulation

* **Nome do dataset:** Turbofan Engine Degradation Simulation Data Set
* **Fonte e link oficial:** [NASA Ames Prognostics Data Repository](https://data.nasa.gov/Aerospace/CMAPSS-Jet-Engine-Simulated-Run-to-Failure-Dataset/5b9c-vjvq)
* **Contexto do problema:** Simulação de sensores de motores turbofan de aeronaves (temperatura, pressão, velocidade do fan). Cada motor inicia saudável e degrada até a falha. O objetivo clássico é prever o RUL (*Remaining Useful Life*) para manutenção preditiva.
* **Principais atributos disponíveis:** `unit_number` (id do motor), `time_in_cycles`, 3 configurações operacionais e 21 medições de sensores.
* **Frequência dos dados:** Baseado em ciclos de operação (pode ser mapeado para séries temporais).
* **Volume aproximado de registros:** Centenas de milhares de linhas divididas em conjuntos de treino e teste.
* **Formato dos arquivos:** Arquivos de texto (TXT/CSV).
* **Licença de uso:** Domínio Público (EUA).
* **Nível de qualidade dos dados:** Alta qualidade e perfeitamente estruturado, pois foi gerado por simulação de alta fidelidade física.
* **Vantagens e limitações:**
  * **Vantagens:** Cenário industrial (manutenção preditiva) extremamente realista. Excelente para justificar *pipelines* analíticos complexos e *Machine Learning* no final do funil.
  * **Limitações:** Requer um entendimento básico do domínio para criar painéis (dashboards) que façam sentido de negócio. O tempo não é medido em relógio (timestamp), mas em ciclos, exigindo uma transformação para simular "tempo real".
* **Integração na arquitetura e Adaptações para Streaming:**
  * O produtor Kafka deverá adicionar um *timestamp* atual sintético mapeando os "ciclos" para "segundos/minutos". Exemplo: disparar os sensores do ciclo 1 para 100 motores, aguardar 1 segundo, disparar o ciclo 2, etc.

---

## Tabela Comparativa

| Critério | Wind Turbine SCADA (Kaggle) | NYC Taxi Data | Household Power (UCI) | NASA Turbofan (CMAPSS) |
| :--- | :--- | :--- | :--- | :--- |
| **Facilidade de implementação** | Muito Fácil | Médio (Devido ao alto volume) | Fácil | Médio (Devido aos *cycles*) |
| **Realismo dos dados** | Alto | Muito Alto | Alto | Muito Alto |
| **Complexidade** | Baixa | Alta | Média | Alta |
| **Potencial Eng. de Dados** | Médio (poucos dados) | Muito Alto (Volume, anomalias) | Alto (Time series, valores nulos) | Muito Alto (Manutenção Preditiva) |
| **Adequação ao Pipeline proposto**| Perfeita (IoT/SCADA) | Boa (Eventos) | Perfeita (Smart Meters) | Perfeita (IoT Industrial) |
| **Valor para Portfólio** | Bom | Excelente | Muito Bom | Excelente |

---

## Recomendação Final e Justificativa

**A melhor opção para o seu projeto de portfólio é o dataset: "Individual Household Electric Power Consumption (UCI)"**, com uma pequena modificação na camada de ingestão.

### Justificativa Técnica e de Negócio:

1. **Aderência ao Padrão IoT e Semi-Real-Time:** O dataset é a representação perfeita de telemetria de sensores de *smart meters* (medidores inteligentes), que é um dos casos de uso mais comuns para arquiteturas baseadas em Kafka + Spark Streaming. Diferente dos táxis de NY, que são eventos pontuais, medidores enviam pulsos em frequência fixa.
2. **Desafios Reais Inclusos:** Ele possui cerca de 1% de dados nulos espalhados na base. Isso exigirá que no dbt (ou no Spark) você implemente lógica de tratamento de qualidade (imputação de dados ou filtragem), o que é excelente para demonstrar conhecimentos com *Great Expectations* e modelagem robusta.
3. **Escalabilidade Simulada:** O dataset possui ~2 milhões de linhas de uma única casa. O "pulo do gato" para o seu portfólio será o seu gerador Kafka. Em vez de criar um *FastAPI* que gera números aleatórios, você criará um **Python Kafka Producer** que lê este arquivo CSV, clona cada linha atribuindo, por exemplo, 500 IDs de casas diferentes, e publica isso em *streaming*.
   * **Por que isso é incrível para o portfólio?** Isso simulará um ambiente recebendo milhares de medições por segundo. Você verá na prática a importância do **particionamento no Kafka** e do **checkpointing e triggers no Spark Structured Streaming**, justificando o uso do MinIO em formato Parquet/Snappy e o uso do dbt para consolidações em "Hora/Dia".
4. **Viabilidade Local:** Apesar de podermos escalá-lo via script, o dataset original cabe tranquilamente na RAM, não travando o seu cluster de *containers* local com o Docker Compose (como ocorreria baixando o histórico completo dos Táxis de NY).

### Como adaptar o Passo 1 (Fase 1) com essa escolha?
O container *FastAPI Simulador* idealizado inicialmente torna-se um **Replayer de Dataset**. Ele vai fazer o download do dataset da UCI (ou carregar de uma pasta `/data`), iterar sobre as linhas cronologicamente, converter de TXT/CSV para JSON, adicionar metadados (como *house_id* e o timestamp atual do sistema) e publicar continuamente no Kafka ou expor via rota API, unindo o melhor dos mundos: a previsibilidade de dados reais e o estresse de infraestrutura do *streaming*.
