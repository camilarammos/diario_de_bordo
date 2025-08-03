# 🚖 Diário de Bordo - Pipeline de Processamento com PySpark

Projeto desenvolvido para transformar e estruturar dados de um app de transporte privado, organizando-os em camadas de um pipeline de dados com foco em qualidade, resiliência e escalabilidade.

---

## 🧠 Objetivo do Projeto

A partir de um arquivo CSV contendo corridas de transporte, o pipeline realiza:

1. Ingestão e padronização dos dados (camada **raw**)
2. Limpeza e enriquecimento (camada **silver**)
3. Agregações e geração de métricas (camada **gold**)
4. Persistência dos dados no MySQL
5. Validação com testes automatizados usando Pytest

---

## 🏗️ Arquitetura do Projeto

```bash
diario_de_bordo/
├── data/
│   ├── raw/         # CSV original + Parquet bruto
│   ├── silver/      # Dados limpos e normalizados
│   └── gold/        # Métricas agregadas finais
│
├── docker-compose.yml
├── Dockerfile
├── main.py          # Pipeline principal
├── jars/            # Conector JDBC do MySQL
│   └── mysql-connector-j-8.3.0.jar
│
├── src/
│   ├── raw_layer.py
│   ├── silver_layer.py
│   ├── gold_layer.py
│   └── utils.py     # Persistência no MySQL
│
└── tests/
    ├── test_raw_layer.py
    ├── test_silver_layer.py
    ├── test_gold_layer.py
    └── conftest.py  # Fixture SparkSession para testes
```

---

## 🚀 Tecnologias Utilizadas

- Python 3.13
- PySpark 3.5
- Docker e Docker Compose
- MySQL 8
- Adminer (interface para banco)
- Pytest

---

## 📦 Como executar o projeto

### 1. Clone o repositório

```bash
git clone https://github.com/camilarammos/diario_de_bordo.git
cd diario_de_bordo
```

### 2. Caso necessite baixar o conector JDBC do MySQL

```bash
mkdir -p jars
wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.3.0/mysql-connector-j-8.3.0.jar -P jars/
```

> O JAR será montado no contêiner em `/opt/spark/jars/`.

---

### 3. Suba o ambiente

```bash
docker-compose up --build -d
```

- Isso inicia:
  - Spark
  - MySQL
  - Adminer (porta 8080)

---

### 4. Execute o pipeline

Entre no contêiner do Spark:

```bash
docker exec -it spark bash
```

Execute:

```bash
python main.py
```

Você verá logs de execução e amostra dos dados.
---

## 🧪 Executando os testes

Ainda dentro do contêiner:

```bash
python -m pytest tests/ -v
```
---

## 🛠️ Detalhes por camada

### 🔹 Raw

- Leitura do `.csv`
- Persistência como Parquet
- Persistência no MySQL (tabela `raw_info_transportes`)

### 🔸 Silver

- Conversão e normalização de datas
- Limpeza de campos
- Preenchimento de valores ausentes
- Persistência em Parquet e MySQL (`silver_info_transportes`)

### 🟡 Gold

- Agrupamento por data (`DT_REFE`)
- Métricas: média, máximos, contagens, filtragens
- Escrita como `.parquet`, `.csv` e persistência MySQL (`info_corridas_do_dia`)

---

## ⚙️ Conexão com o MySQL

A persistência ocorre via `DataFrame.write.jdbc()` com uso do driver JDBC no contêiner.

Com tolerância a falhas e log de erro.

---

## ✅ Resiliência e boas práticas

- Dados organizados em camadas raw → silver → gold
- Tratamento de erros
- Pipeline modular com logs
- Testes automatizados e isolados

---

## 👤 Credenciais (padrão de teste)

| Sistema   | Usuário | Senha     |
|-----------|---------|-----------|
| MySQL     | ce      | senha123  |
| Adminer   | ce      | senha123  |

---
## ✨ Exemplo de saída (gold)

### Tabelas criadas no MySQL (`db_ce`)

Base de dados db_ce

Tabelas:

| Tables_in_db_ce         |
|-------------------------|
| info_corridas_do_dia    |
| raw_info_transportes    |
| silver_info_transportes |

### Amostra de dados

| DT_REFE    | QT_CORR | QT_CORR_NEG | QT_CORR_PESS | VL_MAX_DIST | VL_MIN_DIST | VL_AVG_DIST | QT_CORR_REUNI | QT_CORR_NAO_REUNI |
|------------|---------|-------------|--------------|-------------|-------------|-------------|----------------|--------------------|
| 2022-01-01 | 20      | 12          | 8            | 2.2         | 0.7         | 1.1         | 6              | 10                 |

---

## 👨‍💻 Autor

Desenvolvido por **Camila Ramos de Oliveira** (https://github.com/camilarammos)

