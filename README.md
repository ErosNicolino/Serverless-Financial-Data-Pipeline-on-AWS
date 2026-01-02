# 📈 AWS Serverless Financial Data Pipeline

![Python](https://img.shields.io/badge/Python-3.12-blue?style=for-the-badge&logo=python&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-Serverless-orange?style=for-the-badge&logo=amazon-aws&logoColor=white)
![Spark](https://img.shields.io/badge/Apache_Spark-PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Status](https://img.shields.io/badge/Status-Concluído-success?style=for-the-badge)

---

## 📌 Visão Geral

Pipeline completo de Engenharia de Dados **100% Serverless na AWS**, orientado a eventos, para ingestão, processamento e análise de dados financeiros da **B3 (Bolsa de Valores Brasileira)**.

Projeto desenvolvido com foco em **escalabilidade**, **baixo custo operacional**, **Data Lake moderno** e **Analytics em tempo quase real**, utilizando serviços gerenciados da AWS.

---

## 🎯 Objetivos do Projeto

- 📥 Ingestão automática de dados financeiros diários
- 🗂️ Organização de Data Lake (Bronze / Silver)
- 🔄 Processamento ETL escalável com Apache Spark
- 📊 Cálculo de indicadores financeiros
- 🔍 Consulta SQL Serverless via Amazon Athena

---

## 🧩 Ativos Processados

- Ações: PETR4, VALE3, ITUB4
- Índice: IBOVESPA

---

## 🏗️ Arquitetura da Solução

![Diagrama de Arquitetura](assets/Diagrama.png)

### 🔄 Fluxo do Pipeline

| Etapa     | Serviço            | Descrição                         |
| --------- | ------------------ | --------------------------------- |
| Ingestão  | AWS Lambda         | Coleta dados da API Yahoo Finance |
| Trigger   | S3 Events          | Dispara automaticamente o ETL     |
| ETL       | AWS Glue (PySpark) | Limpeza, tipagem e cálculos       |
| Data Lake | Amazon S3          | Camadas Raw e Refined             |
| Catálogo  | Glue Crawler       | Atualiza schema e partições       |
| Analytics | Amazon Athena      | Consultas SQL Serverless          |

---

## 📊 Evidências de Execução

### 📁 Data Lake (Amazon S3)

![S3](assets/S3%20Final.png)

### 🔄 Processamento ETL (AWS Glue)

![Glue](assets/ETL%20jobs%20Final.png)

### 🗃️ Catalogação (Glue Crawler)

![Crawler](assets/Crawlers%20Final.png)

### 🔍 Consulta Final (Athena)

![Athena](assets/AWS%20Resultado%20Final.png)

---

## 📂 Estrutura do Repositório

```
aws-serverless-stocks/
├── assets/
├── datalake/
├── src_aws/
│   ├── glue_etl.py
│   ├── lambda_ingestion.py
│   └── lambda_trigger.py
├── src_local/
│   ├── ingestao_local.py
│   ├── etl_pandas.py
│   └── validacao.py
├── requirements.txt
└── README.md
```

---

## 🚀 Execução Local

```bash
pip install -r requirements.txt
python src_local/ingestao_local.py
python src_local/etl_pandas.py
python src_local/validacao.py
```

---

## 📝 Query de Validação (Athena)

```sql
SELECT
    symbol,
    data_particao,
    dias_desde_pregao,
    valor_fechamento,
    volume_negociado,
    media_movel_7d,
    valor_total_negociado
FROM refined
WHERE symbol = 'PETR4'
ORDER BY data_particao DESC
LIMIT 10;
```

---

## 👨‍💻 Autor

**Eros Nicolino da Rocha**
Projeto desenvolvido como **Tech Challenge – Engenharia de Dados (2026)**
