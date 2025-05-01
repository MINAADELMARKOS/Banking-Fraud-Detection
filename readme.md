# 🚨 Banking Fraud Detection Pipeline (Real-Time, Dockerized)

This project is a fully containerized **real-time banking fraud detection system** built on a modern big data stack. It simulates **ATM transactions**, detects fraudulent activity using **Apache Spark MLlib**, and visualizes results using **Power BI**. All components are orchestrated using **Docker Compose**.

---

## 📌 Key Features

- Real-time ingestion of ATM transactions using **Apache Kafka**
- Stream processing and ML model inference using **Apache Spark Streaming**
- Raw and processed data stored in **HDFS** (Bronze/Silver Zones)
- Automated data flow from **HDFS to HBase** via **Apache NiFi**
- Visualization of fraud detection insights in **Power BI**
- Fully containerized using **Docker & Docker Compose**

---

## 📊 Architecture Overview

<center>
<img src="/Architecture/Picture2.png" alt="" width="990px" height="450px">
</center>

---

## 🔧 Tools

| Layer                      | Tools Used                                                                 |
|---------------------------|-----------------------------------------------------------------------------|
| **Data Ingestion**        | Apache Kafka, Zookeeper, ATM Transaction Simulator (Python)           |
| **Data Processing**       | Apache Spark Streaming, Spark MLlib, HDFS                                  |
| **ETL & Feature Engineering** | PySpark / Scala, Spark SQL                                              |
| **Data Orchestration**    | Apache NiFi, HDFS → HBase (via PutHBaseJSON)                               |
| **Data Storage**          | HDFS (Bronze/Silver), HBase                                                |
| **Visualization**         | Power BI (external)                                   |
| **Infrastructure**        | Docker, Docker Compose, Docker Network                                     |





## ⚙️ Kafka Configuration
Broker: 1

Zookeeper: 1

Topics:

raw_transactions: Raw ATM data

cleaned_transactions: Processed (fraud-flagged) data

ATM Simulator:

Sends synthetic transactions to raw_transactions

Built using Python (can be extended to Java)

## 💡 Spark ML Pipeline
Ingestion: Stream from Kafka → HDFS

Bronze Layer: Raw ATM transactions written to /data/bronze/raw

Silver Layer: Cleaned + transformed data written to /data/silver/processed

ML Model: Spark MLlib pipeline (e.g., Logistic Regression, Random Forest)

Output: Flagged fraud transactions pushed to Kafka cleaned_transactions topic

<center>
<img src="/spark/spark01.png" alt="" width="990px" height="450px">
</center>

<center>
<img src="/spark/spark02.png" alt="" width="990px" height="450px">
</center>

<center>
<img src="/spark/spark03.png" alt="" width="990px" height="450px">
</center>


## 🔄 NiFi Flow
Uses ListHDFS → FetchHDFS → ConvertRecord → PutHBaseJSON

Loads silver-layer JSON records into HBase

Table: fraud_transactions

Rowkey: transaction_id

<center>
<img src="/nifi/nifi-01.png" alt="" width="990px" height="450px">
</center>


<center>
<img src="/nifi/nifi-02.png" alt="" width="990px" height="450px">
</center>




## 📊 Power BI Dashboard
Connects to HBase or Spark SQL via JDBC/ODBC

KPIs Visualized:

Real-time fraud counts

Top fraudulent accounts/locations

Anomaly trends over time


## 📊 Cover
<center>
<img src="/powerbi/Cover.png" alt="" width="990px" height="450px">
</center>

## 📊 Dashboard

<center>
<img src="/powerbi/Dashboard.png" alt="" width="990px" height="450px">
</center>



