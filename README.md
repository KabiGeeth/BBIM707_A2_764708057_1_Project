# BBIM707 — Advanced Data Engineering Portfolio

End-to-end data engineering pipeline over the Online Retail II dataset, enriched with
country reference data — covering ETL, distributed storage, and real-time streaming.

## Overview
- **Part A — ETL (pandas):** extract from two sources (CSV file + SQLite database),
  clean and transform both, integrate into a unified table, and analyse it.
- **Part B — Big data (Docker):** ingest into HDFS, store/query with Hive + MongoDB,
  stream through Kafka, and cluster transactions in real time with Spark Structured Streaming.

## Tech stack
pandas, SQLite, Hadoop HDFS, Hive, MongoDB, Apache Kafka, Apache Spark (Structured Streaming), Docker

## Data sources (download separately - not included, too large)
- Online Retail II: https://www.kaggle.com/datasets/mashlyn/online-retail-ii-uci
- Countries of the World: https://www.kaggle.com/datasets/fernandol/countries-of-the-world

## Repository structure
- Part A notebook: ETL and analysis
- Part B notebook: Kafka producer, MongoDB load, scripts that generate the Docker files
- bbim707-hadoop/: HDFS + Hive Docker setup
- bbim707-streaming/: Kafka + Spark Docker setup and the streaming job

## Author
Kabishcka Geethanjan - BBIM707 Advanced Data Engineering, 2026
