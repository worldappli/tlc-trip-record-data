# 🚖 TLC Trip Record Data – End-to-End Big Data Platform

## 📌 Project Overview

This project demonstrates a production-like **Data Engineering platform** for processing the New York City TLC Trip Record dataset.

The platform integrates **AWS cloud services, Apache Airflow, Spark, Docker and PostgreSQL** to ingest, process, store and analyze large-scale taxi trip data.

---

## 🏗 Architecture

```text
NYC TLC Dataset
       │
       ▼
AWS S3
       │
       ▼
Glue Data Catalog
       │
       ▼
Airflow (Docker on EC2)
       │
 ┌─────┴─────┐
 │           │
 ▼           ▼
Python     Spark
 │           │
 └─────┬─────┘
       ▼
Parquet
       ▼
Athena
       ▼
PostgreSQL (RDS)
       ▼
Apache Superset
```

---

## 🛠 Technologies Used

- Apache Airflow
- Docker
- Apache Spark / PySpark
- Python / Pandas
- PostgreSQL
- AWS EC2
- AWS S3
- AWS Glue
- AWS Athena
- AWS RDS
- Apache Superset
- Parquet

---

## ✨ Key Features

- Automated workflow orchestration with Airflow
- Containerized deployment with Docker
- Batch processing of large datasets
- Out-of-core data processing
- Parquet optimization for analytics
- AWS Glue catalog integration
- SQL analytics with Athena
- Dashboard-ready data in PostgreSQL

---

## 🚀 Pipeline Workflow

1. Upload TLC data to S3
2. Register dataset in Glue Catalog
3. Trigger Airflow DAG
4. Process data with Spark and Python
5. Write optimized Parquet files
6. Query data with Athena
7. Load curated results into PostgreSQL
8. Visualize results with Superset

---

## 📊 Example Analytics

- Trip count by hour
- Average fare amount
- Passenger count distribution
- Pickup location statistics
- Daily trip trends

---

## 🖥 Deployment

### Start services

```bash
docker compose up -d
```

### Trigger Airflow DAG

Open:

```text
http://localhost:8080
```

---

## 📸 Screenshots

See the `screenshots/` folder for:

- Airflow DAG execution
- AWS Glue tables
- Athena queries
- EC2 deployment
- Superset dashboards

---

## ⚠ Challenges & Solutions

| Challenge | Solution |
|-----------|----------|
| PyArrow installation issues | Installed compatible wheel version |
| S3 permission errors | Updated IAM policy for Glue/Athena |
| Parquet schema mismatch | Enforced explicit schema before writing |
| Large file memory usage | Implemented batch/out-of-core processing |

---

## 📚 What I Learned

- Distributed processing with Spark
- Workflow orchestration with Airflow
- Containerized data platforms with Docker
- AWS data lake architecture
- Query optimization with Athena
- Production-style ETL design

---

## 🔮 Future Improvements

- Kafka streaming ingestion
- EMR cluster execution
- CI/CD pipeline
- Terraform infrastructure
- Kubernetes deployment

---

## 👤 Author

**Ruben KLOUTSE**  
Software & Data Engineer  
GitHub: https://github.com/worldappli
