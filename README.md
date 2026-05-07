# Real-Time Log Processing Pipeline

An end-to-end data engineering pipeline that generates synthetic website logs, streams them in real time through **Confluent Cloud Kafka**, indexes them into **Elasticsearch**, orchestrates the workflow with **Apache Airflow**, and automates deployment to **AWS S3** via **GitHub Actions**.

---

## Architecture

```
┌─────────────────┐     ┌─────────────────────┐      ┌─────────────────┐
│   Log Producer  │────▶│  Confluent Cloud    │────▶│  Log Consumer   │
│ (Faker + Python)│     │  Kafka Topic        │      │  + Parser       │
└─────────────────┘     └─────────────────────┘      └────────┬────────┘
                                                              │
                                                              ▼
                                                   ┌─────────────────────┐
                                                   │    Elasticsearch    │
                                                   │  (Index & Search)   │
                                                   └─────────────────────┘
                                                              
┌─────────────────────────────────────────────────────────────────────┐
│                        Apache Airflow                               │
│              (Orchestrates producer & consumer DAGs)                │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                     GitHub Actions CI/CD                            │
│              (Auto-sync DAGs and code to AWS S3)                    │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| **Log Generation** | Python, Faker |
| **Message Streaming** | Apache Kafka (Confluent Cloud) |
| **Data Indexing** | Elasticsearch (Elastic Cloud) |
| **Orchestration** | Apache Airflow (MWAA) |
| **Secrets Management** | AWS Secrets Manager |
| **CI/CD** | GitHub Actions |
| **Storage** | AWS S3 |

---

## Project Structure

```
realtime-logs-processing/
├── dags/
│   ├── logs_producer.py          # Generates and streams logs to Kafka
│   └── logs_processing_pipeline.py  # Consumes logs and indexes to Elasticsearch
├── .github/
│   └── workflows/
│       └── s3_sync.yml           # GitHub Actions CI/CD workflow
├── .gitignore
├── requirements.txt
└── README.md
```

---

## How It Works

### 1. Log Production
The producer generates **15,000 synthetic website logs** per run using the `Faker` library, simulating real traffic patterns with:
- IP addresses
- HTTP methods (GET, POST, PUT, DELETE)
- Endpoints and status codes
- User agents and referrers

Each log is serialized and streamed to a **Confluent Cloud Kafka topic**.

### 2. Log Consumption & Indexing
The consumer continuously polls the Kafka topic, parses each raw log string using a **regex pattern**, and batches them into groups of 15,000 before performing a **bulk insert** into Elasticsearch for efficient indexing.

### 3. Orchestration
Both the producer and consumer are wrapped as **Apache Airflow DAGs**, allowing scheduled execution, monitoring, retries, and dependency management through the Airflow UI.

### 4. CI/CD
Every push to the `main` branch triggers a **GitHub Actions workflow** that automatically syncs the latest DAGs and code to an **AWS S3 bucket**, which MWAA uses as its DAG source.

---

## Secrets Management

All sensitive credentials are stored in **AWS Secrets Manager** and fetched at runtime. No credentials are hardcoded or committed to the repository.

Secrets used:
- `KAFKA_BOOTSTRAP_SERVER`
- `KAFKA_SASL_USERNAME`
- `KAFKA_SASL_PASSWORD`
- `ELASTIC_SEARCH_URL`
- `ELASTIC_SEARCH_API_KEY`

---

## Getting Started

### Prerequisites
- Python 3.10+
- Docker (recommended for running Airflow locally)
- AWS account with Secrets Manager access
- Confluent Cloud account
- Elastic Cloud account

### Installation

**1. Clone the repository**
```bash
git clone https://github.com/your-username/realtime-logs-processing.git
cd realtime-logs-processing
```

**2. Create and activate virtual environment**
```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# Linux/Mac
source .venv/bin/activate
```

**3. Install dependencies**
```bash
pip install -r requirements.txt
```

**4. Configure AWS credentials**
```bash
aws configure
```

**5. Run the producer**
```bash
python dags/logs_producer.py
```

**6. Run the consumer**
```bash
python dags/logs_processing_pipeline.py
```

---

## Running Airflow Locally (Docker)

```bash
# Download official Airflow docker-compose
curl -o docker-compose.yaml https://airflow.apache.org/docs/apache-airflow/2.9.0/docker-compose.yaml

# Initialize
docker-compose up airflow-init

# Start
docker-compose up -d
```

Open `http://localhost:8080` — login with `airflow / airflow`.

---

## Requirements

```
apache-airflow
confluent-kafka
elasticsearch
faker
boto3
```

---

## License

MIT License — feel free to use and adapt this project.
