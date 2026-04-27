# 🚕 NYC Taxi Batch ETL Pipeline

A production-grade batch ETL pipeline processing **2.7M NYC Yellow Taxi records** using Google Cloud Platform.

## 🏗️ Architecture

Cloud Scheduler (daily 2AM)
↓
Cloud Function 1 — Ingest
↓
GCS Bucket (raw storage)
↓
Cloud Function 2 — Transform
↓
Apache Beam / Dataflow
↓
BigQuery (partitioned table)
↓
Looker Studio Dashboard

## ⚡ Pipeline Stats
| Metric | Value |
|--------|-------|
| Records Processed | 2,723,762 |
| Data Quality | 91.8% valid rows |
| Processing Time | ~2 minutes |
| Cost | $0/month |
| Schedule | Daily 2:00 AM EST |

## 🛠️ Tech Stack
| Service | Purpose |
|---------|---------|
| Cloud Scheduler | Daily cron trigger |
| Cloud Functions | Serverless Python |
| GCS | Raw file storage |
| BigQuery | Analytics warehouse |
| Apache Beam | Distributed processing |
| Dataflow | Managed Beam runner |
| Looker Studio | Dashboard |

## 📊 Key Results
- ✅ 2.7M rows processed daily
- ✅ Partitioned BigQuery table (cost optimized)
- ✅ Automated end-to-end pipeline
- ✅ Real-time Looker Studio dashboard

## 🚀 How to Run

### 1. Setup GCP
```bash
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
```

### 2. Enable APIs
```bash
gcloud services enable cloudfunctions.googleapis.com \
  bigquery.googleapis.com storage.googleapis.com \
  cloudscheduler.googleapis.com dataflow.googleapis.com
```

### 3. Deploy Ingest Function
```bash
cd ingestion/
gcloud functions deploy ingest-taxi-data \
  --gen2 --runtime=python311 \
  --trigger-http --allow-unauthenticated \
  --memory=512MB --timeout=300s
```

### 4. Deploy Transform Function
```bash
cd transform/
gcloud functions deploy transform-taxi-data \
  --gen2 --runtime=python311 \
  --trigger-http --allow-unauthenticated \
  --memory=2GB --timeout=540s
```

### 5. Run Dataflow Pipeline
```bash
cd dataflow/
python3 pipeline.py --runner DataflowRunner \
  --region us-central1 --num_workers=2
```

## 📁 Project Structure
nyc-taxi-batch-etl-pipeline/
├── ingestion/
│   ├── main.py           # Download → GCS
│   └── requirements.txt
├── transform/
│   ├── main.py           # Clean → BigQuery
│   └── requirements.txt
├── dataflow/
│   └── pipeline.py       # Apache Beam pipeline
└── README.md
## 👤 Author
**Arti** | GCP Data Engineer
GitHub: [@Arti434](https://github.com/Arti434)