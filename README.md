# nyc-taxi-batch-etl-pipeline
A production-grade batch ETL pipeline processing **2.7M NYC Yellow Taxi records** using Google Cloud Platform.
## 🏗️ Architecture
Cloud Scheduler (daily 2AM)
↓
Cloud Function 1 — Ingest
(downloads parquet from NYC TLC)
↓
GCS Bucket (raw storage)
↓
Cloud Function 2 — Transform
(pandas: clean, validate, rename)
↓
BigQuery (partitioned by date)
↓
Looker Studio Dashboard
## ⚡ Pipeline Stats
- **Records processed:** 2,723,762 rows
- **Data quality:** 91.8% valid rows
- **Processing time:** ~2 minutes
- **Cost:** $0/month (free tier)
- **Schedule:** Daily at 2:00 AM EST

## 🛠️ Tech Stack
| Service | Purpose |
|---------|---------|
| Cloud Scheduler | Daily cron trigger |
| Cloud Functions | Serverless Python execution |
| GCS | Raw file storage |
| BigQuery | Analytical warehouse |
| Apache Beam | Distributed processing (Path B) |
| Dataflow | Managed Beam runner |
| Looker Studio | Dashboard & visualization |

## 📊 Dashboard
[View Live Dashboard](#) ← https://datastudio.google.com/s/rSwgWksnyH8

## 🚀 How to Run

### Prerequisites
```bash
pip install google-cloud-storage google-cloud-bigquery \
            functions-framework pandas pyarrow
```

### Deploy Cloud Functions
```bash
# Function 1 - Ingest
cd ingestion/
gcloud functions deploy ingest-taxi-data \
  --gen2 --runtime=python311 \
  --trigger-http --allow-unauthenticated

# Function 2 - Transform  
cd transform/
gcloud functions deploy transform-taxi-data \
  --gen2 --runtime=python311 \
  --trigger-http --allow-unauthenticated
```

### Run Dataflow Pipeline
```bash
cd dataflow/
python3 pipeline.py \
  --runner DataflowRunner \
  --region us-central1
```

## 📁 Project Structure
nyc-taxi-batch-etl-pipeline/
├── ingestion/
│   ├── main.py           # Cloud Function 1
│   └── requirements.txt
├── transform/
│   ├── main.py           # Cloud Function 2
│   └── requirements.txt
├── dataflow/
│   └── pipeline.py       # Apache Beam pipeline
└── README.md
## 👤 Author
**Arti** | GCP Data Engineer
GitHub: [@Arti434](https://github.com/Arti434)
EOF
