# NYC Taxi Batch ETL Pipeline

A production-grade batch ETL pipeline processing **2.7M NYC Yellow Taxi records** using Google Cloud Platform.

**Stack:** Python · BigQuery · dbt · Apache Beam · GCP

---

## Architecture

### Path A — Cloud Functions (Free Tier)

```text
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
```

### Path B — Dataflow (Production)

```text
Cloud Scheduler (daily 2AM)
        ↓
Cloud Function 1 — Ingest
        ↓
GCS Bucket (raw storage)
        ↓
Apache Beam / Dataflow
(distributed processing)
        ↓
BigQuery (partitioned by date)
        ↓
Looker Studio Dashboard
```

### Path C — dbt (ELT Pattern)

```text
BigQuery (raw trips table)
        ↓
dbt Staging Layer
(stg_taxi_trips — clean + enrich)
        ↓
dbt Mart Layer
(daily_trips | hourly_patterns | location_performance)
        ↓
Looker Studio dbt Dashboard
```

---

## Pipeline Stats

| Metric            | Value                        |
| :---              | :---                         |
| Records Processed | 2,723,762 rows               |
| Data Quality      | 91.8% valid rows             |
| Processing Time   | ~2 minutes                   |
| Cost              | $0/month (free tier)         |
| Schedule          | Daily at 2:00 AM EST         |
| dbt Models        | 4 (1 staging + 3 marts)      |
| dbt Tests         | 9 tests — all passing        |

---

## Tech Stack

| Service          | Purpose                        |
| :---             | :---                           |
| Cloud Scheduler  | Daily cron trigger             |
| Cloud Functions  | Serverless Python execution    |
| Cloud Storage    | Raw file storage               |
| BigQuery         | Analytical data warehouse      |
| Apache Beam      | Distributed data processing    |
| Dataflow         | Managed Beam runner            |
| dbt Cloud        | ELT transformation layer       |
| Looker Studio    | Dashboard & visualization      |

---

## Dashboards

| Dashboard              | Link                                                          |
| :---                   | :---                                                          |
| Batch ETL Dashboard    | [View Dashboard](https://datastudio.google.com/s/rSwgWksnyH8) |
| dbt Analytics Dashboard |[Dashboard](https://datastudio.google.com/reporting/6aa6da35-6072-4c90-b09e-9815f92ea38a)                                               |

---

## dbt Transformation Layer

Built an ELT transformation layer using dbt Cloud on **16M+ NYC Taxi records**.

### Models

| Model                      | Type  | Description                                           |
| :---                       | :---  | :---                                                  |
| `stg_taxi_trips`           | View  | Cleans raw data, adds trip duration, tip %, rush hour flags |
| `mart_daily_trips`         | Table | Daily KPIs — trips, revenue, rush hours               |
| `mart_hourly_patterns`     | Table | Demand by hour x day of week                          |
| `mart_location_performance`| Table | Top 20 pickup locations by revenue                    |

### Data Quality Tests

- 9 automated tests — all passing
- `not_null` checks on critical columns
- `unique` constraints on dimension keys

### Run dbt

```bash
dbt run              # build all models
dbt test             # run quality tests
dbt docs generate    # generate documentation
```

---

## Project Structure

```text
nyc-taxi-batch-etl-pipeline/
├── ingestion/
│   ├── main.py               # Cloud Function 1 — Download → GCS
│   └── requirements.txt
├── transform/
│   ├── main.py               # Cloud Function 2 — Clean → BigQuery
│   └── requirements.txt
├── dataflow/
│   └── pipeline.py           # Apache Beam pipeline
├── models/
│   ├── sources.yml           # Raw data source definition
│   ├── schema.yml            # Data quality tests
│   ├── staging/
│   │   └── stg_taxi_trips.sql
│   └── marts/
│       ├── mart_daily_trips.sql
│       ├── mart_hourly_patterns.sql
│       └── mart_location_performance.sql
├── dbt_project.yml
└── README.md
```

---

## Author

**Arti** | GCP Data Engineer
GitHub: [@Arti434](https://github.com/Arti434)
