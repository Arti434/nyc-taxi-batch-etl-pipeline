"""
Dataflow pipeline: reads NYC Taxi parquet from GCS,
cleans and validates rows, writes to BigQuery.

Usage:
  # Local testing (free)
  python3 pipeline.py --runner DirectRunner

  # Production (Dataflow)
  python3 pipeline.py --runner DataflowRunner
"""

import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
    GoogleCloudOptions,
    SetupOptions
)
from apache_beam.io.parquetio import ReadFromParquet
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT = "nyc-taxi-etl-demo-494006"
BUCKET  = "nyc-taxi-etl-demo-494006-data"
DATASET = "taxi_data"
TABLE   = "trips"

BQ_SCHEMA = {
    "fields": [
        {"name": "vendor_id",            "type": "STRING"},
        {"name": "pickup_datetime",      "type": "TIMESTAMP"},
        {"name": "dropoff_datetime",     "type": "TIMESTAMP"},
        {"name": "passenger_count",      "type": "INTEGER"},
        {"name": "trip_distance",        "type": "FLOAT"},
        {"name": "fare_amount",          "type": "FLOAT"},
        {"name": "tip_amount",           "type": "FLOAT"},
        {"name": "total_amount",         "type": "FLOAT"},
        {"name": "pickup_location_id",   "type": "INTEGER"},
        {"name": "dropoff_location_id",  "type": "INTEGER"},
    ]
}


class ParseAndClean(beam.DoFn):
    """
    Validates and transforms raw parquet rows.
    Drops rows with:
      - Missing pickup datetime
      - Negative fare amount
      - Zero or negative trip distance
      - Invalid passenger count (>6)
    """

    def process(self, element):
        try:
            pickup  = element.get("tpep_pickup_datetime")
            dropoff = element.get("tpep_dropoff_datetime")
            fare    = float(element.get("fare_amount") or 0)
            dist    = float(element.get("trip_distance") or 0)
            pax     = int(element.get("passenger_count") or 0)

            # Quality filters
            if not pickup:
                return
            if fare < 0:
                return
            if dist <= 0:
                return
            if pax < 1 or pax > 6:
                return

            yield {
                "vendor_id"          : str(element.get("VendorID", "")),
                "pickup_datetime"    : str(pickup),
                "dropoff_datetime"   : str(dropoff) if dropoff else None,
                "passenger_count"    : pax,
                "trip_distance"      : dist,
                "fare_amount"        : fare,
                "tip_amount"         : float(element.get("tip_amount") or 0),
                "total_amount"       : float(element.get("total_amount") or 0),
                "pickup_location_id" : int(element.get("PULocationID") or 0),
                "dropoff_location_id": int(element.get("DOLocationID") or 0),
            }

        except Exception as e:
            logger.warning(f"Skipping bad row: {e}")
            return


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",   default=f"gs://{BUCKET}/raw/yellow_tripdata_2024-01.parquet")
    parser.add_argument("--month",   default="2024-01")
    parser.add_argument("--runner",  default="DirectRunner")
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Pipeline options
    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True

    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project        = PROJECT
    google_cloud_options.region         = "us-central1"
    google_cloud_options.staging_location = f"gs://{BUCKET}/staging"
    google_cloud_options.temp_location    = f"gs://{BUCKET}/tmp"
    google_cloud_options.job_name         = f"nyc-taxi-etl-{known_args.month}"

    std_options = options.view_as(StandardOptions)
    std_options.runner = known_args.runner

    table_ref = f"{PROJECT}:{DATASET}.{TABLE}"

    logger.info(f"Starting pipeline — input: {known_args.input}")
    logger.info(f"Runner: {known_args.runner}")

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadParquet"    >> ReadFromParquet(known_args.input)
            | "ParseAndClean"  >> beam.ParDo(ParseAndClean())
            | "WriteToBQ"      >> WriteToBigQuery(
                table               = table_ref,
                schema              = BQ_SCHEMA,
                write_disposition   = BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition  = BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location = f"gs://{BUCKET}/tmp"
            )
        )

    logger.info("Pipeline complete!")


if __name__ == "__main__":
    run()
