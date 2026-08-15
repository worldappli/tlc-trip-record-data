import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
from glob import glob
import os
import s3fs

fs = s3fs.S3FileSystem()

writer = None

import boto3

BUCKET = "ruben-tlc-trip-record-data"

PREFIX = "raw/"

s3 = boto3.client("s3")

response = s3.list_objects_v2(
    Bucket=BUCKET,
    Prefix=PREFIX
)

files = []

for obj in response["Contents"]:

    key = obj["Key"]

    if key.endswith(".parquet"):
        files.append(key)

print(files)

#INPUT_FOLDER = "data/raw"
#OUTPUT_FILE = "data/processed/nyc_taxi_cleaned1.parquet"


#INPUT_FOLDER = "/opt/airflow/data/raw"
OUTPUT_FILE = f"s3://{BUCKET}/processed/nyc_taxi_clean.parquet"

# compteur global
hourly_stats = {h: 0 for h in range(24)}

#Schema
schema = pa.schema([
            ("VendorID", pa.int32()),
            ("tpep_pickup_datetime", pa.timestamp("us")),
            ("tpep_dropoff_datetime", pa.timestamp("us")),
            ("passenger_count", pa.float64()),
            ("trip_distance", pa.float64()),
            ("RatecodeID", pa.float64()),
            ("store_and_fwd_flag", pa.string()),
            ("PULocationID", pa.int32()),
            ("DOLocationID", pa.int32()),
            ("payment_type", pa.int64()),
            ("fare_amount", pa.float64()),
            ("extra", pa.float64()),
            ("mta_tax", pa.float64()),
            ("tip_amount", pa.float64()),
            ("tolls_amount", pa.float64()),
            ("improvement_surcharge", pa.float64()),
            ("total_amount", pa.float64()),
            ("congestion_surcharge", pa.float64()),
            ("Airport_fee", pa.float64()),
            ("cbd_congestion_fee", pa.float64()),
            ("hour", pa.int32())
        ])

for file in files:

    print(f"\nTraitement : {file}")

    parquet_file = pq.ParquetFile( f"s3://{BUCKET}/{file}",
        filesystem=fs)

    for batch in parquet_file.iter_batches(
        batch_size=100_000
    ):

        df = batch.to_pandas()

        # conversion date
        df["tpep_pickup_datetime"] = pd.to_datetime(
            df["tpep_pickup_datetime"],
            errors="coerce"
        )

        # extraction heure
        df["hour"] = (
            df["tpep_pickup_datetime"]
            .dt.hour
        )

        # comptage batch
        counts = df["hour"].value_counts()
        #print(counts)

        # ajout au compteur global
        for hour, count in counts.items():
            hourly_stats[hour] += count

        # Ecriture parquet Streaming
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

        if writer is None :
            writer = pq.ParquetWriter(OUTPUT_FILE, schema = schema)
        writer.write_table(table)

if writer :
    writer.close()

print("\nRésultat final")

#display(df.columns)

'''for hour in sorted(hourly_stats):
    print(
        f"{hour:02d}h : {hourly_stats[hour]}"
    )'''

# Lecture du gros fichier

meta = pq.read_metadata(OUTPUT_FILE)

print(f"\nZeilen :  {meta.num_rows}")
print(f"Zeilengruppen : {meta.num_row_groups}")

#sample = pq.read_table(OUTPUT_FILE).slice(0,5).to_pandas()
#print(f" \nfirst line : {sample}")

parquet_file = pq.ParquetFile(OUTPUT_FILE)

first_batch = next(
    parquet_file.iter_batches(batch_size=5)
)

sample = first_batch.to_pandas()

print(sample)
