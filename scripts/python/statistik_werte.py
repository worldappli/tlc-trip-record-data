#%%
import math
import pandas as pd
import os
import pyarrow.parquet as pq
import pyarrow as pa
from glob import  glob
from collections import Counter
from pyarrow.parquet import ParquetFile

# Speicherort der Datei
#OUTPUT_FILE = "../data/processed/"
OUTPUT_FILE = "/opt/airflow/data/processed/"
os.makedirs("/opt/airflow/data/output", exist_ok=True)

files = sorted(
    glob(os.path.join(OUTPUT_FILE,"nyc_taxi_cleaned1.parquet"))
)
#files = sorted( glob(os.path.join(OUTPUT_FILE,"*.parquet")))

# Statisitk Werte :
#Initialization
total_fare = 0
trip_count = 0
global_min = float("inf")
global_max = float("-inf")
counter = Counter()

n=0
mean = 0
M2 = 0
# Korrelation
mean_x = 0
mean_y = 0
M2_x = 0
M2_y = 0
C_xy = 0

BIN_SIZE = 5
histogram = Counter()

for file in files :
    print(f"\nTraitement du fichier : {file}")

    parquet_file = ParquetFile(file)
    for batch in parquet_file.iter_batches(batch_size=100_000):
        df = batch.to_pandas()

        total_fare += df["fare_amount"].sum()
        trip_count += len(df)
        global_min = min(global_min, min(df["fare_amount"]))
        global_max = max(global_max, max(df["fare_amount"]))
        counter.update(df["hour"])

        #Varianz, Sdt (Welford-Algorithmus)
        values = df["fare_amount"].dropna()
        for x in  values:

            n+=1
            delta = x-mean
            mean += delta/n
            delta2 = x-mean
            M2 = delta * delta2

            bucket = int(x // BIN_SIZE)
            histogram[bucket]+=1

        #Korrelation
        subset = df[
            ["trip_distance", "fare_amount"]
            ].dropna()

        for x, y in zip(
            subset["trip_distance"],
            subset["fare_amount"]
        ):

            n += 1

            dx = x - mean_x
            mean_x += dx / n

            dy = y - mean_y
            mean_y += dy / n

            M2_x += dx * (x - mean_x)
            M2_y += dy * (y - mean_y)

            C_xy += dx * (y - mean_y)

var_x = M2_x / (n - 1)
var_y = M2_y / (n - 1)

cov_xy = C_xy / (n - 1)

corr = cov_xy / math.sqrt(var_x * var_y)

mean1 = total_fare/trip_count

variance = M2/(n-1)
std_dev = math.sqrt(variance)

print(f"\nSumme = {total_fare}")
print(f"\nAnzahl = {trip_count}")
print(f"Mean = {round(mean1,2)}")
print(f"Min = {global_min}")
print(f"Max = {global_max}")
print(counter.most_common(5))

print(f"Moyenne : {mean:.2f}")
print(f"Variance : {variance:.4f}")
print(f"Std Dev : {std_dev:.2f}")

print(f"Korrelation : {corr:.4f}")

#%%
#Histogram
for bucket in sorted(histogram):

    start = bucket * BIN_SIZE
    end = start + BIN_SIZE

    print(
        f"{start:3d}-{end:3d} : "
        f"{histogram[bucket]}"
    )

#Exportieren Histogramm
hist_df = pd.DataFrame([
    {
        "bin_start": b * BIN_SIZE,
        "bin_end": (b + 1) * BIN_SIZE,
        "count": c
    }
    for b, c in histogram.items()
])

hist_df.to_csv(
    "/opt/airflow/data/output/fare_histogram.csv",
    index=False
)
#%%
total_count = sum(histogram.values())
median_pos = total_count / 2
running = 0

for bucket in sorted(histogram):

    running += histogram[bucket]

    if running >= median_pos:

        median_bucket = bucket
        break

median_estimate = (
    median_bucket * BIN_SIZE
    + BIN_SIZE / 2
)
print(median_bucket)
print(median_estimate)
#%%
#Quantile
def histogram_quantile(
    histogram,
    total_count,
    quantile,
    bin_size
):

    target = total_count * quantile

    running = 0

    for bucket in sorted(histogram):

        running += histogram[bucket]

        if running >= target:

            return (
                bucket * bin_size
                + bin_size / 2
            )

#%%
q25 = histogram_quantile(
    histogram,
    total_count,
    0.25,
    BIN_SIZE
)

q50 = histogram_quantile(
    histogram,
    total_count,
    0.50,
    BIN_SIZE
)

q75 = histogram_quantile(
    histogram,
    total_count,
    0.75,
    BIN_SIZE
)

print(f" {q25} {q50} {q75}")