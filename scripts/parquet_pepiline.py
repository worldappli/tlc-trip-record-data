
"""
NYC Taxi - Parquet Out-of-Core Pipeline (PRO VERSION)

Objectifs :
- Lire plusieurs fichiers Parquet (gros volume)
- Traitement chunk-like (fichier par fichier)
- Nettoyage + transformation
- Agrégation progressive
- Export Parquet optimisé

Auteur : toi 😎
"""

import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import os
from glob import glob

# ==============================
# CONFIGURATION
# ==============================

INPUT_FOLDER = "../data/raw/"
OUTPUT_FILE = "../data/processed/nyc_taxi_cleaned.parquet"

os.makedirs("../data/processed", exist_ok=True)

# ==============================
# VARIABLES D'AGREGATION
# ==============================

total_rows = 0
total_revenue = 0
monthly_stats = {}

# ==============================
# INITIALISATION WRITER PARQUET
# ==============================

writer = None

# ==============================
# LISTE DES FICHIERS PARQUET
# ==============================

files = glob(os.path.join(INPUT_FOLDER, "*.parquet"))

print(f"📂 {len(files)} fichiers détectés")

# ==============================
# TRAITEMENT
# ==============================

for i, file in enumerate(files):

    print(f"\n📦 Traitement fichier {i+1}/{len(files)} : {file}")

    # Lecture Parquet
    df = pd.read_parquet(file)

    # ==========================
    # NETTOYAGE
    # ==========================

    # Conversion types
    df["fare_amount"] = pd.to_numeric(df["fare_amount"], errors="coerce")

    # Conversion dates
    df["tpep_pickup_datetime"] = pd.to_datetime(
        df["tpep_pickup_datetime"], errors="coerce"
    )

    # Suppression lignes invalides
    df = df.dropna(subset=["fare_amount", "tpep_pickup_datetime"])

    # ==========================
    # FEATURE ENGINEERING
    # ==========================

    df["month"] = df["tpep_pickup_datetime"].dt.to_period("M")

    # ==========================
    # AGREGATION PROGRESSIVE
    # ==========================

    total_rows += len(df)
    total_revenue += df["fare_amount"].sum()

    monthly_counts = df["month"].value_counts()

    for month, count in monthly_counts.items():
        monthly_stats[month] = monthly_stats.get(month, 0) + count

    # ==========================
    # ECRITURE PARQUET STREAM
    # ==========================

    table = pa.Table.from_pandas(df)

    if writer is None:
        writer = pq.ParquetWriter(OUTPUT_FILE, table.schema)

    writer.write_table(table)

# ==============================
# FINALISATION
# ==============================

if writer:
    writer.close()

print("\n✅ Traitement terminé")

# ==============================
# RESULTATS
# ==============================

print("\n📊 Résumé global :")
print(f"Total lignes : {total_rows}")
print(f"Revenue total : {total_revenue:.2f}")

print("\n📅 Nombre de trajets par mois :")
for month in sorted(monthly_stats):
    print(f"{month} : {monthly_stats[month]}")

# ==============================
# VALIDATION FICHIER FINAL
# ==============================

print("\n📖 Lecture du fichier final...")

df_final = pd.read_parquet(OUTPUT_FILE)

print(df_final.head())
print(df_final.info())

