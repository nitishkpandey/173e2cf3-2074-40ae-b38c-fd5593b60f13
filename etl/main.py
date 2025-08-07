# --- Importing required Libraries---
import requests
import pandas as pd
import time
import psycopg2
import argparse
from sqlalchemy import create_engine, text
from datetime import datetime
import os
from dotenv import load_dotenv
from pathlib import Path


# Load .env from project root
dotenv_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path)

DB_USER = os.environ["DB_USER"]
DB_PASS = os.environ["DB_PASS"]
DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
DB_NAME = os.environ["DB_NAME"]
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- Command Line Interface (CLI) argument for load mode ---
parser = argparse.ArgumentParser(description="ETL pipeline mode")
parser.add_argument("--mode", choices=["append", "truncate", "full-refresh"], default="full-refresh", help="Data load mode")
args = parser.parse_args()

# --- Waiting for DB to be ready ---
def wait_for_db(max_retries=20, delay=5):
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASS,
                host=DB_HOST,
                port=DB_PORT
            )
            conn.close()
            print("PostgreSQL is available.")
            return
        except psycopg2.OperationalError:
            print(f"Waiting for PostgreSQL. attempt {i + 1}")
            time.sleep(delay)
    raise Exception("Could not connect to PostgreSQL after multiple attempts.")

wait_for_db()
engine = create_engine(DB_URL)

# --- Defining API URLs and desired indicators ---
DATASETS = {
    "nrg_cb_e": {
        "url": "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nrg_cb_e?nrg_bal=GEP&lang=EN",
        "indicators": ["GEP"]
    },
    "ten00124": {
        "url": "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/ten00124?lang=EN",
        "indicators": [
            "FC_E", "FC_IND_E", "FC_TRA_E", "FC_OTH_CP_E", "FC_OTH_HH_E"
        ]
    }
}

# --- NEW: Auto-detect indicator dimension key ---
def detect_indicator_dimension(dimensions, indicators):
    for dim_name, dim_info in dimensions.items():
        labels = dim_info.get("category", {}).get("label", {})
        if any(ind in labels for ind in indicators):
            return dim_name
    return None

# --- Fetching & transforming function ---
def fetch_and_transform(dataset_code, url, indicators):
    response = requests.get(url)
    data = response.json()

    if 'dimension' not in data or 'value' not in data or 'size' not in data:
        print(f"Skipping {dataset_code}: Missing expected keys.")
        return pd.DataFrame()

    dim = data['dimension']
    dim_ids = data.get('id', list(dim.keys()))
    sizes = data['size']
    value_data = data['value']

    labels = {k: dim[k]['category']['label'] for k in dim if 'category' in dim[k]}
    indexes = [dim[d]['category']['index'] for d in dim_ids]

    # --- Detect indicator dimension dynamically ---
    indicator_dim = detect_indicator_dimension(dim, indicators)
    if not indicator_dim:
        print(f"Could not detect indicator dimension in dataset {dataset_code}")
        return pd.DataFrame()

    def unravel_index(flat_index):
        coords = []
        for size in reversed(sizes):
            coords.append(flat_index % size)
            flat_index //= size
        return list(reversed(coords))

    result = []
    for flat_index_str, val in value_data.items():
        idx = unravel_index(int(flat_index_str))
        keys = [list(indexes[i].keys())[idx[i]] for i in range(len(idx))]
        dim_map = {dim_ids[i]: keys[i] for i in range(len(dim_ids))}

        # --- Use dynamic indicator dimension ---
        indicator = dim_map.get(indicator_dim)
        if indicator not in indicators:
            continue

        result.append({
            "dataset_code": dataset_code,
            "country_code": dim_map.get("geo"),
            "country_name": labels.get("geo", {}).get(dim_map.get("geo"), dim_map.get("geo")),
            "indicator_code": indicator,
            "indicator_label": labels.get(indicator_dim, {}).get(indicator),
            "unit_code": dim_map.get("unit"),
            "unit_label": labels.get("unit", {}).get(dim_map.get("unit")),
            "time": dim_map.get("time"),
            "value": float(val)
        })

    print(f"Transformed {len(result)} rows for {dataset_code}")

    # --- Converting to DataFrame and cleaning the data ---
    df = pd.DataFrame(result)

    num_duplicates = df.duplicated().sum()
    if num_duplicates > 0:
        print(f"Found {num_duplicates} duplicate rows. Removing them.")
        df = df.drop_duplicates()

    missing_count = df.isnull().sum().sum()
    if missing_count > 0:
        print("Missing values detected. Dropping rows with missing critical values.")
        df = df.dropna(subset=[
            'country_code', 'country_name', 'indicator_code',
            'indicator_label', 'time', 'value'
        ])

    # Parse time as date (year only)
    df['time'] = pd.to_datetime(df['time'], format='%Y')

    print(f"Cleaned data: {len(df)} rows remaining after cleaning.")
    return df

# --- Processing all datasets into df ---
all_dataframes = []

for dataset_code, config in DATASETS.items():
    df = fetch_and_transform(
        dataset_code,
        config["url"],
        config["indicators"]
    )
    all_dataframes.append(df)

# --- Concatenating all cleaned dataframes ---
full_df = pd.concat(all_dataframes, ignore_index=True)
full_df["load_timestamp"] = datetime.now()

# --- Loading to PostgreSQL DB ---
with engine.begin() as conn:
    if args.mode == "full-refresh":
        conn.execute(text("DROP TABLE IF EXISTS observations"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS observations (
                id SERIAL PRIMARY KEY,
                dataset_code TEXT,
                country_code TEXT,
                country_name TEXT,
                indicator_code TEXT,
                indicator_label TEXT,
                unit_code TEXT,
                unit_label TEXT,
                time DATE,
                value FLOAT,
                load_timestamp TIMESTAMP
            );
        """))
        print("Dropped and recreated 'observations' table.")
    elif args.mode == "truncate":
        conn.execute(text("TRUNCATE TABLE observations"))
        print("Truncated 'observations' table.")
    elif args.mode == "append":
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS observations (
                id SERIAL PRIMARY KEY,
                dataset_code TEXT,
                country_code TEXT,
                country_name TEXT,
                indicator_code TEXT,
                indicator_label TEXT,
                unit_code TEXT,
                unit_label TEXT,
                time DATE,
                value FLOAT,
                load_timestamp TIMESTAMP
            );
        """))
        print("Append mode: ensured 'observations' table exists.")

full_df.to_sql("observations", engine, if_exists="append", index=False)
print(f"Loaded {len(full_df)} rows to 'observations' table.")