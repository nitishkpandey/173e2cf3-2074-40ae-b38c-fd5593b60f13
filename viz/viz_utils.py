import os
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv
from pathlib import Path

# Loading .env from project root
dotenv_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path)

DB_USER = os.environ["DB_USER"]
DB_PASS = os.environ["DB_PASS"]
DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
DB_NAME = os.environ["DB_NAME"]

# Create output directory
OUTPUT_DIR = "outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Connect to PostgreSQL
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
df = pd.read_sql("SELECT * FROM observations", engine)
print(f"Loaded {len(df)} records from PostgreSQL.")

# Plot 1: Country trend
def plot_country_trend(df, country_code, indicator_code):
    subset = df[(df['country_code'] == country_code) & (df['indicator_code'] == indicator_code)]
    subset = subset.sort_values(by='time')

    if subset.empty:
        print(f"No data for {country_code} and {indicator_code}")
        return

    plt.figure(figsize=(12, 6))
    sns.lineplot(data=subset, x='time', y='value', marker='o')
    plt.title(f"{indicator_code} Trend for {country_code}")
    plt.xlabel("Year")
    plt.ylabel("Value")
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    filepath = os.path.join(OUTPUT_DIR, f"{country_code}_{indicator_code}_trend.png")
    plt.savefig(filepath)
    print(f"Saved plot: {filepath}")
    plt.close()

# Plot 2: Top countries
def plot_top_countries(df, indicator_code):
    latest_year = df['time'].max()
    subset = df[(df['indicator_code'] == indicator_code) & (df['time'] == latest_year)]

    top = subset.groupby('country_code')['value'].sum().nlargest(10).reset_index()

    plt.figure(figsize=(10, 6))
    sns.barplot(data=top, x='value', y='country_code')
    plt.title(f"Top 10 Countries by {indicator_code} in {latest_year.year}")
    plt.xlabel("Value")
    plt.ylabel("Country")
    plt.grid(True)
    filepath = os.path.join(OUTPUT_DIR, f"top_10_{indicator_code}_{latest_year.year}.png")
    plt.savefig(filepath)
    print(f"Saved plot: {filepath}")
    plt.close()

# Plot 3: Heatmap
def plot_heatmap(df, indicator_code):
    subset = df[df['indicator_code'] == indicator_code]
    pivot = subset.pivot_table(index='country_code', columns='time', values='value', aggfunc='sum')

    plt.figure(figsize=(12, 8))
    sns.heatmap(pivot, cmap='viridis', linewidths=0.5, linecolor='gray')
    plt.title(f"Heatmap of {indicator_code} Over Time")
    plt.xlabel("Year")
    plt.ylabel("Country")
    filepath = os.path.join(OUTPUT_DIR, f"heatmap_{indicator_code}.png")
    plt.savefig(filepath)
    print(f"Saved plot: {filepath}")
    plt.close()

# Run all plots
def generate_all():
    plot_country_trend(df, 'DE', 'GEP')
    plot_top_countries(df, 'GEP')
    plot_heatmap(df, 'GEP')

if __name__ == '__main__':
    generate_all()