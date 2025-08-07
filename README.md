# Eurostat Energy ETL Pipeline

## Overview

This project is a Dockerized ETL pipeline built in Python that extracts public energy statistics from the [Eurostat REST API](https://ec.europa.eu/eurostat/web/main/home), transforms the data, and loads it into a PostgreSQL database. It also generates visualizations to highlight trends in gross electricity production across EU countries.


## Technical Stack
- Python 3.11
- Pandas / SQLAlchemy
- Matplotlib / Seaborn
- PostgreSQL 16
- Docker & Docker Compose
- Eurostat REST API
- PGAdmin (for Database inspection)


## Eurostat REST API overview
The data is retreived from the Eurostat API, which is publicly accessible without authentication.
Following is the dataset overview:
- **nrg_cb_e**: Supply, transformation and consumption of electricity.
     #### Extracted Indicator:
     `Gross electricity production (GEP)`
- **ten00124**: Final energy consumption by sector.
     #### Extracted Indicators:
     `FC_E` – Final consumption – energy use
  
     `FC_IND_E` – Industry sector
  
     `FC_TRA_E` – Transport sector
  
     `FC_OTH_CP_E` – Commercial & public services
  
     `FC_OTH_HH_E` – Households


## Project Structure
`etl/main.py` – ETL pipeline script

`viz/viz_utils.py` – Generates visualizations

`outputs/` – Contains output .png charts

`postgres/init.sql` – Database schema initialization

`.env` – Environment variables (excluded from GitHub)

`.gitignore` – Prevents committing the .env file storing secrets, caches, etc.

`docker-compose.yml` – Defines and orchestrates services

`Dockerfile` – Builds ETL container

`requirements.txt` – Python dependencies


## Extract Transform Load (ETL) Flow

1. **Extract**:
   - Connects to Eurostat API.
   - Retrieves energy datasets `nrg_cb_e` and `ten00124`.

2. **Transform**:
   - Parses metadata, indicators, dimensions.
   - Converts time fields to proper date format.
   - Handles missing and duplicate data.

3. **Load**:
   - Inserts data into a normalized PostgreSQL schema.
   - Ensures idempotency via `ON CONFLICT` clauses.
   - Supports Command Line Interface (CLI) mode for `append`, `truncate` and `full-refresh`. By default it is set to `full-refresh`. 


## Visualizations

Three types of plots are generated in the `outputs/` directory:

- **Line Plot**: Gross electricity production trend for Germany (`DE`)
- **Bar Chart**: Top 10 countries by Gross ELectricity Production (GEP)
- **Heatmap**: Year-wise heatmap of energy production across countries


## Setup Instructions (Using Docker)

### 1. Clone the Repository
<pre>
git clone https://github.com/your-username/your-repo-name.git
cd your-repo-name
</pre>

### 2. Environment Variables

This project uses a .env file to manage environment variables such as database credentials securely.

Important: The .env file is not included in the repository (.gitignore protects it). The .env file must be created locally before running the project.

### 3. Security & Git Ignore

.env is used to store credentials and never pushed to GitHub

.gitignore is used to not push the .env file to GitHub

### 4. Running the ETL Pipeline via Docker Compose

Open the terminal and navigate to the project directory.

Run the full Dockerized setup via:

<pre>
docker-compose up --build
</pre>

The above script will:

- Wait for PostgreSQL to be ready

- Run the ETL process (main.py)

- Generate visualizations (viz_utils.py) and save under outputs/

To remove volumes/data:

<pre>
docker-compose down -v
</pre>

## Observations from the visualisations

- Gross Electricity Production (GEP) is highest in EU aggregates (EU27_2020, EA20), reflecting regional consolidation of data.

- Germany's energy production peaked around 2017 but has shown a declining trend in recent years.
  
- Some smaller or non-EU countries have sparse or missing data points, highlighting reporting differences across nations.

##### Note: These observations are derived from data pulled at execution time and may vary as the Eurostat API updates in real-time.






