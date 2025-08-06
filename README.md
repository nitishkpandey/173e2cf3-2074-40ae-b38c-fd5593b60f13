# Eurostat Energy ETL Pipeline

This project is a Dockerized ETL pipeline built in Python that extracts public energy statistics from the [Eurostat REST API](https://ec.europa.eu/eurostat/web/json-and-unicode-web-services), transforms the data, and loads it into a PostgreSQL database. It also generates visualizations to highlight trends in gross electricity production across EU countries.

---

## Project Structure


---

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
   - Supports CLI mode for `append`, `truncate`, or `full-refresh`.

---

## Visualizations

Three types of plots are generated in the `outputs/` directory:

- **Line Plot**: Gross electricity production trend for Germany (`DE`)
- **Bar Chart**: Top 10 countries by gross production
- **Heatmap**: Year-wise heatmap of energy production across countries

---

## Instructions to Run (Using Docker)

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/your-repo-uuid.git
cd your-repo-uuid


