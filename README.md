# Positium Mobility Insights Platform

This repository contains a geospatial data engineering project designed to analyze mobile network event data for mobility insights. I developed this system to ingest, transform, and visualize user movement and population distribution patterns across administrative units. The pipeline is optimized for local reproducibility using Docker and includes PostgreSQL with PostGIS, PySpark, GeoPandas, and Flask APIs.

---

##  Overview

The project ingests raw mobile event and cell plan data, processes them using Spark and Python, enriches them with geospatial LAU (Local Administrative Unit) boundaries, and stores them in a PostgreSQL/PostGIS database. I then expose analytical insights through:

* Predefined SQL views for rapid querying
* A Flask-based REST API for easy integration
* Visualizations in Jupyter for exploration

This system is modular, scalable, and production-ready.

---

##  Tech Stack

* **Python 3.10** – Data loading, API, spatial joins
* **Apache Spark** – Efficient compression of large mobile event datasets
* **PostgreSQL + PostGIS** – Geo-aware data warehouse
* **GeoPandas + Shapely** – Coordinate transformation and spatial joins
* **Docker Compose** - container orchestration
* **Flask** - REST API to serve queries
* **Jupyter Notebooks** - for geospatial and temporal visualizations & and data analysis

---
## Design Decisions

### 1. Modular Architecture

I structured the pipeline into modular stages: extract → transform → load → analyze. This allows for:

* Decoupled transformations (e.g., Spark handles compression, GeoPandas handles spatial joins)
* Easier testing and debugging
* Scalability (each component can be containerized and scaled independently)

### 2. Spark for Compression

The raw events from S3 are partitioned into year/month/day. Spark was selected to read and compress these massive files efficiently into parquet format using snappy compression, significantly reducing load times during ETL.

### 3. GeoPandas for Spatial Joins

Instead of relying solely on PostGIS for the LAU-to-cell mapping (which could be slower for large volumes), I choose to perform the spatial join between cell points and LAU polygons using GeoPandas. This:

* Avoids CRS transformation issues between EPSG:4326 (cells) and EPSG:3035 (LAU)
* Keeps the operation separate from DB to reduce lock contention
* Allows exporting a fully mapped parquet before ingestion

### 4. PostGIS Views for Insights

To reduce computational load on Jupyter and API, I pre-computed analytical views in SQL and stored them in the `sql/` folder. Views include:

* Events per hour
* Unique users per hour
* Cell IDs per user per day
* Users per LAU per day
* Daytime vs Nighttime user density per LAU

These views are run via a script (`run_sql_scripts.py`) and decouple analytics logic from the application layer.

### 5. Truncate vs Replace Logic

For critical event data (e.g., `mobile_events_compressed`), I chose **TRUNCATE + APPEND** to preserve schema and indexes. For lookup tables (e.g., `cell_plan`, `lau`), I use **DROP + REPLACE** to simplify updates from new GeoJSONs or cell plans.

### 6. REST API with Flask

Instead of querying the DB directly in notebooks or dashboards, I expose endpoints that return either JSON or HTML tables. Each endpoint matches the view naming convention and helps downstream applications easily consume the results.

### 7. Reproducibility and Extensibility

Every transformation or analysis step is reproducible with one of:

* A Python script
* A SQL file
* A container command (via Docker Compose)

This makes the project production-grade and team-ready.

---
##  Folder Structure

```bash
positium-assignment/
├── .env                            # Environment variables for DB credentials
├── README.md
├── docker-compose.yml             # Docker orchestration file
├── requirements.txt               # Python dependencies
├── requirements-spark.txt         # Spark-specific dependencies

├── data/                          # Local raw & processed data (mounted volume)
│   ├── raw/
│   │   ├── events/
│   │   ├── cells/
│   │   └── lau_boundaries/        # GeoJSON from Eurostat
│   └── processed/
│       ├── compressed_events/     # Spark output
│       └── cells_with_lau/        # GeoPandas spatial join output
│
├── etl/                           # Core ETL logic
│   ├── extract/
│   ├── transform/
│   ├── load/
│   │   └── load_all_to_postgres.py
│   └── __init__.py

├── docker/
│   ├── jupyter/Dockerfile
│   ├── etl/Dockerfile
│   └── postgres/init.sql          # Enables PostGIS

├── notebooks/
│   └── analysis.ipynb             # Visualizations, charting

├── sql/                           # Reusable SQL scripts & views
│   ├── agg_events_per_hour.sql
│   ├── agg_unique_users_per_hour.sql
│   ├── agg_cell_count_per_user_per_day.sql
│   ├── agg_users_per_lau_per_day.sql
│   ├── events_per_hour_2024_10_01.sql
│   ├── unique_locations_per_user_per_day.sql
│   ├── user_location_distribution.sql
│   ├── unique_users_per_lau_per_day.sql
│   ├── unique_users_per_lau_2024_10_01.sql
│   └── view_users_per_lau_day_night.sql

├── api/
│   ├── __init__.py
│   └── routes.py                  # Flask endpoints exposing all views

├── scripts/
│   └── run_sql_scripts.py        # Loads all SQL views into Postgres
```

---

##  Quickstart

### 1. Install Docker & Docker Compose

Ensure Docker is installed and running. Recommended to allocate at least **4 CPUs and 8GB RAM** for smooth performance.

### 2. Clone and Build

```bash
git clone https://github.com/prakharsdev/Positium-Data-Platform.git
cd positium-assignment
docker-compose build
```

### 3. Download Raw Datasets

Make sure the following folders are populated:

```
data/raw/events/                # Mobile event parquet from S3
data/raw/cells/                 # Cell plan parquet
data/raw/lau_boundaries/       # LAU GeoJSON from Eurostat
```

### 4. Transform Data

```bash
docker-compose run etl python etl/load/load_all_to_postgres.py
```

### 5. Create Analytical Views

```bash
docker-compose run etl python scripts/run_sql_scripts.py
```

### 6. Explore in Jupyter

```bash
docker-compose up jupyter
```

Visit: [http://localhost:8888](http://localhost:8888)

### 7. Launch Flask API

```bash
docker-compose run --service-ports etl
python app.py
```

Then visit:

* [http://localhost:5000/number-of-events-per-hour](http://localhost:5000/number-of-events-per-hour)
* [http://localhost:5000/unique-users-per-hour](http://localhost:5000/unique-users-per-hour)
* [http://localhost:5000/users-per-lau-day-night](http://localhost:5000/users-per-lau-day-night)

---

##  SQL Views Created

All heavy computation is pushed to the database using SQL views. Here are the key ones:

| View Name                         | Description                                                         |
| --------------------------------- | ------------------------------------------------------------------- |
| `agg_events_per_hour`             | Event count per hour across entire dataset                          |
| `agg_unique_users_per_hour`       | Unique users active per hour                                        |
| `agg_cell_count_per_user_per_day` | Count of unique `cell_id`s visited per user per day                 |
| `agg_users_per_lau_per_day`       | Unique users per LAU polygon per day                                |
| `user_location_distribution`      | Distribution of users based on number of distinct locations visited |
| `unique_users_per_lau_per_day`    | Same as `agg_users_per_lau_per_day` (alias)                         |
| `unique_users_per_lau_2024_10_01` | Daily snapshot on 1st Oct 2024                                      |
| `events_per_hour_2024_10_01`      | Hourly event count on 1st Oct 2024                                  |
| `view_users_per_lau_day_night`    | Unique users by LAU for daytime (7-20h) and nighttime (20-7h)       |

All these are automatically deployed using the script `scripts/run_sql_scripts.py`.

---

##  REST API Endpoints

The Flask app exposes all views as user-friendly HTML tables:

| Route                              | Description                                                                                       |
| ---------------------------------- | ------------------------------------------------------------------------------------------------- |
| `/number-of-events-per-hour`       | Hourly event counts                                                                               |
| `/number-of-unique-users-per-hour` | Unique users per hour                                                                             |
| `/number-of-unique-cells-per-user` | Daily count of cell towers per user                                                               |
| `/users-per-lau`                   | Daily unique users per LAU                                                                        |
| `/users-per-lau-2024-10-01`        | Snapshot for 1 Oct 2024                                                                           |
| `/user-location-distribution`      | Bar chartable distribution of users by locations visited                                          |
| `/users-per-lau-day-night`         | Daytime vs nighttime population by LAU                                                            |
| `/tables/<table_name>`             | Raw table output for `cell_plan`, `lau`, `mobile_events_compressed`, `mobile_events_uncompressed` |

All API responses are returned as HTML tables for easy browser viewing and testing.

---

## Geospatial Handling

* **CRS Transformations**:

  * Cells: EPSG:4326 (lat/lon)
  * LAU: EPSG:3035 (LAEA Europe)
  * Transformed using GeoPandas before upload to PostGIS

* **Spatial Join**:

  * Each cell converted to Point geometry
  * Joined to LAU polygons using `.within()` operation
  * Output written to `cells_with_lau.parquet`

---

##  Visualizations

All visualizations are included in `notebooks/analysis.ipynb`. These include:

* Line chart of event volume per hour
* Unique user counts per hour
* LAU-level choropleth maps
* Histogram of how many locations users visit per day
* Daytime vs nighttime population distribution per region

---


##  Cleanup

To delete all Docker volumes and start fresh:

```bash
docker-compose down -v
```

---


## Future Enhancements

As the system scales and evolves, here are a few improvements I would prioritize to align it with a modern data engineering stack:

### Spark + Distributed Storage

I would move heavy transformations into **Apache Spark**, especially for larger datasets. Storing raw and processed files in **S3** (instead of local disks) would allow the pipeline to scale better and integrate easily with cloud services.

### Cloud + Container Orchestration

Right now, everything runs locally in Docker. In the future, I would containerize the ETL logic into a job and schedule it with **Airflow**. For deployment, running it on **Kubernetes** or serverless platforms like **Google Cloud Run** would reduce operational overhead.

### Testing & CI

I would add `pytest` for testing all ETL steps and set up a **CI pipeline with GitHub Actions** to catch bugs early, lint code, and deploy containers automatically.

### Bash + Automation

To streamline operations, I would introduce shell scripts or a `Makefile` to run the full pipeline with a single command—ideal for both development and deployment.


### Interactive Dashboard

If needed, I could expose the API data to a **React/Chart.js** frontend for internal stakeholders, replacing the current HTML tables with real-time interactive maps and charts.

---
