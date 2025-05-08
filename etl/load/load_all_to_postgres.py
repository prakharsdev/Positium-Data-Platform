import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# PostgreSQL connection settings from .env
pg_user = os.getenv("POSTGRES_USER", "postgres")
pg_pass = os.getenv("POSTGRES_PASSWORD", "postgres")
pg_db = os.getenv("POSTGRES_DB", "mobility")
pg_host = os.getenv("POSTGRES_HOST", "localhost")
pg_port = os.getenv("POSTGRES_PORT", "5432")

engine = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")

# --------------------------
# Load COMPRESSED EVENTS
# --------------------------
def load_compressed_events():
    print("\n Loading compressed mobile event data...")
    input_path = "data/processed/compressed_events/"
    files = sorted([os.path.join(input_path, f) for f in os.listdir(input_path) if f.endswith(".parquet")])

    with engine.connect() as conn:
        try:
            conn.execute(text("TRUNCATE TABLE mobile_events_compressed;"))
            print("Truncated table: mobile_events_compressed...")
        except Exception as e:
            print("Skip truncating: Table probably doesn't exist yet.", e)

    for i, file in enumerate(files, 1):
        print(f"[COMPRESSED EVENTS] Processing file {i}/{len(files)}: {file}")
        try:
            df = pd.read_parquet(file)
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df["date"] = df["timestamp"].dt.date
            # Decode binary user_id to hex string
            if isinstance(df["user_id"].iloc[0], bytes):
                df["user_id"] = df["user_id"].apply(lambda b: b.hex())
            df[["user_id", "cell_id", "timestamp", "date"]].to_sql(
                "mobile_events_compressed",
                engine,
                if_exists="append",
                index=False
            )
        except SQLAlchemyError as e:
            print(f"Failed to write {file} due to: {e}")
            try:
                conn = engine.raw_connection()
                conn.rollback()
                conn.close()
            except Exception as rollback_err:
                print(f"Rollback failed: {rollback_err}")

# --------------------------
# Load UNCOMPRESSED EVENTS
# --------------------------
def load_uncompressed_events():
    print("\n Loading data into mobile_events_uncompressed from data/raw/events...")
    input_path = "data/raw/events/year=2024/month=10/day=1/"
    files = sorted([os.path.join(input_path, f) for f in os.listdir(input_path) if f.endswith(".parquet")])

    for i, file in enumerate(files, 1):
        print(f"[UNCOMPRESSED EVENTS] File {i}/{len(files)}: {file}")
        try:
            df = pd.read_parquet(file)
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df["date"] = df["timestamp"].dt.date

            if isinstance(df["user_id"].iloc[0], bytes):
                df["user_id"] = df["user_id"].apply(lambda b: b.hex())

            df = df[["user_id", "cell_id", "timestamp", "date"]]

            df.to_sql(
                "mobile_events_uncompressed",
                engine,
                if_exists="replace" if i == 1 else "append",  # Replace only for first file
                index=False
            )
        except SQLAlchemyError as e:
            print(f"Failed to write {file} due to: {e}")
            try:
                conn = engine.raw_connection()
                conn.rollback()
                conn.close()
            except Exception as rollback_err:
                print(f"Rollback failed: {rollback_err}")


# --------------------------
# Load CELL PLAN
# --------------------------
def load_cell_plan():
    print("\n Loading mobile network cell plan...")
    input_file = "data/processed/cells_with_lau.parquet"

    try:
        with engine.connect() as conn:
            try:
                conn.execute(text("DROP TABLE IF EXISTS cell_plan CASCADE;"))
                conn.commit()  
                print("Dropped table: cell_plan (if it existed).")
            except Exception as e:
                print("Could not drop cell_plan table:", e)

        df = pd.read_parquet(input_file)

        # Set date from valid_date_end
        df["date"] = pd.to_datetime(df["valid_date_end"]).dt.date

        # Select only required columns
        df = df[["cell_id", "latitude", "longitude", "date"]].copy()

        # Write to database
        df.to_sql("cell_plan", engine, if_exists="replace", index=False)
        print("Cell plan written to database.")

    except Exception as e:
        print(f"Failed to process or write cell plan: {e}")

# --------------------------
# Load LAU GEOMETRIES
# --------------------------
def load_lau():
    print("\n Loading LAU geometries from GeoJSON...")
    input_file = "data/raw/lau_boundaries/LAU_RG_01M_2021_3035.geojson"

    try:
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS lau CASCADE;"))
            conn.commit()  
            print("Dropped table: lau (if it existed).")

        gdf = gpd.read_file(input_file)
        # Filter only Estonia (CNTR_CODE == 'EE')
        gdf = gdf[gdf["CNTR_CODE"] == "EE"]
        # Rename and select only necessary columns
        gdf = gdf.rename(columns={
            "GISCO_ID": "lau_id",
            "CNTR_CODE": "country_code",
            "LAU_NAME": "lau_name"
        })[["lau_id", "country_code", "lau_name", "geometry"]]

        # Upload to PostGIS (already in EPSG:3035)
        gdf.to_postgis("lau", engine, if_exists="replace", index=False)
        print("LAU table written to database.")

    except Exception as e:
        print(f"Failed to load LAU data: {e}")

def main():
    load_compressed_events()
    load_uncompressed_events()
    load_cell_plan()
    load_lau()
    print("\n Full data load complete.")

if __name__ == "__main__":
    main()
