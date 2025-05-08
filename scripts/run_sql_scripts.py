import os
import glob
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

pg_user = os.getenv("POSTGRES_USER")
pg_pass = os.getenv("POSTGRES_PASSWORD")
pg_db   = os.getenv("POSTGRES_DB")
pg_host = os.getenv("POSTGRES_HOST", "localhost")
pg_port = os.getenv("POSTGRES_PORT", "5433")

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname=pg_db,
    user=pg_user,
    password=pg_pass,
    host=pg_host,
    port=pg_port
)
cursor = conn.cursor()

# Find all .sql files in the sql/ directory
sql_folder = "sql"
sql_files = sorted(glob.glob(os.path.join(sql_folder, "*.sql")))

print(f"Found {len(sql_files)} SQL files to run...")

for file in sql_files:
    print(f"\n Running: {file}")
    with open(file, "r") as f:
        sql = f.read()
        try:
            cursor.execute(sql)
            conn.commit()
        except Exception as e:
            print(f"Error executing {file}: {e}")
            conn.rollback()

print("\n All SQL scripts processed.")
cursor.close()
conn.close()
