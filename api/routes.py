# api/routes.py
from flask import Blueprint, Response
from sqlalchemy import create_engine
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

bp = Blueprint('routes', __name__)

# Database connection
pg_user = os.getenv("POSTGRES_USER", "postgres")
pg_pass = os.getenv("POSTGRES_PASSWORD", "postgres")
pg_db   = os.getenv("POSTGRES_DB", "mobility")
pg_host = os.getenv("POSTGRES_HOST", "localhost")
pg_port = os.getenv("POSTGRES_PORT", "5432")

engine = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")

# Reusable function to fetch and render any table/view
def render_view_as_html(view_name):
    df = pd.read_sql(f"SELECT * FROM {view_name}", con=engine)
    html_table = df.to_html(classes="table table-striped", index=False, border=1)
    return Response(html_table, mimetype='text/html')

# Base tables
@bp.route("/table/cell-plan")
def table_cell_plan():
    return render_view_as_html("cell_plan")

@bp.route("/table/lau")
def table_lau():
    return render_view_as_html("lau")

@bp.route("/table/mobile-events-compressed")
def table_mobile_events_compressed():
    return render_view_as_html("mobile_events_compressed")

@bp.route("/table/mobile-events-uncompressed")
def table_mobile_events_uncompressed():
    return render_view_as_html("mobile_events_uncompressed")

# Views for insights
@bp.route("/number-of-events-per-hour")
def number_of_events_per_hour():
    return render_view_as_html("agg_events_per_hour")

@bp.route("/number-of-unique-users-per-hour")
def number_of_unique_users_per_hour():
    return render_view_as_html("agg_unique_users_per_hour")

@bp.route("/number-of-unique-locations-per-user-per-day")
def unique_locations_per_user():
    return render_view_as_html("unique_locations_per_user_per_day")

@bp.route("/number-of-unique-users-per-lau-per-day")
def unique_users_per_lau_per_day():
    return render_view_as_html("unique_users_per_lau_per_day")

@bp.route("/number-of-users-per-lau-on-2024-10-01")
def users_per_lau_on_2024_10_01():
    return render_view_as_html("unique_users_per_lau_2024_10_01")

@bp.route("/number-of-events-per-hour-on-2024-10-01")
def events_per_hour_2024_10_01():
    return render_view_as_html("events_per_hour_2024_10_01")

@bp.route("/distribution-of-users-by-unique-locations")
def user_location_distribution():
    return render_view_as_html("user_location_distribution")

@bp.route("/number-of-unique-users-per-lau-day-vs-night")
def users_per_lau_day_night():
    return render_view_as_html("view_users_per_lau_day_night")
