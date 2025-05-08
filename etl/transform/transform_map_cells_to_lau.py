import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import os

# Define paths
input_cells_path = "data/raw/cells/year=2024/month=10/day=1/"
input_lau_path = "data/raw/lau_boundaries/LAU_RG_01M_2021_3035.geojson"
output_path = "data/processed/cells_with_lau.parquet"

# Step 1: Read all cell files
print(f"Reading cell parquet files from: {input_cells_path}")
cell_files = [os.path.join(input_cells_path, f) for f in os.listdir(input_cells_path) if f.endswith(".parquet")]
df_cells = pd.concat([pd.read_parquet(f) for f in cell_files], ignore_index=True)

# Step 2: Convert to GeoDataFrame in WGS84
print("Converting cells to GeoDataFrame (EPSG:4326)...")
gdf_cells = gpd.GeoDataFrame(
    df_cells,
    geometry=gpd.points_from_xy(df_cells["longitude"], df_cells["latitude"]),
    crs="EPSG:4326"
)

# Step 3: Read LAU polygons and filter Estonia
print(f"Reading LAU GeoJSON: {input_lau_path}")
gdf_lau = gpd.read_file(input_lau_path)
gdf_lau = gdf_lau[gdf_lau["CNTR_CODE"] == "EE"]

# Step 4: Reproject cells to EPSG:3035 to match LAU
print("Reprojecting cell points to EPSG:3035...")
gdf_cells_3035 = gdf_cells.to_crs(epsg=3035)

# Step 5: Spatial join
print("Performing spatial join between cells and LAU polygons...")
gdf_joined = gpd.sjoin(gdf_cells_3035, gdf_lau, how="left", predicate="within")

# Step 6: Save result (excluding geometry)
print(f"Writing result to: {output_path}")
gdf_joined.drop(columns="geometry").to_parquet(output_path, index=False)

print("Cell-to-LAU mapping complete.")
