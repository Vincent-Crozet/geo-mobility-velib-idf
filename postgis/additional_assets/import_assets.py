import os
import time
import json
import geopandas as gpd
import pandas as pd
import psycopg2
import glob
from shapely.geometry import MultiPolygon
from sqlalchemy import create_engine,text
from psycopg2.extras import execute_values
from shapely.geometry import mapping 


# 🔧 PostgreSQL connexion (Verify availability + connexion)
def wait_for_db(DB_CONN,max_retries=10):
    for i in range(max_retries):
        try:
            engine = create_engine(DB_CONN)
            with engine.connect() as conn:
                print("✅ Connected to database.")
                return
        except Exception as e:
            print(f"⏳ Waiting for DB... ({i+1}/{max_retries})")
            time.sleep(3)
    raise Exception("❌ Could not connect to the database.")

# 🔧 Ensure schema exists
def ensure_schema_exists(engine, schema_name: str):
    with engine.begin() as conn:
        conn.execute(
            text(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        )
        print(f"✅ Schema '{schema_name}' ready.")
############### GEODATA ##########################
def insert_geodata(file):
    gdf = gpd.read_file(file)
    print(f"📦 Loaded {len(gdf)} features from GeoJSON.")
    return gdf

def create_table_with_geometry(engine, gdf,TABLE_NAME: str):
    gdf.to_postgis(TABLE_NAME, engine, if_exists="replace", index=True)
    print(f"✅ Table '{TABLE_NAME}' created (with geometry).")

############# TABLE DATA ##########################
def insert_data(file):
    df = pd.read_csv(file)
    print(f"📦 Loaded {len(df)} features from GeoJSON.")
    return df

def create_table_without_geometry(engine, df,TABLE_NAME: str):
    df.to_sql(TABLE_NAME, engine, if_exists="replace", index=True)
    print(f"✅ Table '{TABLE_NAME}' created (without geometry).")
####################################################


if __name__ == "__main__":
    POSTGRES_DB=os.environ["POSTGRES_DB"]
    POSTGRES_USER=os.environ["POSTGRES_USER"]
    POSTGRES_PASSWORD=os.environ["POSTGRES_PASSWORD"]
    POSTGRES_HOST=os.environ["POSTGRES_HOST"]
    POSTGRES_PORT=os.environ["POSTGRES_PORT"]
    ##
    ADD_ASSETS_SCHEMA = "add_assets"
    ##
    DB_CONN = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    wait_for_db(DB_CONN)
    engine = create_engine(DB_CONN)
    ensure_schema_exists(engine, ADD_ASSETS_SCHEMA)
    # enable_postgis_extension()
    liste_gpkg = glob.glob(f"*.gpkg")
    liste_csv = glob.glob(f"*.csv")
    print(f"Files to insert : {liste_gpkg}")
    print(f"Files to insert : {liste_csv}") 
    for file in liste_gpkg:
        print(f"File currently uploaded : {file}")
        TABLE_NAME=ADD_ASSETS_SCHEMA+"."+os.path.splitext(os.path.basename(file))[0].lower()
        print(f"insertion successfull Table name: {TABLE_NAME}")
        gdf = insert_geodata(file)
        create_table_with_geometry(engine, gdf,TABLE_NAME)
    for file in liste_csv:
        print(f"le fichier a ouvrir est : {file}")
        TABLE_NAME=ADD_ASSETS_SCHEMA+"."+os.path.splitext(os.path.basename(file))[0].lower()
        df = insert_data(file)
        print(f"insertion successfull Table name: {TABLE_NAME}")
        create_table_without_geometry(engine, df,TABLE_NAME)

