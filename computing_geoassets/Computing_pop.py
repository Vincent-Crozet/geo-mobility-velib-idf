import geopandas as gpd
import pandas as pd

# Population data from 
# https://data.humdata.org/dataset/france-high-resolution-population-density-maps-demographic-estimates (Juil 2019)
df = pd.read_csv("population_fra_2019-07-01.csv")

# Local caracterization INSEE over 200m length grid
# https://www.insee.fr/fr/statistiques/4176290#consulter (2015)



gdf = gpd.GeoDataFrame(
    df,
    geometry=gpd.points_from_xy(df["Lon"], df["Lat"]),
    crs="EPSG:4326"  # WGS84
)

communes=gpd.read_file("communes_Idf.gpkg")

Pop_IDF=gdf.sjoin(communes, how="inner", predicate='within')    
Pop_IDF.to_file("Pop_IDF.gpkg", driver="GPKG")

pop_commune = Pop_IDF.groupby("nomcom").agg(
    Population_total=("Population", "sum")
    ).reset_index()

pop_commune=pop_commune.merge(communes[['nomcom', 'geometry']], on='nomcom')
pop_commune_gdf = gpd.GeoDataFrame(pop_commune, geometry='geometry', crs="EPSG:4326")
pop_commune_gdf.to_file("Pop_commune_Idf.gpkg", driver="GPKG")