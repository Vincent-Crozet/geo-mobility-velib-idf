import geopandas as gpd
import pandas as pd

# Population data from 
# https://data.humdata.org/dataset/france-high-resolution-population-density-maps-demographic-estimates (Juil 2019)
Population= pd.read_csv("./input_data/population_fra_2019-07-01.csv")
Population = gpd.GeoDataFrame(Population,geometry=gpd.points_from_xy(Population["Lon"], Population["Lat"]),crs="EPSG:4326")

# IDF communes
communes=gpd.read_file("./input_data/communes_Idf.gpkg")
communes=communes[communes['numdep'].isin([75,92,93,94])]
communes=communes.to_crs(Population.crs)

# Spatial join to retain population data inside IDF communes
Population_idf = gpd.sjoin(Population, communes, how='inner', predicate='within')
Population_idf.columns = Population_idf.columns.str.lower()
Population_idf[["lat","lon","geometry","population"]].to_file("./derived_data/Population_IDF.gpkg",driver="GPKG")