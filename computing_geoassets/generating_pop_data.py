import geopandas as gpd
import pandas as pd

# Population data from 
# https://data.humdata.org/dataset/france-high-resolution-population-density-maps-demographic-estimates (Juil 2019)
df = pd.read_csv("/input_data/population_fra_2019-07-01.csv")
