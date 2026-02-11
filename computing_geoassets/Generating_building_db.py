import geopandas as gpd
import pandas as pd


def Load_bat_file(folder_name)-> gpd.GeoDataFrame :
    gdf=gpd.read_file(f"./input_data/{folder_name}/gpkg/bdnb.gpkg",layer="batiment_groupe_compile")
    return gdf

def gdf_select(gdf) -> gpd.GeoDataFrame:
    gdf=gdf[["bdtopo_bat_l_usage_1","geometry"]]
    return

def select_and_concat(gdf1,gdf2,gdf3,gdf4)-> gpd.GeoDataFrame :
    gdf1=gdf_select(gdf1)
    gdf2=gdf_select(gdf2)
    gdf3=gdf_select(gdf3)
    gdf4=gdf_select(gdf4)
    gdf = pd.concat([gdf1, gdf2, gdf3, gdf4], ignore_index=True)
    gdf = gpd.GeoDataFrame(gdf, geometry='geometry', crs=gdf1.crs)
    return gdf


if __name__ == "__main__":
    # Loading grouped building DB downloaded from https://bdnb.io/download/ (CSTB 2025)
    bat_75=Load_bat_file("open_data_millesime_2025-07-a_dep75_gpkg")
    bat_92=Load_bat_file("open_data_millesime_2025-07-a_dep92_gpkg")
    bat_93=Load_bat_file("open_data_millesime_2025-07-a_dep93_gpkg")
    bat_94=Load_bat_file("open_data_millesime_2025-07-a_dep94_gpkg")
    # Loading population from https://data.humdata.org/dataset/france-high-resolution-population-density-maps-demographic-estimates (Juil 2019)
    Population_dataset = pd.read_csv("population_fra_2019-07-01.csv")
    # Dataset reduction and Concat
    bat_complet=select_and_concat(bat_75,bat_92,bat_93,bat_94)

