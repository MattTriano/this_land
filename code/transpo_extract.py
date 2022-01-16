import os
from typing import Dict, List, Union, Optional

import pandas as pd
import geopandas as gpd

from utils import get_project_root_dir, extract_csv_from_url, extract_file_from_url
from census_extract import (
    crosswalk_state_abrv_to_state_fips_code,
    crosswalk_county_name_to_county_fips_code,
)


def extract_north_american_rail_nodes(
    project_root_dir: os.path = get_project_root_dir(), return_df: bool = True
) -> gpd.GeoDataFrame:
    data_documentation_url = "https://data-usdot.opendata.arcgis.com/datasets/usdot::north-american-rail-nodes/about"
    file_name = "north_american_rail_nodes.geojson"
    url = "https://opendata.arcgis.com/api/v3/datasets/7958468db586471d94f97e99b916175a_0/downloads/data?format=geojson&spatialRefId=4326"
    file_path = os.path.join(project_root_dir, "data_raw", file_name)

    return extract_file_from_url(
        file_path=file_path, url=url, data_format="geojson", return_df=return_df
    )


def extract_north_american_rail_lines(
    project_root_dir: os.path = get_project_root_dir(), return_df: bool = True
) -> gpd.GeoDataFrame:
    data_documentation_url = "https://data-usdot.opendata.arcgis.com/datasets/usdot::north-american-rail-lines/about"
    file_name = "north_american_rail_lines.geojson"
    url = "https://opendata.arcgis.com/api/v3/datasets/d83e85154a304da995837889cc4012e3_0/downloads/data?format=geojson&spatialRefId=4326"
    file_path = os.path.join(project_root_dir, "data_raw", file_name)

    return extract_file_from_url(
        file_path=file_path, url=url, data_format="geojson", return_df=return_df
    )


def extract_amtrak_routes(
    project_root_dir: os.path = get_project_root_dir(), return_df: bool = True
) -> gpd.GeoDataFrame:
    data_documentation_url = (
        "https://data-usdot.opendata.arcgis.com/datasets/usdot::amtrak-routes/about"
    )
    file_name = "amtrak_routes.geojson"
    url = "https://opendata.arcgis.com/api/v3/datasets/baa5a6c4d4ae4034850e99aaca38cfbb_0/downloads/data?format=geojson&spatialRefId=4326"
    file_path = os.path.join(project_root_dir, "data_raw", file_name)

    return extract_file_from_url(
        file_path=file_path, url=url, data_format="geojson", return_df=return_df
    )


def extract_amtrak_stations(
    project_root_dir: os.path = get_project_root_dir(), return_df: bool = True
) -> gpd.GeoDataFrame:
    data_documentation_url = (
        "https://data-usdot.opendata.arcgis.com/datasets/usdot::amtrak-stations-1/about"
    )
    file_name = "amtrak_stations.geojson"
    url = "https://opendata.arcgis.com/api/v3/datasets/4cf728602fa3428ba0a08d30efbb5f45_0/downloads/data?format=geojson&spatialRefId=4326"
    file_path = os.path.join(project_root_dir, "data_raw", file_name)

    return extract_file_from_url(
        file_path=file_path, url=url, data_format="geojson", return_df=return_df
    )


def extract_tiger_rail_lines_2021(
    project_root_dir: os.path = get_project_root_dir(), return_df: bool = True
) -> gpd.GeoDataFrame:
    data_documentation_url = (
        "https://www.census.gov/programs-surveys/geography/technical-documentation/"
        + "complete-technical-documentation/tiger-geo-line.2021.html"
    )
    file_name = "census_tiger_rail_lines_2021.zip"
    url = "https://www2.census.gov/geo/tiger/TIGER2021/RAILS/tl_2021_us_rails.zip"
    file_path = os.path.join(project_root_dir, "data_raw", file_name)

    return extract_file_from_url(
        file_path=file_path, url=url, data_format="shp", return_df=return_df
    )


def extract_tiger_county_roads_from_one_year(
    state_abrv: str,
    county_name: str,
    year: str,
    project_root_dir: os.path = get_project_root_dir(),
) -> None:
    state_abrv = state_abrv.upper()
    state_fips_code = crosswalk_state_abrv_to_state_fips_code(state_abrv=state_abrv)
    county_fips_code = crosswalk_county_name_to_county_fips_code(
        state_abrv=state_abrv, county_name=county_name
    )
    county_geoid = state_fips_code + county_fips_code

    url = f"https://www2.census.gov/geo/tiger/TIGER{year}/ROADS/tl_{year}_{county_geoid}_roads.zip"
    file_name = f"roads_in_{county_name.lower().replace(' ', '_')}_county_{state_abrv.upper()}_{year}.zip"
    file_path = os.path.join(project_root_dir, "data_raw", "roads", file_name)

    extract_file_from_url(
        file_path=file_path, url=url, data_format="shp", return_df=False
    )


def load_tiger_county_roads_from_one_year(
    state_abrv: str,
    county_name: str,
    year: str,
    project_root_dir: os.path = get_project_root_dir(),
) -> gpd.GeoDataFrame:
    file_name = f"roads_in_{county_name.lower().replace(' ', '_')}_county_{state_abrv.upper()}_{year}.zip"
    file_path = os.path.join(project_root_dir, "data_raw", "roads", file_name)
    if not os.path.isfile(file_path):
        extract_tiger_county_roads_from_one_year(
            state_abrv=state_abrv,
            county_name=county_name,
            year=year,
            project_root_dir=project_root_dir,
        )
    return gpd.read_file(file_path)
