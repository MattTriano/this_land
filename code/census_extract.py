import os
from typing import Dict, List, Union, Optional

import pandas as pd
import geopandas as gpd

from utils import (
    get_project_root_dir,
    extract_csv_from_url,
    extract_file_from_url
)


def extract_tiger_state_lines_2019(
    project_root_dir: os.path = get_project_root_dir(), return_df: bool = True
) -> gpd.GeoDataFrame:
    data_documentation_url = (
        "https://www.census.gov/programs-surveys/geography/technical-documentation/"
        + "complete-technical-documentation/tiger-geo-line.2019.html"
    )
    file_name = "census_tiger_state_lines_2019.zip"
    url = "https://www2.census.gov/geo/tiger/TIGER2019/STATE/tl_2019_us_state.zip"
    file_path = os.path.join(project_root_dir, "data_raw", file_name)

    return extract_file_from_url(
        file_path=file_path, url=url, data_format="shp", return_df=return_df
    )


def extract_tiger_county_lines_2019(
    project_root_dir: os.path = get_project_root_dir(), return_df: bool = True
) -> pd.DataFrame:
    data_documentation_url = (
        "https://www.census.gov/programs-surveys/geography/technical-documentation/"
        + "complete-technical-documentation/tiger-geo-line.2019.html"
    )
    file_name = "census_tiger_county_lines_2019.zip"
    url = "https://www2.census.gov/geo/tiger/TIGER2019/COUNTY/tl_2019_us_county.zip"
    file_path = os.path.join(project_root_dir, "data_raw", file_name)

    return extract_file_from_url(
        file_path=file_path, url=url, data_format="shp", return_df=return_df
    )


def extract_tiger_county_lines_2021(
    project_root_dir: os.path = get_project_root_dir(), return_df: bool = True
) -> pd.DataFrame:
    data_documentation_url = (
        "https://www.census.gov/programs-surveys/geography/technical-documentation/"
        + "complete-technical-documentation/tiger-geo-line.2021.html"
    )
    file_name = "census_tiger_county_lines_2021.zip"
    url = "https://www2.census.gov/geo/tiger/TIGER2021/COUNTY/tl_2021_us_county.zip"
    file_path = os.path.join(project_root_dir, "data_raw", file_name)

    return extract_file_from_url(
        file_path=file_path, url=url, data_format="shp", return_df=return_df
    )


def extract_tiger_state_lines_2021(
    project_root_dir: os.path = get_project_root_dir(), return_df: bool = True
) -> gpd.GeoDataFrame:
    data_documentation_url = (
        "https://www.census.gov/programs-surveys/geography/technical-documentation/"
        + "complete-technical-documentation/tiger-geo-line.2021.html"
    )
    file_name = "census_tiger_state_lines_2021.zip"
    url = "https://www2.census.gov/geo/tiger/TIGER2021/STATE/tl_2021_us_state.zip"
    file_path = os.path.join(project_root_dir, "data_raw", file_name)

    return extract_file_from_url(
        file_path=file_path, url=url, data_format="shp", return_df=return_df
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
