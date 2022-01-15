import os
from typing import Dict, List, Union, Optional

import pandas as pd
import geopandas as gpd

from utils import (
    get_project_root_dir,
    extract_csv_from_url,
    extract_file_from_url,
    crosswalk_state_abrv_to_state_fips_code,
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


def extract_census_tracts_for_one_state_and_year(
    state_abrv: str,
    year: str,
    project_root_dir: os.path = get_project_root_dir(),
    return_df: bool = True,
) -> gpd.GeoDataFrame:
    state_abrv = state_abrv.upper()
    state_fips = crosswalk_state_abrv_to_state_fips_code(state_abrv=state_abrv)
    file_name = f"census_tracts_{state_abrv}_{year}.zip"
    url = f"https://www2.census.gov/geo/tiger/TIGER{year}/TRACT/tl_{year}_{state_fips}_tract.zip"
    file_path = os.path.join(project_root_dir, "data_raw", file_name)

    return extract_file_from_url(
        file_path=file_path, url=url, data_format="shp", return_df=return_df
    )


def extract_census_tracts_from_one_year_for_list_of_states(
    year: str,
    state_abrv_list: List[str],
    project_root_dir: os.path = get_project_root_dir(),
    return_df: bool = True,
) -> Optional[gpd.GeoDataFrame]:
    tract_gdf_list = []
    for state_abrv in state_abrv_list:
        tract_gdf_list.append(
            extract_census_tracts_for_one_state_and_year(
                state_abrv=state_abrv,
                year=year,
                project_root_dir=project_root_dir,
                return_df=True,
            )
        )
    if return_df:
        tract_gdf = pd.concat(tract_gdf_list)
        tract_gdf = tract_gdf.reset_index(drop=True)
        return tract_gdf
