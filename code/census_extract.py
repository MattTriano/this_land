import os
import re
from typing import Dict, List, Union, Optional

import pandas as pd
import geopandas as gpd

from utils import (
    get_project_root_dir,
    extract_csv_from_url,
    extract_file_from_url,
)
from constants import STATE_ABRV_TO_FIPS_CODE_CROSSWALK


def extract_tiger_state_lines_from_one_year(
    year: str,
    project_root_dir: os.path = get_project_root_dir(),
    return_df: bool = True,
) -> gpd.GeoDataFrame:
    data_documentation_url = (
        "https://www.census.gov/programs-surveys/geography/technical-documentation/"
        + f"complete-technical-documentation/tiger-geo-line.{year}.html"
    )
    file_name = f"census_tiger_state_lines_{year}.zip"
    url = f"https://www2.census.gov/geo/tiger/TIGER{year}/STATE/tl_{year}_us_state.zip"
    file_path = os.path.join(project_root_dir, "data_raw", file_name)

    return extract_file_from_url(
        file_path=file_path, url=url, data_format="shp", return_df=return_df
    )


def extract_tiger_county_lines_from_one_year(
    year: str,
    project_root_dir: os.path = get_project_root_dir(),
    return_df: bool = True,
) -> pd.DataFrame:
    data_documentation_url = (
        "https://www.census.gov/programs-surveys/geography/technical-documentation/"
        + f"complete-technical-documentation/tiger-geo-line.{year}.html"
    )
    file_name = f"census_tiger_county_lines_{year}.zip"
    url = (
        f"https://www2.census.gov/geo/tiger/TIGER{year}/COUNTY/tl_{year}_us_county.zip"
    )
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


def crosswalk_state_abrv_to_state_fips_code(state_abrv: str) -> str:
    state_abrv = state_abrv.upper()
    assert state_abrv in STATE_ABRV_TO_FIPS_CODE_CROSSWALK.keys()
    return STATE_ABRV_TO_FIPS_CODE_CROSSWALK[state_abrv]


def extract_county_fips_to_county_name_crosswalk(
    project_root_dir: os.path = get_project_root_dir(), year: str = "2021"
) -> None:
    file_path = os.path.join(
        project_root_dir, "data_clean", "crosswalks", "county_fips_code_crosswalk.csv"
    )
    if not os.path.isfile(file_path):
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        counties_gdf = extract_tiger_county_lines_from_one_year(
            year=year, project_root_dir=project_root_dir, return_df=True
        )
        county_fips_code_crosswalk = counties_gdf[
            ["STATEFP", "COUNTYFP", "NAME"]
        ].copy()
        county_fips_code_crosswalk = county_fips_code_crosswalk.sort_values(
            by=["STATEFP", "COUNTYFP"]
        )
        county_fips_code_crosswalk = county_fips_code_crosswalk.reset_index(drop=True)
        county_fips_code_crosswalk.to_csv(file_path, index=False)
        county_fips_code_crosswalk.to_parquet(
            file_path.replace(".csv", ".parquet.gzip"), compression="gzip"
        )


def load_county_fips_to_county_name_crosswalk(
    project_root_dir: os.path = get_project_root_dir(),
    crosswalk_file_extension: str = "parquet.gzip",
) -> pd.DataFrame:
    crosswalk_file_extension = re.sub(r"^\.", "", crosswalk_file_extension).lower()
    assert crosswalk_file_extension.lower() in ["parquet.gzip", "csv"]
    file_path = os.path.join(
        project_root_dir,
        "data_clean",
        "crosswalks",
        f"county_fips_code_crosswalk.{crosswalk_file_extension}",
    )
    if not os.path.isfile(file_path):
        extract_county_fips_to_county_name_crosswalk(project_root_dir=project_root_dir)
    if crosswalk_file_extension == "parquet.gzip":
        return pd.read_parquet(file_path)
    else:
        return pd.read_csv(file_path, dtype="string")


def crosswalk_county_name_to_county_fips_code(
    state_abrv: str,
    county_name: str,
    project_root_dir: os.path = get_project_root_dir(),
) -> str:
    state_fips_code = crosswalk_state_abrv_to_state_fips_code(state_abrv=state_abrv)
    county_fips_to_county_name_crosswalk = load_county_fips_to_county_name_crosswalk(
        project_root_dir=project_root_dir
    )
    county_fips_code = county_fips_to_county_name_crosswalk.loc[
        (county_fips_to_county_name_crosswalk["STATEFP"] == state_fips_code)
        & (
            county_fips_to_county_name_crosswalk["NAME"].str.lower()
            == county_name.lower()
        ),
        "COUNTYFP",
    ].values[0]
    return county_fips_code


def load_tiger_county_boundary_from_one_year_and_county(
    state_abrv: str,
    county_name: str,
    year: str,
    counties_gdf: Optional[gpd.GeoDataFrame] = None,
    project_root_dir: os.path = get_project_root_dir(),
) -> gpd.GeoDataFrame:
    state_fips_code = crosswalk_state_abrv_to_state_fips_code(state_abrv=state_abrv)
    if counties_gdf is None:
        counties_gdf = extract_tiger_county_lines_from_one_year(
            year=year, project_root_dir=project_root_dir
        )
    county_gdf = counties_gdf.loc[
        (counties_gdf["STATEFP"] == state_fips_code)
        & (counties_gdf["NAME"].str.lower() == county_name.lower())
    ].copy()
    county_gdf = county_gdf.reset_index(drop=True)
    return county_gdf
