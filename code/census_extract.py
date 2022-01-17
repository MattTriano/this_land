import os
import re
from typing import Dict, List, Union, Optional

import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt

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


def plot_roads_by_feature_class_in_county_in_census_year(
    state_abrv: str,
    county_name: str,
    year: str,
    county_roads_gdf: Optional[gpd.GeoDataFrame] = None,
    counties_gdf: Optional[gpd.GeoDataFrame] = None,
    project_root_dir: os.path = get_project_root_dir(),
    fig_width: int = 20,
    output_image: bool = False,
    pad_pct: float = 0.03,
    top_pad_mult: float = 2.5,
) -> None:
    if county_roads_gdf is None:
        county_roads_gdf = load_tiger_county_roads_from_one_year(
            state_abrv=state_abrv,
            county_name=county_name,
            year=year,
        )
    county_gdf = load_tiger_county_boundary_from_one_year_and_county(
        state_abrv=state_abrv,
        county_name=county_name,
        year=year,
        counties_gdf=counties_gdf,
    )
    fig, ax = plt.subplots(figsize=(fig_width, fig_width))
    ax = county_roads_gdf.loc[(county_roads_gdf["MTFCC"] == "S1740")].plot(
        color="#8c510a",
        label="Private Road",
        linewidth=fig_width * 0.015,
        linestyle="--",
        alpha=0.8,
        ax=ax,
    )
    ax = county_roads_gdf.loc[(county_roads_gdf["MTFCC"] == "S1400")].plot(
        color="#b7b7b9",
        label="Public Local Road",
        linewidth=fig_width * 0.025,
        alpha=0.8,
        ax=ax,
    )
    ax = county_roads_gdf.loc[(county_roads_gdf["MTFCC"] == "S1200")].plot(
        color="#ec1c24",
        label="Secondary Road",
        linewidth=fig_width * 0.05,
        alpha=0.8,
        ax=ax,
    )
    ax = county_roads_gdf.loc[
        (county_roads_gdf["MTFCC"].isin(["S1100", "S1630"]))
    ].plot(
        color="#59abdd",
        label="Primary Road",
        linewidth=fig_width * 0.075,
        alpha=0.8,
        ax=ax,
    )
    ax = county_gdf.plot(
        color="none", linewidth=fig_width * 0.25, edgecolor="black", ax=ax
    )

    county_bounds = county_gdf["geometry"].bounds
    county_max_lat = county_bounds["maxy"].max()
    county_min_lat = county_bounds["miny"].min()
    county_lat_span = county_max_lat - county_min_lat
    county_lat_pad = county_lat_span * pad_pct
    _ = ax.set_ylim(
        [
            county_min_lat - county_lat_pad,
            county_max_lat + (county_lat_pad * top_pad_mult),
        ]
    )

    _ = ax.set_title(
        f"Roads in {county_name} County, {state_abrv} (per {year} census data)",
        fontsize=fig_width * 1.5,
    )
    _ = ax.tick_params(labelsize=fig_width * 0.5)

    handles, labels = ax.get_legend_handles_labels()
    handles.reverse()
    labels.reverse()
    lgnd = ax.legend(
        handles,
        labels,
        fontsize=fig_width * 0.9,
        loc="upper center",
        ncol=4,
        frameon=False,
        markerscale=0.1,
    )
    for legend_handle in lgnd.legendHandles:
        legend_handle.set_linewidth(fig_width * 0.25)
    if output_image:
        cn = county_name.lower().replace(" ", "_")
        sn = state_abrv.upper()
        output_image_file_path = os.path.join(
            project_root_dir,
            "output",
            f"map_of_roads_in_{cn}_{sn}_in_{year}_by_road_feature_class.png",
        )
        plt.savefig(
            output_image_file_path,
            transparent=False,
            facecolor="white",
            bbox_inches="tight",
        )
