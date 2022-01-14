import os
from typing import Dict, List, Union, Optional

from utils import (
    get_project_root_dir,
    extract_csv_from_url,
    extract_file_from_url
)


def extract_north_american_rail_nodes(
    project_root_dir: os.path = get_project_root_dir(), return_df: bool = True
) -> pd.DataFrame:
    data_documentation_url = (
        "https://data-usdot.opendata.arcgis.com/datasets/usdot::north-american-rail-nodes/about"
    )
    file_name = "north_american_rail_nodes.geojson"
    url = "https://opendata.arcgis.com/api/v3/datasets/7958468db586471d94f97e99b916175a_0/downloads/data?format=geojson&spatialRefId=4326"
    file_path = os.path.join(project_root_dir, "data_raw", file_name)

    return extract_file_from_url(
        file_path=file_path, url=url, data_format="geojson", return_df=return_df
    )


def extract_north_american_rail_lines(
    project_root_dir: os.path = get_project_root_dir(), return_df: bool = True
) -> pd.DataFrame:
    data_documentation_url = (
        "https://data-usdot.opendata.arcgis.com/datasets/usdot::north-american-rail-lines/about"
    )
    file_name = "north_american_rail_lines.geojson"
    url = "https://opendata.arcgis.com/api/v3/datasets/d83e85154a304da995837889cc4012e3_0/downloads/data?format=geojson&spatialRefId=4326"
    file_path = os.path.join(project_root_dir, "data_raw", file_name)

    return extract_file_from_url(
        file_path=file_path, url=url, data_format="geojson", return_df=return_df
    )


def extract_amtrak_routes(
    project_root_dir: os.path = get_project_root_dir(), return_df: bool = True
) -> pd.DataFrame:
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
) -> pd.DataFrame:
    data_documentation_url = (
        "https://data-usdot.opendata.arcgis.com/datasets/usdot::amtrak-stations-1/about"
    )
    file_name = "amtrak_stations.geojson"
    url = "https://opendata.arcgis.com/api/v3/datasets/4cf728602fa3428ba0a08d30efbb5f45_0/downloads/data?format=geojson&spatialRefId=4326"
    file_path = os.path.join(project_root_dir, "data_raw", file_name)

    return extract_file_from_url(
        file_path=file_path, url=url, data_format="geojson", return_df=return_df
    )