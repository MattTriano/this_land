import os
from typing import Dict, List, Union, Optional
from urllib.request import urlretrieve

import pandas as pd
import geopandas as gpd

def get_project_root_dir() -> os.path:
    return os.path.dirname(os.path.dirname(os.path.abspath("__file__")))


def setup_project_structure(project_root_dir: os.path = get_project_root_dir()) -> None:
    os.makedirs(os.path.join(project_root_dir, "data_raw"), exist_ok=True)
    os.makedirs(os.path.join(project_root_dir, "data_clean"), exist_ok=True)
    os.makedirs(os.path.join(project_root_dir, "code"), exist_ok=True)

    
def extract_csv_from_url(
    file_path: os.path, url: str, force_repull: bool = False, return_df: bool = True
) -> pd.DataFrame:
    if not os.path.isfile(file_path) or force_repull:
        urlretrieve(url, file_path)
    if return_df:
        return pd.read_csv(file_path)
    

def extract_file_from_url(
    file_path: os.path, url: str, data_format: str, force_repull: bool = False, return_df: bool = True
) -> pd.DataFrame:
    if not os.path.isfile(file_path) or force_repull:
        urlretrieve(url, file_path)
    if return_df:
        if data_format in ["csv", "zipped_csv"]:
            return pd.read_csv(file_path)
        elif data_format in ["shp", "geojson"]:
            return gpd.read_file(file_path)