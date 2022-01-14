import os
from typing import Dict, List, Union, Optional
from urllib.request import urlretrieve

import pandas as pd

from utils import (
    get_project_root_dir,
    extract_csv_from_url
)

def extract_fcc_broadband_geography_lookup_table(
    project_root_dir: os.path = get_project_root_dir(), return_df: bool = True
) -> pd.DataFrame:
    data_documentation_url = (
        "https://opendata.fcc.gov/Wireline/Geography-Lookup-Table/v5vt-e7vw"
    )
    file_name = "fcc_broadband_geography_lookup_table.csv"
    url = "https://opendata.fcc.gov/api/views/v5vt-e7vw/rows.csv?accessType=DOWNLOAD&sorting=true"
    file_path = os.path.join(project_root_dir, "data_raw", file_name)

    return extract_csv_from_url(file_path=file_path, url=url, return_df=return_df)


def extract_fcc_broadband_providers_12_2020(
    project_root_dir: os.path = get_project_root_dir(), return_df: bool = True
) -> pd.DataFrame:
    data_documentation_url = (
        "https://opendata.fcc.gov/Wireline/Provider-Table-December-2020/2ra3-4jd4"
    )
    file_name = "fcc_broadband_provider_table_12_2020.csv"
    url = "https://opendata.fcc.gov/api/views/2ra3-4jd4/rows.csv?accessType=DOWNLOAD&sorting=true"
    file_path = os.path.join(project_root_dir, "data_raw", file_name)

    return extract_csv_from_url(file_path=file_path, url=url, return_df=return_df)


def extract_fcc_broadband_area_coverage_12_2020(
    project_root_dir: os.path = get_project_root_dir(), return_df: bool = True
) -> pd.DataFrame:
    data_documentation_url = (
        "https://opendata.fcc.gov/Wireline/Area-Table-December-2020/ymd4-xaiz"
    )
    file_name = "fcc_broadband_area_coverage_12_2020.csv"
    url = "https://opendata.fcc.gov/api/views/ymd4-xaiz/rows.csv?accessType=DOWNLOAD&sorting=true"
    file_path = os.path.join(project_root_dir, "data_raw", file_name)

    return extract_csv_from_url(file_path=file_path, url=url, return_df=return_df)


def extract_fcc_broadband_wi_fixed_12_2020(
    project_root_dir: os.path = get_project_root_dir(), return_df: bool = True
) -> pd.DataFrame:
    data_documentation_url = (
        "https://www.fcc.gov/general/explanation-broadband-deployment-data"
    )
    file_name = "fcc_broadband_wi_fixed_12_2020.zip"
    url = (
        "https://public.boxcloud.com/d/1/b1!MLzsLVgzxBl-hChTRyvk-prUAoAy4OLuDCE5J8"
        + "Yt-q5ci9a5gyeTFLIeiH8ZKkLF1VQ4Wvu6GaGyrszfNGMQn_seQUYjHO3irK8v6rt6gTWgfY4"
        + "BtJDc6SWkdOyEMRKre_8RAIVgLVZL3-y7rJMOXgW7VzTJuoszd-rp6t36zGabqxbFqkJFWvR9"
        + "L0WCboq7rwlGgu8RfXy-QYh5TESK5v1mMF91V_K6wlkzw17Pir_NgdP0TkmpzY_RcD-xWFshc"
        + "S_JL_RMLmH_HycBE0NAVIs7twrNbJROMgIQjg83AWqYZD502E7upG5C8YYfWb6s3ZrFudUH5V"
        + "3LnebtT2tqrT2InmdHPLePfUxqmNjdABdq1KXQWbV5PPC3Sadyt3n6p7_tjE4YoE5KEPCETzn"
        + "oteC4YHFJq0AHPleoE9H2fSjNL6AYBmk--MuehsywN851ztNKRlpIGJBXw1XOEIaWiatjz8sb"
        + "m-YPtjnt7AMouwXHGHtQLwc1jawBzo82FRkbV9bvXUDLfwCAyaGWsBr8PJJUBQhaBTV70oPCE"
        + "Mzflv75cBAdZGayVkeGnIEoX3MfaiQTuRydrLKLjC9iK1XaFwe-ShizvMMJx-_-5fGeAfgcX6"
        + "oBoZyexv3344D2vl8cwomwF1UuYCHKpPSkEBnssdb56bZ0HLoAxoVcicqU7JQAWKGdwB3YP5P"
        + "-QGzOzd51132ggfOnDelkYGJJW1JLZae3K8B7Mbp5OLEYuU1V_jszrc6lndhUD9ohZtrpUQZb"
        + "D9xd9DOdT9BYwUuAXw2SpGXt7cQEfTt5SgIOnQ2w1NnxxTav7ce1rhyvVU081qAUZmCTReBjt"
        + "0RgMl6UWOod39oHENR1QY1fQ8Jk_nmgn3PqbxHwv3V_tmypXWVrZdiErp34YkWBy85Sjc-XF_"
        + "XaoA9Ad_Pfly7uPzDXdp_nTmDXmahsmGHV5QKAcvmsFfcZwcWe9FcYiFNv_c56ZyX4pyKyPy7"
        + "GSYPeL9Y3__v2ROCpgMntJD1ccmaz_05hoa8-B0CoDYhjNYlhbdC_lRlsK0Ii_Doi6l26Z8XG"
        + "YEgUGy6WnmJYUn9JhLDnN3FcxfowKUTtZ-7Q93-YzvBB0hsuwtqLLHXQPz8G3w9qlU-5yGdDq"
        + "3H3PHZfG1FdHRIMULNvj7Lpo5Nal2hJbDMc0gT-iurz0tSiLysqIBXlPZwP2zB6g2Y8C6Enw1"
        + "bRnfyA0tJyzqd0mleo0EazvkVG-iXn5z0gH8gdIsfRuYQKHYz-QnzDdOEywcXgoSeIOBcetWP"
        + "Wlv50JYtz2T9rXjzu5CUi1Zc2wJfqx6Q_4gci/download"
    )
    file_path = os.path.join(project_root_dir, "data_raw", file_name)

    return extract_csv_from_url(file_path=file_path, url=url, return_df=return_df)


def extract_fcc_broadband_mi_fixed_12_2020(
    project_root_dir: os.path = get_project_root_dir(), return_df: bool = True
) -> pd.DataFrame:
    data_documentation_url = (
        "https://www.fcc.gov/general/explanation-broadband-deployment-data"
    )
    file_name = "fcc_broadband_mi_fixed_12_2020.zip"
    url = (
        "https://public.boxcloud.com/d/1/b1!COa6U5tDIOCIcV4k4q1lKubvVXfqq3Oc-f3DTzSyw"
        + "cffjnKxp2uujAI9l69J7WLaDdoh7SKQh9WzPqe2Af4PGn7uNy4fqmETjF8yu9ciez6Ia0XEIammt"
        + "2DNguZwtHmogsagYqx5uPmzazTFtCppjkRF7WaFKZkKMLMDRXarYhRp9i_tT3jdv66CE45P9K-w_"
        + "mjA-uoNqGIwBDS3opgaQonDbQPVnubxx2dE87GYB_SsPxAGXEeRXnrJ0QTL_jMN0s8TFUP9uWtE5"
        + "PUOsBd2sWsHoAlU8SlqY86rjb1Yxxz5sws0sCZ8NRew6ImyHsPS-GEFfOjMkd04y9f7hJsjSvkTi"
        + "TjBjdtIJwy_tt06kQSA3AafiQ5cqgyTjCsuVWl8hrX9EdSB474YAfm497rXdVUnlvrTjzU6Hp-s-"
        + "5yV3nXWeNg9nIhRc1YWeEA_SNkeXFe2FXJbul5qiwNPymPriJ3hR0VgPuOgHKPhobgrY-VwRp4lX"
        + "aPcnX02r6hKCNYx1kwzFC66-7VAfJ-4lIe9hNkj9cdun6V5ooDHGJ64b57GcZQ69OTQLw1J4fb48"
        + "T6N41ovzUX-m6p1GMUqyrZWodX2T0I_yPc15ZFIPRJzjUvwlOrLApW_eEmMA8wenEOMaErpuRD5V"
        + "IJXzqGqcsPLoVxFJuolsMZkE6BzS9CY8YkQPUG0F3yzmidEnM7-cMCx-J2djl8R6zJJIbYNts4rF"
        + "WtmGBkijMPqdliiLlikCK5Zo6Brq8kN4llG0kiTnIWLIUo5uIaOB19RdU9NuHUM80Edh7RtyV7wI"
        + "JSVnTkBQhcBq6Rvq6RvVVUhO5bEN4TQeg5TyxFXWHJjOhSdy4rMg-Gy8a6zvad5H29Y3C5BdeRHQ"
        + "oSAgmDPXoUzShS61lojpODTJz6zRV20pnizJmQZ-Kn3C36Qg1aQIFG4Q8P7sY1C5JI7VTahSKVBE"
        + "vukxWngqZozWThY79Ntbib5FkK9oNTrkhH8KR_xgT_8CzpPQwnuI33dmL5pr6_CM-8M5OzLhKvex"
        + "38GuWEJaqh8yqifxzvMJX_fkhGgyUjWJf8A4UApeUtQU9XRwy8LofAZcNTWLyfhWLcX37eFo05CK"
        + "qTZ4sedoFp1Ioz7XOc-cVxUOs3PtX-RlxGK-RNlnlSMN-aOfweYInwz1mlKCBFBlDpMdlfOj4Qt-"
        + "AZH00T5Id93UHA1ncjmlYMi2WJSeKfMCUV8YilY0utq1uiTSCveZZssRvD7gUsFY2ZvE6-hy9yIu"
        + "z-WYwkyYMoGLH7_bA7CGFtNmQ1Im5sd7ZGacISJEJhjyvuuk2t9n0x18UG_/download"
    )
    file_path = os.path.join(project_root_dir, "data_raw", file_name)

    return extract_csv_from_url(file_path=file_path, url=url, return_df=return_df)


def main() -> None:
    PROJECT_ROOT_DIR = get_project_root_dir()
    setup_project_structure(project_root_dir=PROJECT_ROOT_DIR)
    extract_fcc_broadband_geography_lookup_table(
        project_root_dir=PROJECT_ROOT_DIR, return_df=False
    )
    extract_fcc_broadband_providers_12_2020(
        project_root_dir=PROJECT_ROOT_DIR, return_df=False
    )
    extract_fcc_broadband_area_coverage_12_2020(
        project_root_dir=PROJECT_ROOT_DIR, return_df=False
    )
    extract_fcc_broadband_wi_fixed_12_2020(
        project_root_dir=PROJECT_ROOT_DIR, return_df=False
    )
    extract_fcc_broadband_mi_fixed_12_2020(
        project_root_dir=PROJECT_ROOT_DIR, return_df=False
    )


if __name__ == "__main__":
    main()
