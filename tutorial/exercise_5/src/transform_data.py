import datetime as dt

import pandas as pd
from fsspec import AbstractFileSystem
from config import settings


def load_raw_data(location: str, fs: AbstractFileSystem) -> pd.DataFrame:
    with fs.open(location) as f:
        df = pd.read_csv(f, usecols=["beds", "accommodates"])
    return df


def add_date(df: pd.DataFrame, date: dt.datetime) -> pd.DataFrame:
    df["datetime"] = date
    return df


def calculate_beds_per_accommodates(df: pd.DataFrame) -> pd.DataFrame:
    beds_pr_accomodation = df["accommodates"] / df["beds"]
    beds_pr_accomodation.name = "beds_pr_accomodation"
    return beds_pr_accomodation.to_frame()


def upload_feature(feature_name: str, df: pd.DataFrame, fs: AbstractFileSystem) -> None:
    containers = fs.ls(".")

    # Issue with exist_ok flag: see https://github.com/dask/adlfs/issues/130
    if not any(c.startswith(settings.features_container_name.strip("/")) for c in containers):
        fs.mkdir(settings.features_container_name)

    with fs.open(settings.feature_location(feature_name), mode="wb") as f:
        df.to_parquet(f)
