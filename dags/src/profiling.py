"""Module for Profiling functions """

from pandas_profiling import ProfileReport
import pandas as pd


def create_pandas_profile(df: pd.DataFrame, file_name: str, **parameter):
    """Create Pandas Profile from Dataframe"""
    file_name_upper = file_name.replace("_", " ").upper()
    title = f"{file_name_upper} Profile Report"
    profile = ProfileReport(df, title, **parameter)
    return profile
