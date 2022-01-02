"""Module for Profiling functions """

from pandas_profiling import ProfileReport


def create_pandas_profile(df, output_file_prefix, **parameter):
    """Create Pandas Profile from Dataframe"""
    output_file_prefix = output_file_prefix.replace("_", " ").upper()
    title = f"{output_file_prefix} Profile Report"
    profile = ProfileReport(df, title, **parameter)
    return profile
