""" Module with functions to clean data in Dataframe """

import re
import numpy as np
import pandas as pd
from typing import Dict


def remove_html(raw_html):
    """Remove HTML Tags from String"""
    cleaner = re.compile("<.*?>")
    clean_text = re.sub(cleaner, "", raw_html)
    return clean_text


def remove_html_tags_in_df(df, html_cols):
    """Remove Html tags for specific Columns"""
    _remove_html = np.vectorize(remove_html)
    if isinstance(html_cols, list):
        if len(html_cols) > 0:
            for col in html_cols:
                df[col] = _remove_html(df[col])
    return df


def convert_string_to_datetime_in_df(df, date_cols):
    """Convert String Columns containg dates to Datetime"""
    if isinstance(date_cols, list):
        if len(date_cols) > 0:
            for col in date_cols:
                df[col] = pd.to_datetime(
                    df[col].str.replace("T", " ").replace("Z", ""),
                    format="%Y-%m-%d %H:%M:%S",
                )
    return df


def camel_to_snake(name):
    """Turn String written in Camel Case to Snake Case"""
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()


def camel_to_snake_in_df(df):
    """Convert Camel Cases Column Names to Snake Case for all Columns"""
    _camel_to_snake = np.vectorize(camel_to_snake)
    df.columns = _camel_to_snake(df.columns)
    return df


def rename_columns(df, prefixes_to_remove, rename_mapping):
    """First remove prefixes (if there are any) then rename columns (if there is a mapping)"""
    # Remove prefix from all column names
    if isinstance(prefixes_to_remove, list):
        if len(prefixes_to_remove) > 0:
            for prefix in prefixes_to_remove:
                df.columns = df.columns.str.replace(prefix, "")

    # Rename columns according to mapping
    if isinstance(rename_mapping, Dict):
        if len(rename_mapping) > 0:  # Check if Dictionary contains values
            df = df.rename(columns=rename_mapping)

    return df


def clean_df(
    df: pd.DataFrame,
    duplicate_identifier=None,
    drop_columns=None,
    html_columns=None,
    date_columns=None,
    prefixes_to_remove=None,
    rename_columns_mapping=None,
) -> pd.DataFrame:
    """Run different cleaning functions over Dataframe"""

    # Drop specific Columns
    if isinstance(drop_columns, list):
        if len(drop_columns) > 0:
            df = df.drop(columns=drop_columns)

    # Deduplication
    if isinstance(duplicate_identifier, str):
        df = df.drop_duplicates(subset=duplicate_identifier)

    # Replace Missings with Empty String to do String Operations
    df = df.fillna("")

    # Remove Html tags for specific Columns
    df = remove_html_tags_in_df(df, html_columns)

    # Replace Empty String with Missing for all Columns
    df = df.replace("", np.nan)

    # Remove Whitespaces for all Columns
    df = df.apply(lambda x: x.str.strip(), axis=1)

    # Convert String Columns containg dates to Datetime
    df = convert_string_to_datetime_in_df(df, date_columns)

    # Convert Camel Cases Column Names to Snake Case for all Columns
    df = camel_to_snake_in_df(df)

    # Rename specific Columns
    df = rename_columns(df, prefixes_to_remove, rename_columns_mapping)

    cleaned_df = df.copy()
    return cleaned_df
