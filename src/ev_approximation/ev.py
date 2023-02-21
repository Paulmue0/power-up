import os
import sys

import pandas as pd
from halo import Halo

from validation import validation

from .ev_cache import Cache

sys.path.append(os.path.join(os.path.dirname(__file__), os.path.pardir))


"""
This module contains functions to calculate the amount of Electronic Vehicles (EV) in Germany
"""


def generate_ev_estimates_for_census_data() -> None:
    """
    This function creates a new parquet file from the census data by adding a column "EV" 
    which represents the estimated number of electronic vehicles in each cell.
    The estimate is based on the population in the cell and the percentage of electronic 
    vehicles in the municipality the cell belongs to.

    Returns:
    None

    Raises:
    FileNotFoundError:  If the file population_per_municipality.parquet
                        or fz_27_15.parquet does not exist.
    """
    try:
        fields_f27 = ['Anzahl insgesamt',
                      'Statistische Kennziffer', 'davon Elektro (BEV)']
        df_f27 = pd.read_parquet('./datasets/generated/fz_27_15.parquet',
                                 columns=fields_f27, engine='pyarrow')
        fields_municipality = ['AGS', 'BFS_EWZ']
        df_municipality = pd.read_parquet('./datasets/generated/population_per_municipality.parquet',
                                          columns=fields_municipality, engine='pyarrow')
        cache = Cache(df_f27, df_municipality)
        df = sorted_data()
        cache.set_dataframe(df)

        municipality_keys = df['ags'].unique()
        ev_percent_map = {key: ev_percent_for_municipality(
            key, cache) for key in municipality_keys}

        df['EV'] = df['Einwohner'] * df['ags'].map(ev_percent_map)
        df.to_parquet('./datasets/generated/cleared_ev.parquet')
    except FileNotFoundError as e:
        print(
            f"The File population_per_municipality.parquet or fz_27_15.parquet was not found: {e}")


def ev_percent_for_municipality(municipality_key: str, cache: Cache) -> float:
    """
    Calculate the electric vehicle percentage for a given municipality key

    Parameters:
    municipality_key (str): The municipality key identifier
    cache (cache.Cache): The cache object to retrieve the required dataframe

    Returns:
    float: The electric vehicle percentage

    Raises:
    KeyError: If the given municipality key is not found in the dataframe
    """
    df_f27 = cache.get_f27dataframe()
    df_municipality = cache.get_municipality_dataframe()
    municipality_code = municipality_key[0:5]
    try:
        row_f27 = df_f27.loc[df_f27['Statistische Kennziffer']
                             == municipality_code]
        row_municipality = df_municipality.loc[df_municipality['AGS']
                                               == municipality_code]
        population_in_municipality = row_municipality['BFS_EWZ'].values[0]
        ev_in_municipality = row_f27['davon Elektro (BEV)'].values[0]
        ev_percent = ev_in_municipality/population_in_municipality
        return ev_percent
    except KeyError as e:
        print(
            f"The administrative region {municipality_key} was not found in the dataframe: {e}")


def sorted_data() -> pd.DataFrame:
    """
    This function reads a parquet file, adds a sort key to it based on the value of the 'id' 
    column and returns the sorted dataframe.

    The id_sort_key column is created by concatenating the values from the 4th to 7th
    and the 10th to 13th characters of the Gitter_ID_100m column. 
    This creates a sort key in the format of NXXEXX, where N/E are letters and X is a variable.

    Returns:
    pd.DataFrame: The sorted dataframe

    Raises:
    FileNotFoundError: If the file './datasets/generated/100m_4326.parquet' does not exist.
    """
    try:
        df = pd.read_parquet(
            './datasets/generated/merged_100m_cleared.parquet')
        df['id_sort_key'] = df['id'].str[4:7] + \
            df['id'].str[10:13]
        df = df.sort_values(by='id_sort_key')
        df = df.drop('id_sort_key', axis=1)
        return df
    except FileNotFoundError as e:
        print(
            f"The file './datasets/generated/merged_100m_cleared.parquet' does not exist: {e}")


def run_ev_calculation() -> None:
    """
    Method to run all necessary steps for the ev calculation.
    """
    spinner = Halo(text="Calculating EV's For Each Grid")
    spinner.start()
    if not validation.check_generated("cleared_ev.parquet"):
        generate_ev_estimates_for_census_data()
    spinner.succeed()
