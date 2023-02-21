import os
import sys

import dask.dataframe as dd
import pandas as pd
from halo import Halo
from pyproj import Transformer

from validation import validation

sys.path.append(os.path.join(os.path.dirname(__file__), os.path.pardir))


"""
This module contains functions to work with Zensus data.

Zensus data can be downloaded here: "https://ergebnisse2011.zensus2022.de/datenbank/online/"
"""


def create_parquet_without_rows_with_no_residents() -> None:
    """
    Method drops every row in the census dataset without residents.
    Result is saved as a parquet file for efficient future handling.
    """
    df = pd.read_csv(
        "./datasets/Zensus_Bevoelkerung_100m-Gitter.csv", sep=";")
    df = df[df.Einwohner != -1]
    df.to_parquet('./datasets/generated/100m_cleared.parquet', index=False)


# def join_grid_with_census_dask() -> None:
#     """
#     For a faster performance and better handling,
#     the files of the census data in the 100x100m raster,
#     which contain the inhabitants per grid and those,
#     which contain the municipality key are merged into a joint parquet file.
#     Dask is used to prevent the RAM from being overused.
#     The resulting dataframe is then saved as a parquet file.

#     Learn more about dask here:
#     https://docs.dask.org/en/stable/
#     """
#     df1 = dd.read_parquet('./datasets/generated/combined_grid.parquet')
#     df2 = dd.read_parquet(
#         "./datasets/generated/100m_cleared_4326.parquet")

#     df2 = df2.rename(columns={'Gitter_ID_100m': 'id'})
#     df = dd.merge(df1, df2, how='inner', on='id')

#     df.to_parquet('./datasets/generated/merged_100m_cleared.parquet')


def join_grid_with_census() -> None:
    """
    For a faster performance and better handling, 
    the files of the census data in the 100x100m raster, 
    which contain the inhabitants per grid and those, 
    which contain the municipality key are merged into a joint parquet file. 
    """
    df_A = pd.read_parquet('./datasets/generated/combined_grid.parquet')
    df_B = pd.read_parquet(
        "./datasets/generated/100m_cleared_4326.parquet")
    df_B = df_B.rename(columns={'Gitter_ID_100m': 'id'})

    merged_df = pd.merge(df_A, df_B, on='id', how='inner')

    merged_df = merged_df[merged_df.Einwohner != -1]
    merged_df.to_parquet(
        './datasets/generated/merged_100m_cleared.parquet', index=False)


def cleanup_grid_census() -> None:
    """
    Some values in the census data of 2011 are incorrect. 
    These are corrected in this method. 
    The method overwrites the data file in which the unfixed file is located.
    """
    df = pd.read_parquet(
        './datasets/generated/merged_100m_cleared.parquet')
    # Remove Trier, City since it is not in the f27 dataset :(
    df = df[~df['ags'].str.startswith('07211')]
    # Fix AGS for Trier-Saarburg
    df['ags'] = df['ags'].apply(
        lambda x: '072' + x[3:] if x.startswith('079') else x)
    df['ags'] = df['ags'].apply(
        lambda x: '16063105' + x[3:] if x.startswith('16056000') else x)
    df.to_parquet(
        './datasets/generated/merged_100m_cleared.parquet', index=False)


def create_parquet_with_4326_crs() -> None:
    """
    This method transforms the census data from CRS epsg 3035 to epsg 4326. 
    This has the purpose that for later work with OpenStreetMap epsg 4326 is of advantage.
    The Result is saved in a parquet file.
    """
    df = pd.read_parquet(
        "./datasets/generated/100m_cleared.parquet")

    transformer = Transformer.from_crs("EPSG:3035", "EPSG:4326")

    df['x_mp_100m'], df['y_mp_100m'] = transformer.transform(
        df['y_mp_100m'].values, df['x_mp_100m'].values)

    df.to_parquet(
        './datasets/generated/100m_cleared_4326.parquet', index=False)


def transform_f27_to_parquet() -> None:
    """
    Method reads and processes the F27 record of the 2022 vehicle register 
    and thus prepares them for further processing. 
    The result is saved as a parquet file
    """
    path = './datasets/fz27_202207.xlsx'

    df = pd.read_excel(path,
                       sheet_name='FZ 27.15',
                       skiprows=12,
                       index_col=None,
                       dtype=str,
                       header=None)

    column_names = ['tmp', "Land", "Statistische Kennziffer", "Zulassungsbezirk", "Anzahl insgesamt", "Alternativer Antrieb Anzahl insgesamt", "Alternativer Antrieb Anteil in %", "davon Elektro-Antriebe insgesamt",
                    "davon Elektro-Antriebe anteil", "davon Elektro (BEV)", "davon Plug-in-Hybrid", "Hybrid Anzahl insgesamt", "darunter Benzin-Hybrid", "darunter Diesel-Hybrid", "Gas insgesamt"]
    df.columns = column_names
    numeric_columns = [
        "Anzahl insgesamt",
        "Alternativer Antrieb Anzahl insgesamt",
        "Alternativer Antrieb Anteil in %",
        "davon Elektro-Antriebe insgesamt",
        "davon Elektro-Antriebe anteil",
        "davon Elektro (BEV)",
        "davon Plug-in-Hybrid",
        "Hybrid Anzahl insgesamt",
        "darunter Benzin-Hybrid",
        "darunter Diesel-Hybrid",
        "Gas insgesamt",
    ]
    df.drop('tmp', axis=1, inplace=True)

    # making sure that decimal separators work properly
    for col in numeric_columns:
        df[col] = df[col].str.replace(",", ".").astype(float)

    df.drop(df.tail(5).index,
            inplace=True)

    df.loc[df['Zulassungsbezirk'] == 'TRIER-SAARBURG',
           'Statistische Kennziffer'] = '07235'
    df.to_parquet('./datasets/generated/fz_27_15.parquet', index=False)


def fix_changes_in_municipality_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    In 2011, there was the so-called district reform of Mecklenburg-Vorpommern. 
    Many districts were redistributed as part of this reform. 
    The data from the Federal Motor Transport Authority is from 2022 and takes these changes into account. 
    However, the census data from 2011 did not take all of these changes into account yet. 
    Therefore, in this method, this reform is done by hand for this data. 
    Here you can read more about it:
    https://de.wikipedia.org/wiki/Kreisgebietsreform_Mecklenburg-Vorpommern_2011

    Args:
        df (pd.DataFrame): the municipality dataset from census 2011

    Returns:
        pd.DataFrame: a reformed 2011 dataset
    """
    # Fix Ags for Göttingen
    df['AGS'] = df['AGS'].apply(
        lambda x: '03159' if x == '03152' else x)

    df_sum = df.groupby('AGS', as_index=False)['BFS_EWZ'].sum()
    # key(new district): value(old districts) pair.
    reformed_districts = {'13074': ['13006', '13058'],
                          '13076': ['13054', '13060'],
                          '13071': ['13052', '13056', '13055'],
                          '13072': ['13051', '13053'],
                          '13075': ['13001', '13059', '13062'],
                          '13073': ['13005', '13057', '13061']
                          }
    for district in reformed_districts:
        df_d = pd.DataFrame(
            {'AGS': district, 'BFS_EWZ': df_sum['BFS_EWZ'].sum()}, index=[0])
        df = df[~df['AGS'].isin(reformed_districts[district])]
        df = pd.concat([df, df_d], ignore_index=True)

    return df


def transform_municipality_census_to_parquet() -> None:
    """
    Method reads and processes the 2011 census data on the population 
    of municipalities and thus prepares them for further processing. 
    The result is saved as a parquet file
    """
    path = './datasets/1A_EinwohnerzahlGeschlecht.xls'
    df = pd.read_excel(path,
                       sheet_name='Tabelle_1A',
                       skiprows=12,
                       index_col=None,
                       dtype=str,)

    df['AGS'] = df['AGS'].astype(str)
    df['BFS_EWZ'] = df['BFS_EWZ'].astype(float)
    df['BFS_EWZ'] = df['BFS_EWZ'].apply(lambda x: x * 1000)
    df = df[df['AGS'].str.len() == 5]
    df = fix_changes_in_municipality_data(df)

    df.to_parquet(
        './datasets/generated/population_per_municipality.parquet', index=False)


def concat_csv_to_parquet() -> None:
    """
    For faster performance and better handling, 
    this method merges the csv files of the census data in a 100x100m grid into one large parquet file. 
    Only the columns id and ags are kept. This File is then saved
    """
    csv_folder_path = "./datasets/DE_Grid_ETRS89-LAEA_100m/geogitter"
    dataframes = []

    for filename in os.listdir(csv_folder_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(csv_folder_path, filename)
            df = pd.read_csv(file_path,
                             sep=';',
                             dtype=str,
                             header=None)
            dataframes.append(df)
    df = pd.concat(dataframes, ignore_index=True)

    df.columns = ["id",
                  "x_sw",
                  "y_sw",
                  "x_mp",
                  "y_mp",
                  "f_staat",
                  "f_land",
                  "f_wasser",
                  "p_staat",
                  "p_land",
                  "p_wasser",
                  "ags"]

    df.drop(["x_sw",
             "y_sw",
             "x_mp",
             "y_mp",
             "f_staat",
             "f_land",
             "f_wasser",
             "p_staat",
             "p_land",
             "p_wasser"], axis=1, inplace=True)

    df.to_parquet('./datasets/generated/combined_grid.parquet', index=False)


def run_data_preparation() -> None:
    """
    This Method runs all data preparation steps.
    It uses the Halo library to display a loading spinner 
    during each step of the data preparation process. The spinner indicates 
    which dataset is currently being processed, and whether the step was successful.
    """

    spinner = Halo("Loading")
    spinner.start(text="filtering Census Dataset")
    if not validation.check_generated('100m_cleared.parquet'):
        create_parquet_without_rows_with_no_residents()
    spinner.succeed()

    spinner.start(text="Transforming To EPSG:4326")
    if not validation.check_generated('100m_cleared_4326.parquet'):
        create_parquet_with_4326_crs()
    spinner.succeed()

    spinner.start(text="Preparing Vehicle Registration Dataset")
    if not validation.check_generated('fz_27_15.parquet'):
        transform_f27_to_parquet()
    spinner.succeed()

    spinner.start(text="Transforming 100x100m Grid CSV To Parquet Files")
    if not validation.check_generated('combined_grid.parquet'):
        concat_csv_to_parquet()
    spinner.succeed()
    spinner.start(
        text="Merging Census Data (This Step May Take A While: > 20 min on Apple M1)")
    if not validation.check_generated('merged_100m_cleared.parquet'):
        join_grid_with_census()
    spinner.succeed()
    spinner.start(text="Correcting Values In Census Data")
    cleanup_grid_census()
    spinner.succeed()
    spinner.start(text="Preparing Municipality Population Dataset")
    if not validation.check_generated('population_per_municipality.parquet'):
        transform_municipality_census_to_parquet()
    spinner.succeed()
