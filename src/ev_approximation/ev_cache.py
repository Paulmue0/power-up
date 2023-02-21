import pandas as pd


class Cache:
    def __init__(self, f27_dataframe: pd.DataFrame, muni_dataframe: pd.DataFrame):

        self.path = None
        self.dataframe = None
        self.f27dataframe: pd.DataFrame = f27_dataframe
        self.muni_dataframe: pd.DataFrame = muni_dataframe

    def set_path(self, path) -> None:
        self.path = path

    def set_dataframe(self, dataframe) -> None:
        self.dataframe = dataframe

    def get_path(self) -> str:
        return self.path

    def get_dataframe(self) -> pd.DataFrame:
        return self.dataframe

    def get_f27dataframe(self) -> pd.DataFrame:
        return self.f27dataframe

    def get_municipality_dataframe(self) -> pd.DataFrame:
        return self.muni_dataframe
