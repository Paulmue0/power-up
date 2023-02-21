import os


def create_directories() -> None:
    """
    Checks whether the directories ./maps, ./datasets, and 
    ./datasets/generated exist. 
    If they do not exist, the function creates them.
    """

    dir_paths = ['./maps', './datasets', './datasets/generated']

    for path in dir_paths:
        if not os.path.exists(path):
            os.makedirs(path)
            print(f"Directory '{path}' created successfully.")
        else:
            print(f"Directory '{path}' already exists.")


def check_datasets() -> bool:
    """
    Checks if the required files and geogitter directory are present in the ./datasets directory. 

    Returns:
        True if all required files and the geogitter directory are present in the ./datasets directory. 
        False if one or more files or the geogitter directory are missing.
    """
    dir_path = './datasets'
    file_names = ['Zensus_Bevoelkerung_100m-Gitter.csv',
                  'fz27_202207.xlsx',
                  '1A_EinwohnerzahlGeschlecht.xls',
                  'export.geojson',
                  'Ladesaeulenregister.csv'
                  ]

    for file_name in file_names:
        file_path = os.path.join(dir_path, file_name)
        if not os.path.exists(file_path):
            print(
                f"You are missing the file {file_name}. Lookup the README.md to read more")
            return False

    geogitter_dir_path = os.path.join(
        dir_path, 'DE_Grid_ETRS89-LAEA_100m/geogitter')
    num_files = len(os.listdir(geogitter_dir_path))
    if num_files != 60:
        print(
            f"You are missing at least one file in {file_name}. Lookup the README.md to read more")
        return False
    return True


def check_generated(file_path: str) -> bool:
    """
    Checks if a file exists in the ./datasets/generated directory.

    Args:
        file_path (str): The path to the file to check.

    Returns:
        True if the file exists in the ./datasets/generated directory. 
        False if the file does not exist in the ./datasets/generated directory.
    """
    dir_path = './datasets/generated'
    file_name = os.path.basename(file_path)
    file_exists = os.path.exists(os.path.join(dir_path, file_name))

    return file_exists
