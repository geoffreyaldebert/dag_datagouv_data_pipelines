import csv
import glob
import os
from typing import Any


def save_list_of_dict_to_csv(
    records_list: list[dict[str, Any]], destination_file: str
) -> None:
    """
    Appends a list of dictionaries to a CSV file.

    Creates the CSV if it does not exist yet.
    All dictionaries from the list must share the same keys or a ValueError exception is raised.

    Args:
        records_list (list[dict[str, Any]]): A list of dictionaries where each one
            will represent a row of values in the CSV file. Keys are headers.
        destination_file (str): The path to the destination CSV file.
    """
    file_exists = os.path.isfile(destination_file)
    with open(destination_file, "a", newline="") as csv_file:
        csv_writer = csv.DictWriter(csv_file, records_list[0].keys(), delimiter=";")
        if not file_exists:
            csv_writer.writeheader()
        for row in records_list:
            csv_writer.writerow(row)


def remove_files_from_directory(directory: str) -> None:
    os.makedirs(directory, exist_ok=True)
    for f in glob.glob(f"{directory}/*"):
        os.remove(f)
