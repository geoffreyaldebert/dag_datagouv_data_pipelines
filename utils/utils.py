import csv
from datetime import date, datetime
import gzip
from typing import Optional

import duckdb

MOIS_FR = {
    "01": "janvier",
    "02": "février",
    "03": "mars",
    "04": "avril",
    "05": "mai",
    "06": "juin",
    "07": "juillet",
    "08": "août",
    "09": "septembre",
    "10": "octobre",
    "11": "novembre",
    "12": "décembre",
}


def check_if_monday():
    return date.today().weekday() == 0


def check_if_first_day_of_month():
    return date.today().day == 1


def check_if_first_day_of_year():
    return date.today().day == 1 and date.today().month == 1


def get_fiscal_year(date):
    # Get the fiscal year based on the month of the date
    return date.year if date.month >= 7 else date.year - 1


def month_year_iter(start_month, start_year, end_month, end_year):
    # includes both start and end months
    ym_start = 12 * start_year + start_month - 1
    ym_end = 12 * end_year + end_month - 1
    for ym in range(ym_start, ym_end + 1):
        y, m = divmod(ym, 12)
        yield y, m + 1


def csv_to_parquet(
    csv_file_path: str,
    dtype: Optional[dict] = None,
    columns: Optional[list] = None,
    output_name: Optional[str] = None,
    output_path: Optional[str] = None,
    sep: str = ";",
    compression: str = "zstd",
):
    """
    if dtype is not specified, columns are required to load everything as string (for safety)
    for allowed types see https://duckdb.org/docs/sql/data_types/overview.html
    """
    assert dtype is not None or columns is not None
    if output_name is None:
        output_name = csv_file_path.split('/')[-1].replace('.csv', '.parquet')
    if output_path is None:
        output_path = '/'.join(csv_file_path.split('/')[:-1]) + '/'
    print(f"Converting {csv_file_path}")
    print(f"to {output_path + output_name}")
    db = duckdb.read_csv(
        csv_file_path,
        sep=sep,
        dtype=dtype or {c: 'VARCHAR' for c in columns},
    )
    db.write_parquet(output_path + output_name, compression=compression)


def csv_to_csvgz(
    csv_file_path: str,
    output_name: Optional[str] = None,
    output_path: Optional[str] = None,
    chunk_size: int = 1024 * 1024,
):
    if output_name is None:
        output_name = csv_file_path.split('/')[-1].replace('.csv', '.csv.gz')
    if output_path is None:
        output_path = '/'.join(csv_file_path.split('/')[:-1]) + '/'
    print(f"Converting {csv_file_path}")
    print(f"to {output_path + output_name}")
    with (
        open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile,
        gzip.open(output_path + output_name, 'wt', newline='', encoding='utf-8') as gzfile
    ):
        while True:
            chunk = csvfile.read(chunk_size)
            if not chunk:
                break
            gzfile.write(chunk)


def time_is_between(time1, time2):
    # no date involved here
    if time1 > time2:
        time1, time2 = time2, time1
    return time1 <= datetime.now().time() <= time2
