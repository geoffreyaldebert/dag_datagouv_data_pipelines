import os
from typing import List, TypedDict, Optional

import psycopg2


class File(TypedDict):
    source_name: str
    source_path: str
    column_order: Optional[str]
    header: Optional[bool]


def get_conn(
    PG_HOST: Optional[str],
    PG_PORT: Optional[int],
    PG_DB: Optional[str],
    PG_USER: Optional[str],
    PG_PASSWORD: str,
    PG_SCHEMA: Optional[str],
):
    """Get connection to postgres instance

    Args:
        PG_HOST (str): host
        PG_PORT (int): port
        PG_DB (str): db / schema
        PG_USER (str): user
        PG_PASSWORD (str): password
        PG_SCHEMA (str): schema

    Returns:
        conn: Connection to postgres instance
    """
    if not PG_SCHEMA:
        PG_SCHEMA = "public"

    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT,
        options=f"-c search_path={PG_SCHEMA}",
    )
    return conn


def return_sql_results(cur):
    """Return data from a sql query

    Args:
        cur (Cursor): cursor from postgres connection

    Returns:
        dict, bool: result of sql query or True if no result but
        correct execution
    """
    try:
        data = cur.fetchall()
    except Exception:
        data = None
    if data:
        columns = [desc[0] for desc in cur.description]
        return [{k: v for k, v in zip(columns, d)} for d in data]
    else:
        return True


def execute_query(
    PG_HOST: Optional[str],
    PG_PORT: Optional[int],
    PG_DB: Optional[str],
    PG_USER: Optional[str],
    PG_PASSWORD: str,
    sql: str,
    PG_SCHEMA: Optional[str] = None,
):
    """Run a sql query to postgres instance

    Args:
        PG_HOST (str): host
        PG_PORT (str): port
        PG_DB (str): db / schema
        PG_USER (str): user
        PG_PASSWORD (str): password
        sql (str): sql query to execute

    Returns:
        dict, bool: result of sql query or True if no result but
        correct execution
    """
    conn = get_conn(PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD, PG_SCHEMA)
    with conn.cursor() as cur:
        cur.execute(sql)
        data = return_sql_results(cur)
        conn.commit()
        conn.close()
    return data


def execute_sql_file(
    PG_HOST: Optional[str],
    PG_PORT: Optional[int],
    PG_DB: Optional[str],
    PG_USER: Optional[str],
    PG_PASSWORD: str,
    list_files: List[File],
    PG_SCHEMA: Optional[str] = None,
):
    """Run sql queries in local files to postgres instance

    Args:
        PG_HOST (str): host
        PG_PORT (str): port
        PG_DB (str): db / schema
        PG_USER (str): user
        PG_PASSWORD (str): password
        list_files (List[File]): List of files containing sql queries.
        Local files are specified in a array of dictionnaries containing for each
        `source_path` and `source_name` : path of local sql file to execute.

    Raises:
        Exception: If one of the local file does not exist

    Returns:
        dict, bool: result of sql query or True if no result but
        correct execution
    """
    for file in list_files:
        is_file = os.path.isfile(os.path.join(file["source_path"], file["source_name"]))
        if is_file:
            conn = get_conn(PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD, PG_SCHEMA)
            with conn.cursor() as cur:
                cur.execute(
                    open(
                        os.path.join(file["source_path"], file["source_name"]), "r"
                    ).read()
                )
                data = return_sql_results(cur)
                conn.commit()
                conn.close()
        else:
            raise Exception(
                f"file {file['source_path']}{file['source_name']} does not exists"
            )
    return data


def copy_file(
    PG_HOST: Optional[str],
    PG_PORT: Optional[int],
    PG_DB: Optional[str],
    PG_TABLE: Optional[str],
    PG_USER: Optional[str],
    PG_PASSWORD: str,
    list_files: List[File],
    PG_SCHEMA: Optional[str] = None,
    has_header: Optional[bool] = True,
):
    """Copy raw data from local files to postgres instance

    Args:
        PG_HOST (str): host
        PG_PORT (int): port
        PG_DB (str): db / schema
        PG_TABLE (str): table to upload data
        PG_USER (str): user
        PG_PASSWORD (str): password
        list_files (List[File]): List of files containing raw data.
        Local files are specified in a array of dictionnaries containing for each
        `source_path` and `source_name` : path of local sql file to execute.

    Raises:
        Exception: _description_

    Returns:
        _type_: _description_
    """
    for file_conf in list_files:
        is_file = os.path.isfile(
            os.path.join(file_conf["source_path"], file_conf["source_name"])
        )
        if is_file:
            conn = get_conn(PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD, PG_SCHEMA)
            if "column_order" in file_conf and file_conf["column_order"] is not None:
                COLUMNS = file_conf["column_order"]
            else:
                COLUMNS = ""
            if has_header:
                HEADER = "HEADER"
            else:
                HEADER = ""
            file = open(
                os.path.join(file_conf["source_path"], file_conf["source_name"]), "r"
            )
            with conn.cursor() as cur:
                cur.copy_expert(
                    sql=(
                        f"COPY {PG_TABLE} {COLUMNS} FROM STDIN "
                        f"WITH CSV {HEADER} DELIMITER AS ','"
                    ),
                    file=file,
                )
                data = return_sql_results(cur)
                conn.commit()
                conn.close()
        else:
            raise Exception(
                f"file {file_conf['source_path']}{file_conf['source_name']} does not exists"
            )
    return data
