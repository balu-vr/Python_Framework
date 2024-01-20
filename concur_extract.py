from __future__ import annotations
import xmltodict
import requests as req
import pandas as pd
import io
import os
import boto3
from pydantic import BaseModel, ValidationError
from typing import Any, Tuple
import argparse
from io import BytesIO
import psycopg2
import json
import logging

LOGGER = "concur"


formatter = logging.Formatter("%(asctime)s [%(levelname)s]: %(message)s")
handler = logging.FileHandler(os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "concur.log"
))
handler.setFormatter(formatter)
handler.setLevel(logging.DEBUG)
logger = logging.getLogger(LOGGER)
logger.addHandler(handler)


class DB(BaseModel):
    user: str
    password: str
    host: str
    port: str
    db_name: str

    def connection(self) -> psycopg2.connection:
        """
        Create engine for the database.

        Returns
        -------
            psycopg2.connection
                Connection to the database.
        """
        connection = psycopg2.connect(dbname=self.db_name,
                                      host=self.host,
                                      port=self.port,
                                      user=self.user,
                                      password=self.password)
        return connection

    @classmethod
    def from_file(cls, file_path: str) -> DB:
        """
        Parse connection arguments from .json file under "db" key.

        Parameters
        ----------
            file_path : str
                Path to the file to parse.

        Returns
        -------
            DB
                Instance of the class.
        """
        logger = logging.getLogger(LOGGER)
        try:
            logger.debug(file_path)
            with open(file_path) as json_file:
                return cls(**json.load(json_file)["db"])
        except ValidationError:
            logger.exception(
                "Not appropriate number of parameters \
                or they have incorrect types. Salesforce problem"
            )
            raise


class S3(BaseModel):
    bucket: str
    region: str
    access_key_id: str
    secret_access_key: str

    def connect(self) -> Any:
        """
        Establish connection to the s3.

        Returns
        -------
            Any
                Connection to the ss3.
        """
        connection = boto3.client(
            service_name="s3",
            region_name=self.region,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key
        )
        return connection

    @classmethod
    def from_file(cls, file_path: str) -> S3:
        """
        Parse connection arguments from .json file under the "s3" key.

        Parameters
        ----------
            file_path : str
                Path to the file to parse.

        Returns
        -------
            S3
                Instance of the class.
        """
        logger = logging.getLogger(LOGGER)
        try:
            with open(file_path) as json_file:
                return cls(**json.load(json_file)["s3"])
        except ValidationError:
            logger.exception(
                "Not appropriate number of parameters \
                or they have incorrect types"
            )
            raise


def get_args(
    configurations: str, connections: str
) -> Tuple[str, str, str, str]:
    """
    Get path to connections file passed as argument.

    Parameters
    ----------
        configurations : str
            Path to the configurations file.

        connections : str
            Path to the connections file.

    Returns
    -------
        Tuple[str, str, str, str]
            Paths to the connections and configurations, mode and log level.
    """
    parser = argparse.ArgumentParser()
    if not os.path.isfile(configurations):
        parser.add_argument(
            "-p", "--path",
            type=str,
            dest="path",
            help="Path to the configuration file"
        )
    if not os.path.isfile(connections):
        parser.add_argument(
            "-c", "--connections",
            type=str,
            dest="connections",
            help="Path to the connection file"
        )
    parser.add_argument(
        "-l", "--loglevel",
        type=str,
        dest="loglevel",
        choices=["INFO", "DEBUG", "ERROR"],
        default="INFO",
        help="Log level. One of ['INFO', 'DEBUG', 'ERROR'], default 'INFO'"
    )

    args = parser.parse_args()
    try:
        path_configurations = args.path
        if path_configurations:
            configurations = path_configurations
    except AttributeError:
        pass

    try:
        path_connections = args.connections
        if path_connections:
            connections = path_connections
    except AttributeError:
        pass

    return connections, configurations, args.loglevel


def initialize_connections(path_to_file: str) -> Tuple[DB, S3]:
    """
    Instantiate classes, used for connection purposes.

    Parameters
    ----------
        path_to_file : str
            Path to the configuration file.

    Returns
    -------
        Tuple[DB, S3, HubspotClient]
            Corresponding class instances.
    """
    db = DB.from_file(path_to_file)
    s3 = S3.from_file(path_to_file)
    return db, s3


def get_token(token_url: str, token_payload: dict[str, str]) -> str:
    """
    TODO
    """
    logger = logging.getLogger(LOGGER)

    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    resp = req.post(url=token_url, data=token_payload, headers=headers)

    if resp.status_code != 200:
        raise RuntimeError(
            f"Unable to retrieve access key, {resp.status_code=}"
        )

    logger.info("Access token received")
    token = resp.json()["access_token"]
    logger.debug(f"{token=}")

    return token


def get_data(token: str, data_url: str, mappingfile: str,  json_file=[]) -> pd.DataFrame:
    """
    TODO
    """
    logger = logging.getLogger(LOGGER)
    print("url:", data_url)
    auth = f"Bearer {token}"
    header = {"Authorization": auth}

    data_dict = {}

    if 'https://us2.concursolutions.com/' in data_url:
        resp = req.get(data_url, headers=header, stream=True)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Unable to retrieve access key, {resp.status_code=}"
                )
        data_dict = xmltodict.parse(resp.content)


    else:
        print("reached end of the api calls")

    _json1 = json.dumps(data_dict)
    _json2 = json.loads(_json1)

    Final_Json = []
    if _json2:
        Final_Json = _json2['Reports']['Items']['Report']

    if json_file:
        Final_Json.extend(json_file)
        print("Final Json length:", len(Final_Json))


    try:
        next_page_url = _json2['Reports']['NextPage']
    except KeyError as e :
        next_page_url = None
    except Exception as e:
    # Handle request-related errors
        print(f"Error: {e}")
        next_page_url = None



    if next_page_url:
        get_data(token,next_page_url,mappingfile,Final_Json)

    else:
        df = pd.json_normalize(Final_Json)

        logger.info("Mapping columns from DDL...")
        map_df = pd.read_csv(mappingfile, usecols=["Source", "Target"])
        df = df[df.columns.intersection(map_df["Source"].values)]
        map_dict = dict(map_df.to_dict(orient="split")["data"])
    # refined = refined.drop_duplicates(subset="EntryId", keep="first")
        df = df.rename(columns=map_dict)
        print("df cols after renaming:", df.columns.tolist())

        logger.debug(f"df db columns: {df.columns.tolist()}")

        return df


def put_to_s3(df: pd.DataFrame, s3: S3, key: str) -> None:
    """
    Put the dataframe to the s3.

    Parameters
    ----------
        df : pd.DataFrame
            Dataframe, representing salesforce object.

        s3 : S3
            Instance of S3 class.

        key : str
            Object name in S3 bucket.

    Returns
    -------
        None
    """
    compression = "gzip" if key.endswith(".gz") else None
    s3_connection = s3.connect()
    buffer = BytesIO()

    df.to_csv(
        buffer,
        index=False,
        compression=compression,
        sep=";",
        line_terminator="\n"
    )
    s3_connection.put_object(
        Bucket=s3.bucket,
        Key=key,
        Body=buffer.getvalue()
    )


def put_to_database(
    db: DB,
    s3: S3,
    columns: list[str],
    key: str,
    bidw_schema: str,
    stage_schema: str,
    bidw_table: str,
    stg_table: str
) -> None:
    """
    Put the dataframe to the database.
    Truncate bidw table and reload it.

    Parameters
    ----------
        df : pd.DataFrame
            Dataframe, containing contacts information from the hubspot.

        s3 : S3
            S3 class instance.

        columns : List[str]
            Dataframe/Table columns.

        key : str
            Object name in S3 bucket.

    Returns
    -------
        None
    """
    columns_sql = ', '.join(columns).lower()
    truncate_stage = f"TRUNCATE table {stage_schema}.{stg_table};"
    truncate_bidw = f"TRUNCATE table {bidw_schema}.{bidw_table};"
    copy_command = \
        f"COPY {stage_schema}.{stg_table} ({columns_sql}) "\
        f"FROM 's3://{s3.bucket}/{key}' "\
        f"CREDENTIALS 'aws_access_key_id={s3.access_key_id};"\
        f"aws_secret_access_key={s3.secret_access_key}' delimiter ';' "\
        f"CSV "\
        f"GZIP "\
        f"TIMEFORMAT AS 'YYYY-MM-DDTHH:MI:SS' "\
        f"DATEFORMAT 'MM/DD/YYYY' "\
        f"COMPUPDATE OFF "\
        f"STATUPDATE OFF "\
        f"EMPTYASNULL "\
        f"BLANKSASNULL "\
        f"TRUNCATECOLUMNS "\
        f"TRIMBLANKS "\
        f"IGNOREHEADER AS 1 "\
        f"IGNOREBLANKLINES "\
        f"ACCEPTINVCHARS AS '^';"

    insert_command = \
        f"INSERT INTO {bidw_schema}.{bidw_table} ({columns_sql}) "\
        f"SELECT DISTINCT {columns_sql} "\
        f"FROM {stage_schema}.{stg_table};"

    with db.connection() as con:
        with con.cursor() as cur:
            cur.execute(truncate_stage)
            cur.execute(copy_command)
            cur.execute(truncate_bidw)
            cur.execute(insert_command)


def main():
    connections_path = os.path.join(
        os.path.dirname(__file__), "concur_connections.json"
    )
    configurations_path = os.path.join(
        os.path.dirname(__file__), "concur_configurations.json"
    )
    connections, configurations, loglevel = get_args(
        connections=connections_path, configurations=configurations_path
    )
    logger = logging.getLogger(LOGGER)
    logger.setLevel(loglevel)
    logger.info(f"{'Start':-^40s}")
    logger.debug(f"Using {connections} and {configurations}")

    logger.info("Step 1: Initializing connections...")
    db, s3 = initialize_connections(path_to_file=connections)

    logger.info("Step 2: Reading configurations...")
    with open(configurations) as conf_file:
        json_with_params = json.load(conf_file)
        logger.debug(f"{json_with_params=}")
        s3_folder = json_with_params["s3_folder"]
        s3_file = json_with_params["s3_file"]
        data_url = json_with_params["data_url"]
        mappingfile = json_with_params["mappingfile"]
        bidw_schema = json_with_params["bidw_schema"]
        stage_schema = json_with_params["stage_schema"]
        bidw_table = json_with_params["bidw_table"]
        stage_table = json_with_params["stage_table"]

    with open(connections) as conn_file:
        json_with_params = json.load(conn_file)
        token_url = json_with_params["api"]["token_url"]
        token_payload = json_with_params["api"]["token_payload"]

    logger.info("Step 3: Getting API token...")
    token = get_token(token_url, token_payload)

    logger.info("Step 4: Getting data from the API...")
    df = get_data(token, data_url, mappingfile)
    print("df final count:", len(df))

    """
    logger.info("Step 5: Put the data to the s3...")
    put_to_s3(df=df, s3=s3, key=f"{s3_folder}/{s3_file}")

    logger.info("Step 6: Put the data to the database...")
    put_to_database(
        db=db, s3=s3,
        columns=df.columns.tolist(),
        key=f"{s3_folder}/{s3_file}",
        bidw_schema=bidw_schema,
        bidw_table=bidw_table,
        stage_schema=stage_schema,
        stg_table=stage_table
    )

    logger.info(f"{'End':-^40s}")

    """

if __name__ == "__main__":
    logger = logging.getLogger(LOGGER)
    try:
        main()
    except Exception:
        logger.exception("Unhandled exception occured")
        raise

