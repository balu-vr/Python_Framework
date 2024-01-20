from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
import time
from datetime import datetime, timedelta
from dateutil.parser import parse
from io import BytesIO
from typing import Any, Dict, List, Tuple

import boto3
import numpy as np
import pandas as pd
import psycopg2
from pydantic import BaseModel, ValidationError
from simple_salesforce import Salesforce


class DB(BaseModel):
    user: str
    password: str
    host: str
    port: str
    db_name: str
    stage_schema: str
    bidw_schema: str

    def connection(self):
        """
        Create connection to the database.

        Returns
        -------
            Engine (Need to correct it, it's psycopg now, not sqlalchemy)
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
        Parse connection arguments from .json file under 'db' key

        Parameters
        ----------
            file_path : str
                Path to the file to parse.

        Returns
        -------
            DB
                Instance of the class.
        """
        try:
            with open(file_path) as json_file:
                return cls(**json.load(json_file)["db"])
        except ValidationError:
            sys.exit("Not appropriate number of parameters or they have incorrect types. Salesforce problem")


class SalesforceConnection(BaseModel):
    username: str
    password: str
    security_token: str

    def connect(self) -> Salesforce:
        """
        Establish connection to the salesforce

        Returns
        -------
            Salesforce
                Connection to salesforce.
        """
        connection = Salesforce(
            username=self.username,
            password=self.password,
            security_token=self.security_token
        )
        return connection

    @classmethod
    def from_file(cls, file_path: str) -> SalesforceConnection:
        """
        Parse connection arguments from .json file under 'salesforce' key

        Parameters
        ----------
            file_path : str
                Path to the file to parse.

        Returns
        -------
            SalesforceConnection
                Instance of the class.
        """
        try:
            with open(file_path) as json_file:
                return cls(**json.load(json_file)["salesforce"])
        except ValidationError:
            sys.exit("Not appropriate number of parameters or they have incorrect types. Salesforce problem")


class S3Connection(BaseModel):
    bucket: str
    region: str
    access_key_id: str
    secret_access_key: str

    def connect(self) -> Any:
        """
        Establish connection to the s3

        Returns
        -------
            Any
                Connection to s3.
        """
        connection = boto3.client(
            service_name='s3',
            region_name=self.region,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key
        )
        return connection

    @classmethod
    def from_file(cls, file_path: str) -> S3Connection:
        """
        Parse connection arguments from .json file under 's3' key

        Parameters
        ----------
            file_path : str
                Path to the file to parse.

        Returns
        -------
            S3Connection
                Instance of the class.
        """
        try:
            with open(file_path) as json_file:
                return cls(**json.load(json_file)["s3"])
        except ValidationError:
            sys.exit("Not appropriate number of parameters or they have incorrect types")


def get_args() -> Tuple[str, str]:
    """
    Get path to connections file passed as argument.

    Returns
    -------
        Tuple[str, str]
            Path to the connections file and name of the table.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--path',
                        type=str,
                        dest='path',
                        help='Path to the connections file')
    parser.add_argument('-o', '--object',
                        type=str,
                        dest='object',
                        help='Name of the salesforce object')
    args = parser.parse_args()
    path_to_file = args.path
    if not path_to_file:
        path_to_file = '/home/ec2-user/deploy/kpi/prod/salesforce/connections.json'
    object_name = args.object
    if not object_name:
        sys.exit("Please, specify the salesforce object name")
    return path_to_file, object_name


def initialize_connection_instances(path_to_file: str) -> Tuple[DB, SalesforceConnection, S3Connection]:
    """
    Create database, salesforce and s3 connection instances.

    Parameters
    ----------
        path_to_file : str
            Path to the file to make connections from.

    Returns
    -------
        Tuple[DB, SalesforceConnection, S3Connection]
            DB, SalesforceConnection, S3Connection instances.
    """
    db = DB.from_file(path_to_file)
    salesforce_connection = SalesforceConnection.from_file(path_to_file)
    s3_connection = S3Connection.from_file(path_to_file)
    return db, salesforce_connection, s3_connection


def get_main_and_transposed_columns(object_columns: Dict[str, List[str]]) -> Tuple[List[str], List[str]]:
    """
    Extract the list of main and transposed columns
    from the already read json file.

    Parameters
    ----------
        object_columns : Dict[str, List[str]]
            Dictionary from read json file.

    Returns
    -------
        Tuple[List[str], List[str]]
            Tuple with list of main fields and list of transposed fields
            that are related to the specific object.
    """
    main_columns = object_columns['main_fields']
    if 'transposed_fields' in object_columns:
        transposed_fields = object_columns['transposed_fields']
    else:
        transposed_fields = []
    return main_columns, transposed_fields


def get_last_updated_time(db: DB,
                          object_name: str,
                          timedelta_kwargs: Dict[str, int] = {'hours': 1}) -> datetime:
    """
    Get last updated time in the specific object table
    to refresh the table from this datetime.

    Parameters
    ----------
        db : DB
            Instance of DB class.

        object_name : str
            Object name to select date from appropriate table.

        timedelta_kwargs: Optional[Dict[str, int]]
            Parameters for timedelta function. By default 1 hour.

    Returns
    -------
        datetime
            Datetime of the last update.
    """
    object_name = object_name.lower()
    with db.connection() as con:
        with con.cursor() as cur:
            cur.execute(
                "SELECT createddate "
                + f"FROM {db.bidw_schema}.sfdc_kpi_{object_name} "
                + "ORDER BY createddate DESC "
                + "LIMIT 1;"
            )
            last_updated_time = cur.fetchall()
    last_updated_time = last_updated_time[0][0]  # datetime type
    last_updated_time -= timedelta(**timedelta_kwargs)
    return last_updated_time


def get_salesforce_data(object_name: str,
                        columns: List[str],
                        field_columns: List[str],
                        last_updated_date: datetime,
                        sf_connection: SalesforceConnection) -> pd.DataFrame:
    """
    Get data from salesforce objects

    Parameters
    ----------
        object_name : str
            Salesforce object (e.g. Quote, Contract etc.), equivalent to table.

        limit : int
            Limit the number of records selected at one query. Can't be more than 200.

        last_updated_date : datetime.datetime
            Datetime from which it is neeeded to get the new data.

        sf_connection : SalesforceConnection
            Instance of SalesforceConnection class.

    Returns
    -------
        pd.DataFrame
            Pandas dataframe object, contains selected fields of given object.
    """
    last_updated_date = last_updated_date.strftime("%Y-%m-%dT%H:%M:%SZ")  # type: ignore
    con = sf_connection.connect()
    ids_amount_response = con.query_all(
        "SELECT COUNT(Id) "
        + f"FROM {object_name} "
        + f"WHERE CreatedDate >= {last_updated_date} "
        + (f"AND Field in {tuple(field_columns)}" if field_columns else '')
    )
    ids_amount = ids_amount_response['records'][0]['expr0']  # integer, number of rows in the object
    num_of_iterations = math.ceil(ids_amount / 200)
    query = con.query_all(
        f"SELECT {', '.join(columns)} "
        + f"FROM {object_name} "
        + f"WHERE CreatedDate > {last_updated_date} "
        + (f"AND Field in {tuple(field_columns)} " if field_columns else '')
        + "ORDER BY Id "
        + "LIMIT 200"
    )
    df = pd.DataFrame(query['records']).drop(columns='attributes')
    while num_of_iterations > 0:
        trailing_date = df.iloc[-1]['CreatedDate']
        query = con.query_all(
            f"SELECT {','.join(columns)} "
            + f"FROM {object_name} "
            + f"WHERE CreatedDate > {trailing_date} "
            + (f"AND Field in {tuple(field_columns)}" if field_columns else '')
            + "ORDER BY Id LIMIT 200"
        )
        if query['records']:
            sub_df = pd.DataFrame(query['records']).drop(columns='attributes')
            df = pd.concat([df, sub_df], axis=0)
            num_of_iterations -= 1
        else:
            break
    return df


def transpose_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transpose columns that are basically unique row values in the Field column.
    In addition, drops Field, DataType, OldValue and NewValue from the dataframe.

    Parameters
    ----------
        df : pd.DataFrame
            Processed dataframe.
    """
    for col_name in df['Field'].unique():
        df[f'{col_name}_new'] = df.apply(
            lambda x: x['NewValue'] if x['Field'] == col_name else np.nan, axis=1
        )
        df[f'{col_name}_old'] = df.apply(
            lambda x: x['OldValue'] if x['Field'] == col_name else np.nan, axis=1
        )
    df = df.drop(['Field', 'DataType', 'OldValue', 'NewValue'], axis=1)
    return df


def _change_date(val):
    """
    Pandas function for .apply() method
    for parsing the date in column.
    Don't use anywhere else. No type hints.
    """
    if not pd.isnull(val) and not val:
        date = parse(val)
        return date.strftime('%Y-%m-%d')


def postprocess_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standartize the date and time formats for all the columns.

    Parameters
    ----------
        df : pd.DataFrame
            Dataframe to proceess.

    Returns
    -------
        pd.DataFrame
            Processed dataframe.
    """
    df['CreatedDate'] = df['CreatedDate'].apply(lambda x: x[:19] if x else np.nan)
    for column in df.columns.to_list():
        if column in {'EndDate_new', 'EndDate_old', 'StartDate_new', 'StartDate_old'}:
            df[column] = df[column].apply(_change_date)  # type: ignore
    df.columns = [column.lower() for column in df.columns]
    return df


def add_md5_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace some special characters and calculate md5 column

    Parameters
    ----------
        df : pd.DataFrame
            Dataframe for postprocessing.

    Returns
    -------
        pd.DataFrame
            Processed dataframe (with md5_column).

    """
    df = df.replace(regex={'&amp;': '&', '&nbsp;': ' ', r'<[^<>]*>': ''})  # type: ignore
    concat_series = df[sorted(df.columns)].apply(lambda x: '-'.join(x.astype(str).dropna()), axis=1)
    df['md5_column'] = concat_series.apply(lambda x: hashlib.md5(x.encode('utf-8')).hexdigest())
    return df


def get_data_from_s3(object_name: str, s3: S3Connection) -> pd.DataFrame:
    """
    Get data from s3 and read it as dataframe.

    Parameters
    ----------
        object_name : str
            Name of the object in the salesforce.

        s3 : S3Connection
            Instance of S3Connection class.

    Returns
    -------
        pd.DataFrame
            Dataframe representation of s3 csv file.
    """
    object_name = object_name.lower()
    path_to_s3_file = f'salesforce/{object_name}/{object_name}.csv'
    path_to_store = f'{object_name}.csv'
    s3_ssession = s3.connect()
    s3_ssession.download_file(s3.bucket, path_to_s3_file, path_to_store)
    df = pd.read_csv(path_to_store, sep=';')
    return df


def modify_s3_dataframe(s3_df: pd.DataFrame,
                        df_new_data: pd.DataFrame,
                        last_update_date: datetime) -> pd.DataFrame:
    """
    Modify the datetime range of the dataframe.

    Parameters
    ----------
        s3_df : pd.DataFrame
            Dataframe from s3 csv file.

        df_new_data : pd.DataFrame
            Dataframe with new data.

        last_update_date : datetime
            Datetime of the last update.

    Returns
    -------
        pd.DataFrame
            Updated dataframe.
    """
    # migrate columns from s3 to downloaded from salesforce
    for column in s3_df.columns.to_list():
        if column not in df_new_data.columns:
            df_new_data[column] = np.nan

    # change the order of the downloaded data columns
    df_new_data = df_new_data[s3_df.columns.to_list()]

    # resets for pd.concat()
    s3_df = s3_df.reset_index(drop=True)
    df_new_data = df_new_data.reset_index(drop=True)
    new_df = pd.concat([s3_df, df_new_data], axis=0, ignore_index=True)

    # delete all data later than last updated time + half a year
    lower_border = last_update_date - timedelta(days=1)
    lower_border = lower_border.strftime("%Y-%m-%dT%H:%M:%SZ")
    new_df = new_df[new_df['createddate'] >= lower_border].reset_index(drop=True)
    return new_df


def put_to_s3(df: pd.DataFrame, object_name: str, s3: S3Connection) -> None:
    """
    Put dataframe, represents salesforce object to s3

    Parameters
    ----------
        df : pd.DataFrame
            Dataframe, representing salesforce object.

        object_name : str
            Actual name of the salesforce object.

        s3 : S3Connection
            Instance of S3Connection class.

    Returns
    -------
        None
    """
    s3_ssession = s3.connect()
    buffer = BytesIO()
    object_name = object_name.lower()
    df.to_csv(buffer, index=False, sep=';')
    s3_ssession.put_object(Bucket=s3.bucket,
                           Key=f"salesforce/{object_name}/{object_name}.csv",
                           Body=buffer.getvalue())
    print(f'Object {object_name} was succesfully transfered to s3!')


def change_data_capture(db: DB,
                        object_name: str,
                        columns: List[str],
                        s3: S3Connection) -> None:
    """
    Perform change data capture:
    Truncate stg => copy to stg from s3 => delete rows from stg that are in prod => insert the rest from stg to prod

    Parameters
    ----------
        db : DB
            Instance of DB class.

        object_name : str
            Actual name of the salesforce object.

        columns : List[str]
            List of table columns excuding dw_ columns.

        s3 : S3Connection
            Instance of S3Connection class.
    """
    object_name = object_name.lower()
    columns_sql = ', '.join(columns).lower()

    con = db.connection()
    with con:
        with con.cursor() as cur:
            truncate_from_stage = f"TRUNCATE table {db.stage_schema}.stg_sfdc_kpi_{object_name}"
            cur.execute(truncate_from_stage)
            con.commit()

    time.sleep(2)

    with con:
        with con.cursor() as curs:
            copy_from_stg = f"COPY {db.stage_schema}.stg_sfdc_kpi_{object_name} ({columns_sql}) "\
                            f"FROM 's3://{s3.bucket}/salesforce/{object_name}/{object_name}.csv' "\
                            f"CREDENTIALS 'aws_access_key_id={s3.access_key_id};aws_secret_access_key={s3.secret_access_key}' "\
                            "DELIMITER ';' "\
                            "CSV "\
                            "DATEFORMAT 'YYYY-MM-DD' "\
                            "TIMEFORMAT 'YYYY-MM-DDTHH:MI:SS' "\
                            "COMPUPDATE OFF "\
                            "STATUPDATE OFF "\
                            "EMPTYASNULL "\
                            "BLANKSASNULL "\
                            "TRUNCATECOLUMNS "\
                            "TRIMBLANKS "\
                            "IGNOREHEADER AS 1 "\
                            "IGNOREBLANKLINES "\
                            "ACCEPTINVCHARS AS '^';"
            curs.execute(copy_from_stg)
            con.commit()

    time.sleep(2)

    with con:
        with con.cursor() as cur:
            delete_from_stg = f"DELETE FROM {db.stage_schema}.stg_sfdc_kpi_{object_name} "\
                              f"USING {db.bidw_schema}.sfdc_kpi_{object_name} "\
                              f"WHERE trim({db.bidw_schema}.sfdc_kpi_{object_name}.id) = trim({db.stage_schema}.stg_sfdc_kpi_{object_name}.id) "\
                              f"AND trim({db.bidw_schema}.sfdc_kpi_{object_name}.md5_column) = trim({db.stage_schema}.stg_sfdc_kpi_{object_name}.md5_column)"
            cur.execute(delete_from_stg)
            con.commit()

    time.sleep(2)

    with con:
        with con.cursor() as cur:
            insert_to_prod = f"INSERT INTO {db.bidw_schema}.sfdc_kpi_{object_name} ({columns_sql}) "\
                             f"SELECT DISTINCT {columns_sql} FROM {db.stage_schema}.stg_sfdc_kpi_{object_name}"
            cur.execute(insert_to_prod)
            con.commit()

    con.close()

    print('CDC successful!')


def main() -> None:
    """
    Get salesforce objects data, create dataframes,
    put dataframes to the S3 and renew the database.
    """
    path_to_file, object_name = get_args()

    db, salesforce_connection, s3_connection = initialize_connection_instances(
        path_to_file=path_to_file
    )

    with open(path_to_file, 'r') as columns:
        all_object_columns = json.load(columns)

    main_columns, transposed_columns = get_main_and_transposed_columns(
        object_columns=all_object_columns[object_name]
    )

    last_updated_time = get_last_updated_time(db=db, object_name=object_name)
    df = get_salesforce_data(
        object_name=object_name,
        columns=main_columns,
        field_columns=transposed_columns,
        last_updated_date=last_updated_time,
        sf_connection=salesforce_connection
    )
    if 'Field' in df.columns:
        df = transpose_columns(df=df)
    df = postprocess_dataframe(df=df)
    df = add_md5_column(df=df)
    s3_df = get_data_from_s3(object_name=object_name, s3=s3_connection)
    df = modify_s3_dataframe(
        s3_df=s3_df,
        df_new_data=df,
        last_update_date=last_updated_time
    )
    put_to_s3(
        df=df,
        object_name=object_name,
        s3=s3_connection
    )
    all_columns_list = df.columns.tolist()
    change_data_capture(
        db=db,
        object_name=object_name,
        columns=all_columns_list,
        s3=s3_connection
    )


if __name__ == '__main__':
    main()
