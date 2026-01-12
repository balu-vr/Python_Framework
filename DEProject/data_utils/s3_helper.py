"""
S3 Bucket Helper Module

This module provides the `S3BucketHelper` class, which is used to manage connections 
to an Amazon S3 bucket. The class handles loading credentials from .dbt/profiles.yml 
file, establishing connections using the `boto3` library, and optionally managing 
connections within a context manager.
"""

import logging
import os
import textwrap
from io import BytesIO
from typing import Dict, Optional

import boto3
import pandas as pd
import yaml

# Constants for the required keys in the configuration
S3_BUCKET_REQUIRED_KEYS = {
    "access_key_id",
    "secret_access_key",
    "region",
    "bucket_name",
}


class S3BucketHelper:
    """
    A class to manage the connection and interaction with an S3 bucket.
    """

    def __init__(
        self,
        config_path: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
        bucket_name: Optional[str] = None,
        **kwargs,
    ):
        """
        Initialize the S3BucketHelper with credentials from a YAML dbt profile.yml file.
        """
        self.connection = None
        self.logger = logger or logging.getLogger(__name__)
        self.config_path = config_path or os.path.expanduser("~/.dbt/profiles.yml")
        self._creds = self._get_s3_bucket_credential_from_yaml()

        self.region = self._creds["region"]
        self._access_key_id = self._creds["access_key_id"]
        self._secret_access_key = self._creds["secret_access_key"]
        self.bucket_name = bucket_name or self._creds["bucket_name"]

    def __del__(self) -> None:
        """
        Destructor to ensure any necessary cleanup when the object is destroyed.
        """
        self.close_connection()

    def _get_s3_bucket_credential_from_yaml(self) -> Dict[str, str]:
        """
        Load and validate S3 bucket credentials from the YAML file.
        """
        try:
            with open(self.config_path, "r") as yaml_file:
                s3_bucket_config = yaml.safe_load(yaml_file)["S3_bucket"]
        except (yaml.YAMLError, KeyError) as error:
            self.logger.error(
                "Error reading the YAML file or missing 'S3_bucket' key: %s", error
            )
            raise error

        missing_keys = S3_BUCKET_REQUIRED_KEYS - s3_bucket_config.keys()
        if missing_keys:
            raise KeyError(f"Missing keys {missing_keys} from the S3_bucket key")

        return s3_bucket_config

    def get_connection(self) -> boto3.client:
        """
        Establish a connection to the S3 bucket using boto3 and return the client.
        """
        if self.connection is None:
            try:
                self.connection = boto3.client(
                    service_name="s3",
                    region_name=self.region,
                    aws_access_key_id=self._access_key_id,
                    aws_secret_access_key=self._secret_access_key,
                )
                self.logger.info("Successfully established S3 connection.")
            except boto3.exceptions.Boto3Error as e:
                self.logger.error("Failed to connect to the S3 bucket: %s", e)
                raise e
        return self.connection

    def close_connection(self) -> None:
        """
        Close the S3 connection.

        Note: boto3 client doesn't require explicit closure, but this method is here
        for completeness and potential future use.
        """
        if self.connection:
            self.logger.debug("S3 connection would be closed here if necessary.")
            self.connection = None

    def __enter__(self) -> boto3.client:
        """
        Context manager entry, returns the S3 client connection.
        """
        return self.get_connection()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Context manager exit, ensures the connection is closed.
        """
        self.close_connection()

    def put_to_bucket(
        self, key: str, df: pd.DataFrame, bucket_name: Optional[str] = None
    ) -> None:
        """
        Upload a DataFrame to an S3 bucket as a CSV file.
        """
        bucket_name = bucket_name or self.bucket_name
        conn = self.get_connection()
        buffer = BytesIO()
        df.to_csv(buffer, index=False, compression="gzip", sep=";")
        buffer.seek(0)
        try:
            conn.put_object(Bucket=bucket_name, Key=key, Body=buffer.getvalue())
            self.logger.info("Successfully uploaded DataFrame to S3 with key: %s", key)
        except boto3.exceptions.Boto3Error as e:
            self.logger.error("Failed to upload DataFrame to S3: %s", e)
            raise e

    def copy_from_s3_command(
        self,
        target_table: str,
        s3_path: str,
        columns: list[str],
        bucket_name: Optional[str] = None,
        additional_options: Optional[str] = None,
    ) -> str:
        """
        This method is used to copy s3 file to redshift.
        """

        if not isinstance(columns, (list, tuple)) or not all(
            isinstance(col, str) for col in columns
        ):
            raise ValueError("Columns must be a list or tuple of strings.")

        columns_sql = '"' + '", "'.join(columns) + '"'
        bucket_name = bucket_name or self.bucket_name

        # Construct the base COPY command
        copy_command = textwrap.dedent(
            f"""
            COPY {target_table} ({columns_sql})
            FROM 's3://{bucket_name}/{s3_path}'
            CREDENTIALS 'aws_access_key_id={self._access_key_id};aws_secret_access_key={self._secret_access_key}'
            DELIMITER ';'
            CSV
            GZIP
            TIMEFORMAT AS 'auto'
            COMPUPDATE OFF
            STATUPDATE OFF 
            EMPTYASNULL
            BLANKSASNULL
            TRIMBLANKS
            IGNOREHEADER AS 1
            IGNOREBLANKLINES
            ACCEPTINVCHARS AS '^'
        """
        )

        # Append any additional options if provided
        if additional_options:
            copy_command += f" {additional_options}\n"

        # Final command
        copy_command += ";"

        # Optional: Log the copy command without credentials
        self.logger.debug("Generated COPY command for table %s.", target_table)

        return copy_command
