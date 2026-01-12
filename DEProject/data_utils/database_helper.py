"""
Database Helper Module

This module provides the `Database` class, which is used to manage connections to a  database.
It facilitates the loading of database credentials from .dbt/profile.yml, establishing
and closing connections, and supports the use of context managers for safe and efficient
connection handling.
"""

import logging
import os
from typing import Dict, Optional

import psycopg2
import yaml

# Constants for the required keys in the configuration
DATABASE_REQUIRED_KEYS = {"host", "port", "dbname", "user", "password"}


class DatabaseHelper:
    """
    A class to manage the connection and interaction with a database.

    The `DatabaseHelper` class handles the loading of database credentials from a YAML
    configuration file, establishing and closing connections, and managing
    connections within a context manager.
    """

    def __init__(
        self,
        config_path: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
        dwh_profile: Optional[str] = "intapp_dbt",
        target: Optional[str] = None,
        **kwargs,
    ):
        """
        Initialize the DatabaseHelper class with the target database configuration.
        """
        self.logger = logger or logging.getLogger(__name__)
        self._connection = None

        # Set the configuration path that follows .dbt/profile.yml structure, defaulting to '.dbt/profile.yml' if not provided
        self.config_path = config_path or os.path.expanduser("~/.dbt/profiles.yml")

        # Load the database credentials
        self._creds = self._load_database_credentials(dwh_profile, target)

        self.host = self._creds["host"]
        self.port = self._creds["port"]
        self.dbname = self._creds["dbname"]
        self._user = self._creds["user"]
        self._password = self._creds["password"]

        self.logger.info(
            "DatabaseHelper instance initialized for %s target: %s",
            dwh_profile,
            self.target,
        )

    def __del__(self) -> None:
        """
        Destructor to ensure the database connection is closed when the object is destroyed.
        """
        self.logger.debug(
            "Destructor called. Ensuring the database connection is closed."
        )
        self.close_connection()

    def _load_database_credentials(
        self, dwh_profile: str, target: Optional[str]
    ) -> Dict[str, str]:
        """
        Load and validate database credentials from the YAML file.
        """
        try:
            with open(self.config_path, "r") as yaml_file:
                config = yaml.safe_load(yaml_file)
            self.logger.debug("Loaded database configuration from %s", self.config_path)
        except (yaml.YAMLError, OSError) as error:
            raise RuntimeError(f"Failed to load YAML configuration file") from error

        # Check if the DWH_PROFILE key is present
        try:
            databases_config = config[dwh_profile]
        except KeyError as error:
            raise KeyError(
                f"Configuration YAML is missing '{dwh_profile}' key"
            ) from error

        # Determine the target database
        self.target = target or databases_config.get("target")
        if not self.target or self.target not in databases_config.get("outputs", {}):
            raise KeyError(
                f"Target database '{self.target}' not present in 'outputs' key"
            )

        # Load and validate credentials for the target database
        creds = databases_config["outputs"][self.target]
        missing_keys = DATABASE_REQUIRED_KEYS - creds.keys()
        if missing_keys:
            raise KeyError(
                f"Missing keys {missing_keys} from the connection '{self.target}'"
            )

        self.logger.debug(
            "Database credentials for '%s' loaded successfully", self.target
        )
        return creds

    def get_connection(self) -> psycopg2.extensions.connection:
        """
        Establish a connection to the database if not already connected.
        """
        if self._connection is None:
            try:
                self._connection = psycopg2.connect(
                    dbname=self.dbname,
                    host=self.host,
                    port=self.port,
                    user=self._user,
                    password=self._password,
                )
                self.logger.info("%s Database connection established.", self.dbname)
            except psycopg2.DatabaseError as error:
                self.logger.error("Failed to connect to the database: %s", error)
                raise RuntimeError("Database connection failed") from error
        else:
            self.logger.debug("Returning existing database connection.")
        return self._connection

    def close_connection(self) -> None:
        """
        Close the database connection if it exists.
        """
        if self._connection:
            try:
                self._connection.close()
                self.logger.info("Database connection closed.")
            except psycopg2.DatabaseError as error:
                raise RuntimeError("Failed to close the database connection") from error
            finally:
                self._connection = None
                self.logger.debug("Database connection set to None.")

    def __enter__(self) -> psycopg2.extensions.connection:
        """
        Context manager entry, returns the database connection.
        """
        return self.get_connection()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Context manager exit, ensures the connection is closed.
        """
        self.close_connection()
