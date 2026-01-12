from __future__ import annotations
import argparse
import json
import logging
import os
import ssl
import pandas as pd
from time import sleep
from datetime import datetime, timedelta
import requests
import yaml

from data_utils import (
    CredentialsHelper,
    DatabaseHelper,
    DBTHelper,
    S3BucketHelper,
    SimpleLogger,
)

# Constants
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPT_NAME = os.path.basename(__file__).split(".")[0]

# Global instances
logger: logging = None
s3_helper: S3BucketHelper = None
db_helper: DatabaseHelper = None
dbt_helper: DBTHelper = None
credentials_helper: CredentialsHelper = None
etl_processor: ETLProcessor = None

# Usage for full load:
# poetry run python .\zip\zip_extract.py -o purchase_orders --start_at 0
# This script is used if we need to run a full load.


def parse_arguments():
    parser = argparse.ArgumentParser(description="ETL script for Zip agreements.")
    parser.add_argument(
        "-cp", "--config_path", type=str, help="Path to the configuration file."
    )
    parser.add_argument(
        "-o", "--object", type=str, required=True, help="Object to be loaded."
    )
    parser.add_argument(
        "-l",
        "--log_level",
        type=str,
        choices=["INFO", "DEBUG", "ERROR"],
        default="INFO",
    )
    parser.add_argument("-t", "--target", type=str, help="Database to target.")
    parser.add_argument(
        "--start_at", type=str, help="Load from start timestamp epoch value."
    )
    parser.add_argument("-c", "--additional_config", type=str, default="{}")
    args = parser.parse_args()

    config_path = args.config_path or os.path.join(BASE_DIR, "../config.yaml")
    additional_config = json.loads(args.additional_config)

    return (
        config_path,
        args.object,
        args.log_level,
        args.target,
        args.start_at,
        additional_config,
    )


def initialize():
    config_path, object_name, log_level, target, start_at, additional_config = (
        parse_arguments()
    )
    global logger, s3_helper, db_helper, dbt_helper, credentials_helper, etl_processor

    logger = SimpleLogger(
        log_file=os.path.join(BASE_DIR, f"../logs/{SCRIPT_NAME}_{object_name}.log"),
        level=log_level,
    ).get_logger()

    try:
        with open(config_path, "r") as yaml_file:
            yaml_content = yaml.safe_load(yaml_file)
            if SCRIPT_NAME not in yaml_content:
                logger.error("SCRIPT_NAME '%s' not found in YAML config.", SCRIPT_NAME)
                raise RuntimeError(
                    f"SCRIPT_NAME '{SCRIPT_NAME}' not found in YAML config."
                )
            raw_config = yaml_content[SCRIPT_NAME]

        global_config = raw_config.get("global", {})
        table_config = raw_config.get(object_name, {})

        # Merge global first, then override with table-specific params
        config_data = {**global_config, **table_config}
        logger.debug("Loaded configuration from %s", config_path)
    except (yaml.YAMLError, OSError) as error:
        logger.error("Error reading the YAML file: %s", error)
        raise RuntimeError("Failed to load YAML config file") from error

    config_data.update(additional_config)
    if target:
        config_data["target"] = target
    if start_at:
        config_data["start_at"] = start_at

    # Initialize helpers
    s3_helper = S3BucketHelper(
        logger=logger, **(config_data | config_data.get("s3", {}))
    )
    db_helper = DatabaseHelper(
        logger=logger, **(config_data | config_data.get("database", {}))
    )
    dbt_helper = DBTHelper(logger=logger, **(config_data | config_data.get("dbt", {})))
    credentials_helper = CredentialsHelper(logger=logger, **config_data)

    etl_processor = ETLProcessor(object_name=object_name, **config_data)


class ETLProcessor:
    S3_PATH_TEMPLATE = "{application}/{object_name}/{table_name}/{date}.csv.gz"

    def __init__(self, object_name: str, **config):
        self.object_name = object_name
        self.application = config["application"]
        self.stage_schema = config["stage_schema"]
        self.bidw_schema = config["bidw_schema"]
        self.stage_table = f"{config['stage_table_format']}{self.object_name}"
        self.bidw_table = f"{config['bidw_table_format']}{self.object_name}"
        self.api_url = config["api_url"]
        self.date_columns = config.get("date_columns", [])
        self.start_at = config.get("start_at")
        self._api_key = credentials_helper.get_credential("api_key")
        self.use_pagination = config.get("use_pagination", False)

        # --- Mapping files ---
        self.mapping_file = config.get("mapping_file") or os.path.join(
            BASE_DIR, f"../mapping_files/{self.bidw_table}.csv"
        )
        self.line_mapping_file = config.get("line_mapping_file") or os.path.join(
            BASE_DIR, f"../mapping_files/{self.bidw_table}_line_items.csv"
        )
        self.inv_mapping_file = config.get("invoices_mapping_file") or os.path.join(
            BASE_DIR, f"../mapping_files/{self.bidw_table}_invoices.csv"
        )
        self.contacts_mapping_file = config.get(
            "contacts_mapping_file"
        ) or os.path.join(BASE_DIR, f"../mapping_files/{self.bidw_table}_contacts.csv")
        self.subsidiaries_mapping_file = config.get(
            "subsidiaries_mapping_file"
        ) or os.path.join(
            BASE_DIR, f"../mapping_files/{self.bidw_table}_subsidiaries.csv"
        )
        self.categories_mapping_file = config.get(
            "categories_mapping_file"
        ) or os.path.join(
            BASE_DIR, f"../mapping_files/{self.bidw_table}_categories.csv"
        )

        # --- Load mappings ---
        self.column_mapping = self._load_mapping(self.mapping_file)
        self.child_mappings = {
            "line_items": (self._load_mapping(self.line_mapping_file), "_line_items"),
            "invoices": (self._load_mapping(self.inv_mapping_file), "_invoices"),
            "contacts": (self._load_mapping(self.contacts_mapping_file), "_contacts"),
            "subsidiaries": (
                self._load_mapping(self.subsidiaries_mapping_file),
                "_subsidiaries",
            ),
            "categories": (
                self._load_mapping(self.categories_mapping_file),
                "_categories",
            ),
        }

    @staticmethod
    def _load_mapping(file_path: str):
        if os.path.exists(file_path):
            return dict(pd.read_csv(file_path)[["Source", "Target"]].values)
        return {}

    def extract_child_records(self, item, child_key, mapping, parent_id):
        records = []
        parent_updated_at = item.get("updated_at", "")
        for child in item.get(child_key, []):
            rec = {
                target: self.extract_nested_value(
                    child, source.replace(f"{child_key}.", "")
                )
                for source, target in mapping.items()
                if source.startswith(f"{child_key}.")
            }
            rec["parent_id"] = parent_id
            rec["updated_at"] = parent_updated_at
            records.append(rec)

        return records

    def upsert_run_config(self, value):
        insert_sql = f"""
            INSERT INTO {self.stage_schema}.zip_run_config (object_name, last_run_value, last_run_timestamp)
            VALUES (%s, %s, CURRENT_TIMESTAMP)
        """
        update_sql = f"""
            UPDATE {self.stage_schema}.zip_run_config
            SET last_run_value = %s, last_run_timestamp = CURRENT_TIMESTAMP
            WHERE object_name = %s
        """
        with db_helper.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT 1 FROM {self.stage_schema}.zip_run_config WHERE object_name=%s",
                    (self.object_name,),
                )
                exists = cursor.fetchone()
                if exists:
                    cursor.execute(update_sql, (value, self.object_name))
                else:
                    cursor.execute(insert_sql, (self.object_name, value))
            connection.commit()

    def extract_nested_value(self, data: dict | list, key: str) -> str:
        """Recursively extract nested values using dot notation from dict or list."""
        try:
            keys = key.split(".")

            def recurse(value, keys):
                if not keys:
                    return value
                current_key, remaining_keys = keys[0], keys[1:]

                if isinstance(value, list):
                    result = [recurse(item, keys) for item in value]
                    flattened = []
                    for r in result:
                        if isinstance(r, list):
                            flattened.extend(r)
                        elif r != "":
                            flattened.append(r)
                    return flattened
                elif isinstance(value, dict):
                    return recurse(value.get(current_key, ""), remaining_keys)
                return ""

            result = recurse(data, keys)
            if isinstance(result, list):
                return ", ".join(str(r) for r in result if r != "")
            return str(result) if result else ""
        except Exception:
            return ""

    def extract_data(self):
        headers = {"Zip-Api-Key": self._api_key, "Accept": "application/json"}
        params = {}
        last_updated_at, last_next_page_token = 0, None

        # Start_at override (historical load)
        if self.start_at:
            logger.info(
                "Using start_at override (%s) for object %s",
                self.start_at,
                self.object_name,
            )
            if self.object_name in ["purchase_orders", "vendors"]:
                params["last_updated_after"] = int(self.start_at)
            else:
                params["page_token"] = self.start_at
        else:
            # Fetch last run from DB
            config_query = f"SELECT last_run_value FROM {self.stage_schema}.zip_run_config WHERE object_name = %s"
            with db_helper.get_connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(config_query, (self.object_name,))
                    result = cursor.fetchone()
                    if result and result[0]:
                        if self.object_name in ["purchase_orders", "vendors"]:
                            last_updated_at = int(result[0])
                            params["last_updated_after"] = last_updated_at
                        else:
                            last_next_page_token = result[0]
                            params["page_token"] = last_next_page_token

        params["page_size"] = 100
        all_records, child_dfs = [], {k: [] for k in self.child_mappings}
        next_page_token, last_valid_token = last_next_page_token, None

        while True:
            if self.use_pagination and next_page_token:
                params["page_token"] = next_page_token
                last_valid_token = next_page_token

            response = requests.get(self.api_url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            if "list" not in data:
                logger.warning(
                    "No 'list' key found in API response for %s", self.object_name
                )
                break

            for item in data["list"]:
                record = {
                    tgt: self.extract_nested_value(item, src)
                    for src, tgt in self.column_mapping.items()
                }
                all_records.append(record)
                fk_id = record.get("id")

                # Process all children
                for child_key, (mapping, _) in self.child_mappings.items():
                    child_dfs[child_key].extend(
                        self.extract_child_records(item, child_key, mapping, fk_id)
                    )

            next_page_token = data.get("next_page_token")
            if not next_page_token:
                logger.info("Pagination completed — no more pages.")
                break
            else:
                logger.info(
                    "Fetched %d IDs from page, next_page_token=%s",
                    len(data["list"]),
                    data.get("next_page_token"),
                )

        # Update run config
        if self.object_name in ["purchase_orders", "vendors"] and all_records:
            new_max_updated = max(
            int(r.get("updated_at", 0)) for r in all_records if r.get("updated_at")
            )
            # Subtract 6 hours (in seconds) from the max updated_at
            new_max_updated = new_max_updated - 21600
            self.upsert_run_config(new_max_updated)
        elif (
            self.object_name not in ["purchase_orders", "vendors"]
            and last_valid_token
            and all_records
        ):
            self.upsert_run_config(last_valid_token)

        return pd.DataFrame(all_records), {
            k: pd.DataFrame(v) for k, v in child_dfs.items()
        }

    def transform_data(
        self, data_frame: pd.DataFrame, is_parent: bool = False
    ) -> pd.DataFrame:
        data_frame.fillna("", inplace=True)
        data_frame = data_frame.astype(str)

        if is_parent and self.object_name in ["purchase_orders"]:
            # List of parent-specific numeric/string columns to ensure "0" default
            for col in ["status", "source"]:
                if col in data_frame.columns:
                    # Convert to numeric if possible, fill NaN/empty with 0, then back to string
                    data_frame[col] = (
                        pd.to_numeric(data_frame[col], errors="coerce")
                        .fillna(0)
                        .astype(int)
                        .astype(str)
                    )

        def convert_epoch_to_date(epoch_str):
            try:
                epoch = float(epoch_str)
                if epoch > 1e10:
                    epoch = epoch / 1000
                return datetime.utcfromtimestamp(epoch).date()
            except ValueError:
                return epoch_str

        def convert_epoch_to_timestamp(epoch_str):
            try:
                epoch = float(epoch_str)
                if epoch > 1e10:  # handle milliseconds
                    epoch = epoch / 1000
                return datetime.utcfromtimestamp(epoch).strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                return epoch_str

        for column in self.date_columns:
            if column in data_frame.columns:
                if column in ["updated_at"]:
                    data_frame[column] = data_frame[column].apply(convert_epoch_to_timestamp)
                else:
                    data_frame[column] = data_frame[column].apply(convert_epoch_to_date)

        return data_frame

    def load_data(
        self,
        data_frame: pd.DataFrame,
        is_line_item: bool = False,
        line_keyword: str = "",
    ):
        """Load data into the staging table and then into the BIDW table."""
        logger.info("Starting data load process for application: %s", self.application)
        # Derive table names without mutating instance variables
        stage_table = (
            self.stage_table + line_keyword if is_line_item else self.stage_table
        )
        bidw_table = self.bidw_table + line_keyword if is_line_item else self.bidw_table

        try:
            # Generate S3 path
            s3_path = self.S3_PATH_TEMPLATE.format(
                application=self.application,
                object_name=self.object_name,
                table_name=stage_table,
                date=datetime.now().isoformat(),
            )

            # Upload to S3
            logger.info("Uploading data to S3 at path: %s", s3_path)
            s3_helper.put_to_bucket(df=data_frame, key=s3_path)

            # Prepare COPY command
            copy_command = s3_helper.copy_from_s3_command(
                target_table=f"{self.stage_schema}.{stage_table}",
                s3_path=s3_path,
                columns=data_frame.columns.tolist(),
            )

            # Load into staging table
            logger.info(
                "Loading data into staging table: %s.%s", self.stage_schema, stage_table
            )
            with db_helper as connection:
                with connection.cursor() as cursor:
                    cursor.execute(f"TRUNCATE TABLE {self.stage_schema}.{stage_table};")
                    cursor.execute(copy_command)

            # Run DBT snapshot and scan logs
            logger.info("Running DBT snapshot for table: %s", bidw_table)
            dbt_helper.execute_model("snapshot", bidw_table, "intapp_dbt")
            dbt_helper.scan_log_for_error(bidw_table, "intapp_dbt")

            logger.info("Data loaded successfully to %s.", bidw_table)

        except Exception as e:
            logger.error("Data load failed: %s", str(e), exc_info=True)
            raise


def process_and_load(df: pd.DataFrame, keyword: str, is_parent: bool = False):
    if df is not None and not df.empty:
        df = etl_processor.transform_data(df, is_parent=is_parent)
        etl_processor.load_data(df, is_line_item=not is_parent, line_keyword=keyword)


def main():
    initialize()

    try:
        df_parent, child_dfs = etl_processor.extract_data()
    except Exception as e:
        logger.error("Extraction failed: %s", e, exc_info=True)
        return

    # Parent load
    try:
        process_and_load(df_parent, "", is_parent=True)
    except Exception as e:
        logger.error("Parent processing failed: %s", e, exc_info=True)

    # Child tables load
    for child_key, df in child_dfs.items():
        _, keyword = etl_processor.child_mappings[child_key]
        try:
            process_and_load(df, keyword)
        except Exception as e:
            logger.error(
                "%s processing failed: %s", child_key.capitalize(), e, exc_info=True
            )

    logger.info("Script executed successfully for %s.", SCRIPT_NAME)


if __name__ == "__main__":
    main()
