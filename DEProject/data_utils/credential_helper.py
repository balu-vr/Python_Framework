import logging
import os
from typing import Dict, Optional

import yaml


class CredentialsHelper:
    """
    A class to manage the reading of credentials from a YAML file specific to an application.
    The credentials are stored in the `.creds/{application}/{environment}.yml` file by default, or in a
    user-specified file if `config_path` is provided. If `config_path` is provided, `application` and `environment`
    are ignored.
    """

    def __init__(
        self,
        application: Optional[str] = None,
        environment: Optional[str] = None,
        config_path: Optional[str] = None,
        dbt_profile_config_path: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
        **kwargs,
    ):
        """
        Initialize the CredentialsHelper with the application name, environment, and optional config path.
        If `config_path` is provided, it overrides `application` and `environment`.
        """
        self.logger = logger or logging.getLogger(__name__)

        # Set the configuration path that follows .dbt/profile.yml structure, defaulting to '.dbt/profile.yml' if not provided
        self.dbt_profile_config_path = dbt_profile_config_path or os.path.expanduser(
            "~/.dbt/profiles.yml"
        )
        # If config_path is provided, use it directly. Otherwise, construct the default path.
        if config_path:
            self.config_path = config_path
            self.logger.info(f"Using custom config path: {self.config_path}")
        else:
            environment = environment or self._get_environment_from_profiles_yaml()
            if not application:
                raise ValueError(
                    "'application' must be provided if 'config_path' is not specified."
                )
            self.config_path = os.path.expanduser(
                f"~/.creds/{application}/{environment}.yml"
            )
            self.logger.info(
                f"Using default config path: {self.config_path} for application '{application}' and environment '{environment}'"
            )

        # Load credentials from the YAML file
        self._creds = self._load_credentials_from_yaml()

    def _get_environment_from_profiles_yaml(self) -> Dict[str, str]:
        """
        Load environment from the .dbt/profiles file.
        """
        try:
            with open(self.dbt_profile_config_path, "r") as yaml_file:
                environment = yaml.safe_load(yaml_file)["credentials_helper"][
                    "environment"
                ]
        except (yaml.YAMLError, KeyError) as error:
            self.logger.error(
                "Error reading the YAML file or missing 'environment' key: %s", error
            )
            raise error
        return environment

    def _load_credentials_from_yaml(self) -> Dict[str, str]:
        """
        Load and validate credentials from the YAML file.
        """
        try:
            self.logger.info(f"Loading credentials from {self.config_path}")
            with open(self.config_path, "r") as yaml_file:
                creds = yaml.safe_load(yaml_file)
        except (yaml.YAMLError, FileNotFoundError) as error:
            self.logger.error(
                "Error reading the YAML file or file not found: %s", error
            )
            raise error

        return creds

    def get_credential(self, key: str) -> str:
        """
        Retrieve the credential value for a given key.
        """
        if key not in self._creds:
            raise KeyError(f"Key '{key}' not found in credentials")
        return self._creds[key]
