import json
import logging
import os
from typing import Iterable, Optional, Union
import subprocess

from airflow.models import Connection
from airflow.operators.bash import BashOperator

from edu_edfi_airflow.callables import airflow_util


class EarthmoverOperator(BashOperator):
    """

    """
    template_fields = ('output_dir', 'state_file', 'arguments', 'bash_command', 'env',)

    def __init__(self,
        *,
        earthmover_path: Optional[str] = "earthmover",

        output_dir : Optional[str] = None,
        state_file : Optional[str] = None,
        config_file: Optional[str] = None,
        selector   : Optional[Union[str, Iterable[str]]] = None,
        parameters : Optional[Union[str, dict]] = None,
        results_file: Optional[str] = None,
        snowflake_read_conn_id: Optional[str] = None,

        force          : bool = False,
        skip_hashing   : bool = False,
        show_graph     : bool = False,
        show_stacktrace: bool = False,
        return_exit_code: bool = False,

        **kwargs
    ):
        self.earthmover_path = earthmover_path
        self.output_dir = output_dir
        self.state_file = state_file
        self.snowflake_read_conn_id = snowflake_read_conn_id
        self.return_exit_code = return_exit_code

        ### Building the Earthmover CLI command
        self.arguments = {}

        # Dynamic arguments
        if config_file:
            self.arguments['--config-file'] = config_file

        if selector:  # Pre-built selector string or list of node names
            if not isinstance(selector, str):
                selector = ",".join(selector)
            self.arguments['--selector'] = selector

        if parameters:  # JSON string or dictionary
            if isinstance(parameters, str):
                parameters = json.loads(parameters)
            self.arguments['--params'] = {key: str(val) for key, val in parameters.items()}  # Force all params to strings.

        if results_file:
            self.arguments['--results-file'] = results_file

        # Boolean arguments
        if force:
            self.arguments['--force'] = ""
        if skip_hashing:
            self.arguments['--skip-hashing'] = ""
        if show_graph:
            self.arguments['--show-graph'] = ""
        if show_stacktrace:
            self.arguments['--show-stacktrace'] = ""

        ### Environment variables
        # Pass required `output_dir` parameter as environment variables
        env_vars = {
            'OUTPUT_DIR': self.output_dir,
            'STATE_FILE': self.state_file,
        }

        bash_command_prefix = f"{self.earthmover_path} run "
        super().__init__(bash_command=bash_command_prefix, env=env_vars, append_env=True, do_xcom_push=True, **kwargs)

    def execute(self, conf=None, **context) -> str:
        """

        :param context:
        :return:
        """
        # Construct a Snowflake connection string and add as an environment variable. Currently only Snowflake is supported
        # (This must be done in execute to prevent extraction during DAG-parsing)
        if self.snowflake_read_conn_id:
            db_conn = Connection.get_connection_from_secrets(self.snowflake_read_conn_id)
            database_conn_string = f"snowflake://{db_conn.login}:{db_conn.password}@{db_conn.extra_dejson['extra__snowflake__account']}"
            self.env['SNOWFLAKE_CONNECTION'] = database_conn_string

        # Format values before adding to the bash command.
        cli_arguments = []
        for key, val in self.arguments.items():
            if not val:
                cli_arguments.append(key)
            elif isinstance(val, dict):
                cli_arguments.append(f"{key} '{json.dumps(val)}'")
            else:
                cli_arguments.append(f"{key} '{val}'")
        
        self.bash_command += " ".join(cli_arguments)
        logging.info(f"Complete Earthmover CLI command: {self.bash_command}")

        # Create state_dir if not already defined in filespace
        os.makedirs(os.path.dirname(self.state_file), exist_ok=True)

        # If return_exit_code is enabled, the command always succeeds and the exit code is returned.
        if self.return_exit_code:
            self.bash_command += "; echo $?"
            exit_code = super().execute(context)
            return self.output_dir, int(exit_code)
        else:
            super().execute(context)
            return self.output_dir



class LightbeamOperator(BashOperator):
    """

    """
    template_fields = ('data_dir', 'state_dir', 'arguments', 'bash_command', 'env',)
    valid_commands = ('validate', 'send', 'validate+send')

    def __init__(self,
        *,
        lightbeam_path: Optional[str] = "lightbeam",
        command: str = 'send',

        data_dir: Optional[str] = None,
        state_dir: Optional[str] = None,

        edfi_conn_id: Optional[str] = None,

        config_file: Optional[str] = None,
        selector: Optional[Union[str, Iterable[str]]] = None,
        parameters: Optional[Union[str, dict]] = None,
        results_file: Optional[str] = None,

        wipe: bool = False,
        force: bool = False,

        older_than: Optional[str] = None,
        newer_than: Optional[str] = None,
        resend_status_codes: Optional[Union[str, Iterable[str]]] = None,

        **kwargs
    ):
        self.lightbeam_path = lightbeam_path
        self.data_dir = data_dir
        self.state_dir = state_dir
        self.edfi_conn_id = edfi_conn_id

        # Verify command argument is valid
        if command not in self.valid_commands:
            raise ValueError(
                f"LightbeamOperator command type `{command}` is undefined!"
            )

        ### Building the Lightbeam CLI command
        self.arguments = {}

        # Dynamic arguments
        if config_file:
            self.arguments['--config-file'] = config_file

        if selector:  # Pre-built selector string or list of node names
            if not isinstance(selector, str):
                selector = ",".join(selector)
            self.arguments['--selector'] = selector

        if parameters:  # JSON string or dictionary
            if isinstance(parameters, str):
                parameters = json.loads(parameters)
            self.arguments['--params'] = {key: str(val) for key, val in parameters.items()}  # Force all params to strings.

        if results_file:
            self.arguments['--results-file'] = results_file

        if resend_status_codes:
            if not isinstance(resend_status_codes, str):
                resend_status_codes = ",".join(resend_status_codes)
            self.arguments['--resend-status-codes'] = resend_status_codes

        # Boolean arguments
        if wipe:
            self.arguments['--wipe'] = ""
        if force:
            self.arguments['--force'] = ""

        # Optional string arguments
        if older_than:
            self.arguments['--older-than'] = older_than
        if newer_than:
            self.arguments['--newer-than'] = newer_than

        ### Environment variables
        # Pass required `data_dir`
        env_vars = {
            'DATA_DIR' : self.data_dir,
            'STATE_DIR': self.state_dir,
        }

        bash_command_prefix = f"{self.lightbeam_path} {command} "
        super().__init__(bash_command=bash_command_prefix, env=env_vars, append_env=True, **kwargs)

    def execute(self, conf=None, **context) -> str:
        """

        :param context:
        :return:
        """
        #  Extract EdFi connection parameters as environment variables if defined
        # (This must be done in execute to prevent extraction during DAG-parsing.
        if self.edfi_conn_id:
            edfi_conn = Connection.get_connection_from_secrets(self.edfi_conn_id)

            self.env['EDFI_API_BASE_URL'] = edfi_conn.host
            self.env['EDFI_API_CLIENT_ID'] = edfi_conn.login
            self.env['EDFI_API_CLIENT_SECRET'] = edfi_conn.password

            _api_year = edfi_conn.extra_dejson.get('api_year')
            if _api_year:
                self.env['EDFI_API_YEAR'] = _api_year

            _api_version = edfi_conn.extra_dejson.get('api_version')
            if _api_version:
                self.env['EDFI_API_VERSION'] = _api_version

            _api_mode = edfi_conn.extra_dejson.get('api_mode')
            if _api_mode:
                self.env['EDFI_API_MODE'] = _api_mode

        # Overwrite `force` if defined in DAG configs.
        if airflow_util.get_context_variable(context, 'force', default=False):
            logging.info("Parameter `force` provided in context will overwrite defined operator argument.")
            self.arguments['--force'] = ""

        # Create state_dir if not already defined in filespace
        os.makedirs(self.state_dir, exist_ok=True)

        # Format values before adding to the bash command.
        cli_arguments = []
        for key, val in self.arguments.items():
            if not val:
                cli_arguments.append(key)
            elif isinstance(val, dict):
                cli_arguments.append(f"{key} '{json.dumps(val)}'")
            else:
                cli_arguments.append(f"{key} '{val}'")
        
        self.bash_command += " ".join(cli_arguments)
        logging.info(f"Complete Lightbeam CLI command: {self.bash_command}")

        super().execute(context)
        return self.data_dir