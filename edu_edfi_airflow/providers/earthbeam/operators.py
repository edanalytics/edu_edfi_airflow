import json
import logging
from typing import Iterable, Optional, Union

from airflow.models import Connection
from airflow.operators.bash import BashOperator

from edu_edfi_airflow.dags.dag_util.airflow_util import get_context_parameter


class EarthmoverOperator(BashOperator):
    """

    """
    template_fields = ('output_dir', 'state_file', 'arguments',)

    def __init__(self,
        *,
        earthmover_path: Optional[str] = None,

        output_dir : Optional[str] = None,
        state_file : Optional[str] = None,
        config_file: Optional[str] = None,
        selector   : Optional[Union[str, Iterable[str]]] = None,
        parameters : Optional[Union[str, dict]] = None,

        force          : bool = False,
        skip_hashing   : bool = False,
        show_graph     : bool = False,
        show_stacktrace: bool = False,

        **kwargs
    ):
        self.earthmover_path = earthmover_path or 'earthmover'
        self.output_dir = output_dir
        self.state_file = state_file

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
            if not isinstance(parameters, str):
                parameters = json.dumps(parameters)
            self.arguments['--params'] = f"'{parameters}'"  # Force double-quotes around JSON keys

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
        super().__init__(bash_command=bash_command_prefix, env=env_vars, **kwargs)


    def execute(self, context) -> str:
        """

        :param context:
        :return:
        """
        # Update final Earthmover command with any passed arguments
        # This update occurs here instead of init to allow context parameters to be passed.
        self.bash_command += " ".join(f"{kk} {vv}" for kk, vv in self.arguments.items())
        self.bash_command = self.bash_command.replace("{", "'{").replace("}", "}'")  # Force single-quotes around params
        logging.info(f"Complete Earthmover CLI command: {self.bash_command}")

        super().execute(context)
        return self.output_dir



class LightbeamOperator(BashOperator):
    """

    """
    template_fields = ('data_dir', 'state_dir', 'arguments',)
    valid_commands = ('validate', 'send', 'validate+send')

    def __init__(self,
        *,
        lightbeam_path: Optional[str] = None,
        command: str = 'send',

        data_dir: Optional[str] = None,
        state_dir: Optional[str] = None,

        edfi_conn_id: Optional[str] = None,

        config_file: Optional[str] = None,
        selector: Optional[Union[str, Iterable[str]]] = None,
        parameters: Optional[Union[str, dict]] = None,

        wipe: bool = False,
        force: bool = False,

        older_than: Optional[str] = None,
        newer_than: Optional[str] = None,
        resend_status_codes: Optional[Union[str, Iterable[str]]] = None,

        **kwargs
    ):
        self.lightbeam_path = lightbeam_path or 'lightbeam'
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
            if not isinstance(parameters, str):
                parameters = json.dumps(parameters)
            self.arguments['--params'] = f"'{parameters}'"  # Force double-quotes around JSON keys

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
        super().__init__(bash_command=bash_command_prefix, env=env_vars, **kwargs)


    def execute(self, context) -> str:
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

        # Overwrite `force` if defined in context.
        if get_context_parameter(context, 'force'):
            logging.info("Parameter `force` provided in context will overwrite defined operator argument.")
            self.arguments['--force'] = ""

        # Update final Lightbeam command with any passed arguments
        # This update occurs here instead of init to allow context parameters to be passed.
        self.bash_command += " ".join(f"{kk} {vv}" for kk, vv in self.arguments.items())
        self.bash_command = self.bash_command.replace("{", "'{").replace("}", "}'")  # Force single-quotes around params
        logging.info(f"Complete Lightbeam CLI command: {self.bash_command}")

        super().execute(context)
        return self.data_dir
