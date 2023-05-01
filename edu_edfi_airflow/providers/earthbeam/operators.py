import json
from typing import Iterable, Optional, Union

from airflow.operators.bash import BashOperator

from edu_edfi_airflow.providers.edfi.hooks.edfi import EdFiHook

class EarthmoverOperator(BashOperator):
    """

    """
    def __init__(self,
        *,
        output_dir : Optional[str] = None,
        state_file : Optional[str] = None,
        config_file: Optional[str] = None,
        selector   : Optional[Union[str, Iterable[str]]] = None,
        params     : Optional[Union[str, dict]] = None,

        force          : bool = False,
        skip_hashing   : bool = False,
        show_graph     : bool = False,
        show_stacktrace: bool = False,

        **kwargs
    ):
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

        if params:  # JSON string or dictionary
            if not isinstance(params, str):
                params = json.dumps(params)
            self.arguments['--params'] = params

        # Boolean arguments
        if force:
            self.arguments['--force'] = ""
        if skip_hashing:
            self.arguments['--skip-hashing'] = ""
        if show_graph:
            self.arguments['--show-graph'] = ""
        if show_stacktrace:
            self.arguments['--show-stacktrace'] = ""

        # Build out the final Earthmover command with any passed arguments
        arguments_string = " ".join(f"{kk} {vv}" for kk, vv in self.arguments.items())
        bash_command = f"earthmover run {arguments_string}"

        ### Environment variables
        # Pass required `output_dir` parameter as environment variables
        env_vars = {
            'OUTPUT_DIR': self.output_dir,
            'STATE_FILE': self.state_file,
        }

        super().__init__(bash_command=bash_command, env=env_vars, **kwargs)






class LightbeamOperator(BashOperator):
    """

    """
    valid_commands = ('validate', 'send', 'validate+send')

    def __init__(self,
        *,
        command: str = 'send',

        data_dir: Optional[str] = None,
        state_dir: Optional[str] = None,

        edfi_conn_id: Optional[str] = None,

        config_file: Optional[str] = None,
        selector: Optional[Union[str, Iterable[str]]] = None,
        params: Optional[Union[str, dict]] = None,

        wipe: bool = False,
        force: bool = False,

        older_than: Optional[str] = None,
        newer_than: Optional[str] = None,
        resend_status_codes: Optional[Union[str, Iterable[str]]] = None,

        **kwargs
    ):
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

        if params:  # JSON string or dictionary
            if not isinstance(params, str):
                params = json.dumps(params)
            self.arguments['--params'] = params

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

        # Build out the final Lightbeam command with any passed arguments
        arguments_string = " ".join(f"{kk} {vv}" for kk, vv in self.arguments.items())
        bash_command = f"lightbeam {command} {arguments_string}"

        ### Environment variables
        # Pass required `data_dir`
        env_vars = {
            'DATA_DIR'  : self.data_dir,
            'STATE_DIR': self.state_dir,
        }

        super().__init__(bash_command=bash_command, env=env_vars, **kwargs)


    def execute(self, context) -> str:
        """

        :param context:
        :return:
        """
        #  Extract EdFi connection parameters as environment variables if defined
        # (This must be done in execute to prevent extraction during DAG-parsing.
        if self.edfi_conn_id:
            edfi_conn = EdFiHook(self.edfi_conn_id).get_conn()

            self.env['BASE_URL'] = edfi_conn.host
            self.env['CLIENT_ID'] = edfi_conn.login
            self.env['CLIENT_SECRET'] = edfi_conn.password

            _api_year = edfi_conn.extra_dejson.get('api_year')
            if _api_year:
                self.env['YEAR'] = _api_year

            _api_version = edfi_conn.extra_dejson.get('api_version')
            if _api_version:
                self.env['VERSION'] = _api_version

            _api_mode = edfi_conn.extra_dejson.get('api_mode')
            if _api_mode:
                self.env['MODE'] = _api_mode

        super().execute(context)
        return self.data_dir
