import json
from typing import Iterable, Optional, Union

from airflow.utils.decorators import apply_defaults
from airflow.operators.bash import BashOperator

from edu_edfi_airflow.providers.edfi.hooks.edfi import EdFiHook

class EarthmoverOperator(BashOperator):
    """

    """
    @apply_defaults
    def __init__(self,
        *,
        output_dir : str,
        config_file: Optional[str] = None,
        selector   : Optional[Union[str, Iterable[str]]] = None,
        params     : Optional[Union[str, dict]] = None,

        force          : bool = False,
        skip_hashing   : bool = False,
        show_graph     : bool = False,
        show_stacktrace: bool = False,

        **kwargs
    ):
        arguments = {}

        # Dynamic arguments
        if config_file:
            arguments['--config-file'] = config_file

        if selector:  # Pre-built selector string or list of node names
            if not isinstance(selector, str):
                selector = ",".join(selector)
            arguments['--selector'] = selector

        # Parameters can be set manually, but output_dir must be defined
        if params or output_dir:  # JSON string or dictionary

            # Force to a dictionary to allow editing
            params = params or {}
            if isinstance(params, str):
                params = json.loads(params)

            # Add required `output_dir` parameter
            params['OUTPUT_DIR'] = output_dir

            arguments['--params'] = json.dumps(params)

        # Boolean arguments
        if force:
            arguments['--force'] = ""
        if skip_hashing:
            arguments['--skip-hashing'] = ""
        if show_graph:
            arguments['--show-graph'] = ""
        if show_stacktrace:
            arguments['--show-stacktrace'] = ""

        # Build out the final Earthmover command with any passed arguments
        arguments_string = " ".join(f"{kk} {vv}" for kk, vv in arguments.items())
        bash_command = f"earthmover run {arguments_string}"

        super().__init__(bash_command=bash_command, **kwargs)



class LightbeamOperator(BashOperator):
    """

    """
    valid_commands = ('validate', 'send', 'validate+send')

    @apply_defaults
    def __init__(self,
        *,
        data_dir: str,

        command: str = 'send',
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
        # Verify command argument is valid
        if command not in self.valid_commands:
            raise ValueError(
                f"LightbeamOperator command type `{command}` is undefined!"
            )

        arguments = {}

        # Dynamic arguments
        if config_file:
            arguments['--config-file'] = config_file

        if selector:  # Pre-built selector string or list of node names
            if not isinstance(selector, str):
                selector = ",".join(selector)
            arguments['--selector'] = selector

        # Parameters can be set manually, or inferred from an Airflow connection
        if params or edfi_conn_id or data_dir:  # JSON string or dictionary

            # Force to a dictionary to allow editing
            params = params or {}
            if isinstance(params, str):
                params = json.loads(params)

            # Add required `data_dir` parameter
            params['DATA_DIR'] = data_dir

            if edfi_conn_id:
                edfi_conn = EdFiHook(edfi_conn_id).get_conn()

                params['BASE_URL'] = edfi_conn.host
                params['CLIENT_ID'] = edfi_conn.login
                params['CLIENT_SECRET'] = edfi_conn.password

                _api_year = edfi_conn.extra_dejson.get('api_year')
                if _api_year:
                    params['YEAR'] = _api_year

                _api_version = edfi_conn.extra_dejson.get('api_version')
                if _api_version:
                    params['VERSION'] = _api_version

                _api_mode = edfi_conn.extra_dejson.get('api_mode')
                if _api_mode:
                    params['MODE'] = _api_mode

            arguments['--params'] = json.dumps(params)

        if resend_status_codes:
            if not isinstance(resend_status_codes, str):
                resend_status_codes = ",".join(resend_status_codes)
            arguments['--resend-status-codes'] = resend_status_codes

        # Boolean arguments
        if wipe:
            arguments['--wipe'] = ""
        if force:
            arguments['--force'] = ""

        # Optional string arguments
        if older_than:
            arguments['--older-than'] = older_than
        if newer_than:
            arguments['--newer-than'] = newer_than

        # Build out the final Lighbeam command with any passed arguments
        arguments_string = " ".join(f"{kk} {vv}" for kk, vv in arguments.items())
        bash_command = f"lightbeam {command} {arguments_string}"

        super().__init__(bash_command=bash_command, **kwargs)
