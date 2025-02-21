import itertools
import logging
import os
import re
import subprocess
import tempfile

from collections.abc import Iterable
from jinja2 import Template

from typing import Any, Callable, Dict, List, Optional
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from airflow import DAG
    from airflow.models.base import Operator
    from airflow.utils.task_group import TaskGroup

from airflow.exceptions import AirflowSkipException
from airflow.models import Param
from airflow.models.baseoperator import chain

from ea_airflow_util.callables import ftp, sharefile
from ea_airflow_util.callables.zip import extract_zips
from edu_edfi_airflow import EarthbeamDAG


class EarthbeamDAGFactory:
    """
    EarthbeamDAGFactory is an interface for dynamically generating EarthbeamDAGs with different source preprocesses.
    Each input source uses a bespoke child class of this interface, with the following methods to be overridden:
      - build_custom_preprocess: Optional method called before tenant-year taskgroups (e.g., pulling and sharding data from an SFTP)
      - python_preprocess_callable: Class method to check/prepare data in the tenant-year taskgroup
      - build_taskgroup_kwargs: Method to populate kwargs passed to `python_preprocess_callable` with tenant-year grain-level information

    The generated DAGs can be retrieved using dictionary methods (keys, values, items).
    """
    RUN_DYNAMIC: bool = False  # Class attribute set by child factory class, determines whether to use dynamic task mapping in Earthbeam taskgroup.

    def __init__(self,
        run_type: str,
        tenant_codes: List[str],
        api_years: List[str],

        earthmover_bundle: str,
        student_id_types: str,
        input_vars: List[str] = ["INPUT_FILE"],

        subtypes: List[Optional[str]] = [None],
        endpoints: List[str] = [
            "assessments",
            "objectiveAssessments",
            "studentAssessments",
            "assessmentReportingMethodDescriptors",
            "performanceLevelDescriptors",
        ],

        # Optional postprocess callable (e.g., uploading failed students to ShareFile)
        python_postprocess_callable: Optional['Callable'] = None,
        python_postprocess_kwargs: Optional[dict] = None,

        # Optional Earthmover parameter overrides
        earthmover_pool: str = 'memory_intensive',
        input_filetype: str = 'csv',
        descriptor_namespace: str = 'uri://ed-fi.org',
        opt_earthmover_kwargs: Optional[Dict[str, str]] = None,

        # Optional sideloading arguments
        sideload_before: Optional[str] = None,
        ods_version: str = '5.3',
        data_model_version: str = "3.3.1-b",

        # Implementation-specific overrides
        snowflake_conn_id: str = "snowflake",
        snowflake_read_conn_id: str = "snowflake_read",
        s3_conn_id: str = "data_lake",
        s3_filepath: str = "_earthmover",

        # Optional lambdas for naming Ed-Fi connections and tenant-codes in ID xwalking 
        edfi_conn_id_lambda: Callable[[str, str], str] = lambda tenant_code, api_year: f"edfi_{tenant_code}_{api_year}",
        snowflake_tenant_code_override: Optional[Callable[[str, str], str]] = None,  # Historic tenant-consolidations
        
        # Optional sharding arguments
        tenant_col: Optional[str] = None,  # Shard the output on tenant_col if specified
        year_col: Optional[str] = None,  # Shard the output on year_col if specified (default `api_year`)
        tenant_map: Optional[dict] = None,
        year_map: Optional[dict] = None,

        # Generic DAG arguments
        schedule_interval: Optional[str] = None,
        default_args: Optional[dict] = None,

        run_in_dev: bool = False,  # When true, sideloads data to dev Stadium instead of posting to Ed-Fi.
    ):
        # Globals can't be updated within a method: `globals()[earthbeam_dag.dag.dag_id] = earthbeam_dag.dag`
        # Save these in a dictionary to be accessed with `keys`, `values`, and `items`.
        self.DAGS_MAPPING = {}
        self.RUN_IN_DEV: bool = run_in_dev 

        self.run_type: str = run_type
        self.earthmover_bundle: str = earthmover_bundle
        self.student_id_types: str = student_id_types
        self.input_vars: List[str] = [input_vars] if isinstance(input_vars, str) else input_vars
        self.endpoints: List[str] = endpoints

        self.input_filetype: str = input_filetype
        self.descriptor_namespace: str = descriptor_namespace
        self.opt_earthmover_kwargs: Dict[str, str] = opt_earthmover_kwargs or {}

        self.sideload_before: Optional[str] = str(sideload_before) if sideload_before else None
        self.ods_version: str = ods_version
        self.data_model_version: str = data_model_version

        self.snowflake_conn_id: str = snowflake_conn_id
        self.snowflake_read_conn_id: str = snowflake_read_conn_id
        self.s3_conn_id: str = s3_conn_id
        self.s3_filepath: str = s3_filepath

        self.edfi_conn_id_lambda: Callable[[str, str], str] = edfi_conn_id_lambda
        self.snowflake_tenant_code_override: Optional[Callable[[str, str], str]] = snowflake_tenant_code_override

        self.python_postprocess_callable: Optional['Callable'] = python_postprocess_callable
        self.python_postprocess_kwargs: dict = python_postprocess_kwargs or {}

        # Optional sharding arguments (TODO: Make an optional mixin supported by all DAGs)
        self.tenant_col: Optional[str] = tenant_col  # Shard the output on tenant_col if specified
        self.year_col: Optional[str] = year_col  # Shard the output on year_col if specified (default `api_year`)
        self.tenant_map: Optional[dict] = tenant_map
        self.year_map: Optional[dict] = year_map

        # Build one DAG per year and subtype
        for api_year, subtype in itertools.product(api_years, subtypes):

            run_type_full = f"{self.run_type}_{subtype.lower()}" if subtype else self.run_type

            # Instantiate and register DAG with run-level params.
            params = {
                "tenant_codes": Param(
                    default=tenant_codes,
                    examples=tenant_codes,
                    type="array",
                    description="Newline-separated list of tenants to process",
                ),
                # "endpoints": Param(
                #     default=endpoints,
                #     examples=endpoints,
                #     type="array",
                #     description="Newline-separated list of endpoints to post to Ed-Fi or sideload to Stadium"
                # ),
            }
            default_args = default_args or {}
            default_args.update({'params': params})  # Inject params into default_args to prevent double-declaration.

            earthbeam_dag = EarthbeamDAG(
                run_type=run_type_full,
                earthmover_path="/home/airflow/.venv/airflow/bin/earthmover",
                lightbeam_path="/home/airflow/.venv/airflow/bin/lightbeam",
                earthmover_pool=earthmover_pool,

                dag_id=f"earthbeam__{run_type_full}_{api_year}",
                schedule_interval=schedule_interval,
                default_args=(default_args or {})
            )

            # Use custom child logic to build any optional top-level tasks to chain before tenant-taskgroups.
            top_level_preprocess: Optional['Operator'] = self.build_custom_preprocess(api_year, subtype, earthbeam_dag)

            # One taskgroup per tenant
            for tenant_code in tenant_codes:

                # Use custom child logic to build the tenant-level grain tasks.
                custom_taskgroup: 'TaskGroup' = self.build_earthbeam_taskgroup(tenant_code, api_year, subtype, earthbeam_dag)

                # Add optional tasks outside the grain if defined.
                if top_level_preprocess:
                    chain(top_level_preprocess, custom_taskgroup)

            self.DAGS_MAPPING[earthbeam_dag.dag.dag_id] = earthbeam_dag.dag

    # Methods for accessing dynamic DAGs
    def keys(self):
        return self.DAGS_MAPPING.keys()
    
    def values(self):
        return self.DAGS_MAPPING.values()
    
    def items(self):
        return self.DAGS_MAPPING.items()
    

    # Methods to overwrite in child classes
    def build_custom_preprocess(self, api_year: str, subtype: Optional[str], earthbeam_dag: 'DAG') -> Optional['Operator']:
        """ Default to None and override as necessary. Note: DAG level, so no tenant_code."""
        return None
    
    @classmethod
    def python_preprocess_callable(cls, **kwargs):
        """ This is the preprocess callable passed into the tenant-level Earthbeam taskgroup init. """
        raise NotImplementedError("EarthbeamDAGFactory.python_preprocess_callable() requires custom implementation!")

    def build_taskgroup_kwargs(self, tenant_code: str, api_year: str, subtype: Optional[str], earthbeam_dag: 'DAG') -> dict:
        """
        Return a dictionary with the following keys:
        - python_kwargs: Input for python_preprocess_callable
        - input_file_mapping OR input_file_var: Mutually-exclusive taskgroup mapping.
        """
        raise NotImplementedError("EarthbeamDAGFactory.build_taskgroup_kwargs() requires custom implementation!")

    def build_earthbeam_taskgroup(self, tenant_code: str, api_year: str, subtype: Optional[str], earthbeam_dag: 'DAG') -> 'TaskGroup':
        """
        Internal helper method to simplify declaration of EarthbeamDAG.build_tenant_year_taskgroup().
        """
        # Build the formatted preprocess and file pathing arguments based on the grain of the taskgroup.
        format_kwargs = {
            'tenant_code': tenant_code,
            'api_year': str(api_year),
            'subtype': subtype,
            'run_type': self.run_type,
            'earthmover_bundle': self.earthmover_bundle,
        }

        taskgroup_kwargs: dict = self.build_taskgroup_kwargs(tenant_code, api_year, subtype, earthbeam_dag=earthbeam_dag)
        
        python_kwargs: dict = taskgroup_kwargs.get('python_kwargs', {})
        input_file_var: Optional[str] = taskgroup_kwargs.get('input_file_var')
        input_file_mapping: Optional[Dict[str, str]] = taskgroup_kwargs.get('input_file_mapping')
        
        if not (input_file_var or input_file_mapping):
            raise KeyError("Key `input_file_var` or `input_file_mapping` required in output of `EarthbeamDAGFactory.build_taskgroup_kwargs()`!")

        # Must be defined within scope to set tenant_code.
        def check_tenant_and_run(**context):
            """
            Helper method to force tenant_code checking at the start of the Python preprocess.
            """
            if tenant_code not in context["params"]["tenant_codes"]:
                logging.info(f"Tenant code not specified to run. Skipping taskgroup.")
                raise AirflowSkipException

            preprocess_kwargs = {key: val for key, val in context.items() if key in python_kwargs}
            return self.python_preprocess_callable(**preprocess_kwargs)


        # Taskgroup kwargs differ depending on whether the data is sent into Ed-Fi or sideloaded into Stadium
        NO_ODS_EXISTS = int(api_year) < int(self.sideload_before) if self.sideload_before else False
        
        if self.RUN_IN_DEV or NO_ODS_EXISTS:
            input_kwargs = {
                'ods_version': self.ods_version,
                'data_model_version': self.data_model_version,
                'endpoints': self.endpoints,
            }
        else:
            input_kwargs = {
                'edfi_conn_id': self.edfi_conn_id_lambda(tenant_code, api_year),
            }

        # Use dynamic taskgroup if specified.
        taskgroup_callable = earthbeam_dag.build_dynamic_tenant_year_taskgroup if self.RUN_DYNAMIC else earthbeam_dag.build_tenant_year_taskgroup

        tenant_year_taskgroup = taskgroup_callable(
            tenant_code=tenant_code,
            api_year=api_year,
            raw_dir=None if self.RUN_DYNAMIC else earthbeam_dag.build_local_raw_dir(tenant_code, api_year, subtype),
            grain_update=subtype,

            # Check for tenant_code in params before running the callable.
            python_callable=check_tenant_and_run,
            python_kwargs=python_kwargs,

            python_postprocess_callable=self.python_postprocess_callable,
            python_postprocess_kwargs={  # Inject grain-level variables into postprocess.
                **self.render_jinja(self.python_postprocess_kwargs, format_kwargs),  # Apply Jinja templating to postprocess kwargs.
                'tenant_code': tenant_code,
                'api_year': api_year,
                'subtype': subtype,
                'earthmover_bundle': self.earthmover_bundle
            },

            # These arguments are mutually-exclusive depending on dynamic vs sequential runs.
            input_file_mapping=input_file_mapping,
            input_file_var=input_file_var,

            logging_table='_earthbeam_logging',
            full_refresh=True,

            snowflake_conn_id=self.snowflake_conn_id,
            s3_conn_id=self.s3_conn_id,
            s3_filepath=self.s3_filepath,

            assessment_bundle=self.earthmover_bundle,
            student_id_match_rates_table='dev_raw.data_integration.student_id_match_rates' if self.RUN_IN_DEV else 'raw.data_integration.student_id_match_rates',
            snowflake_read_conn_id=self.snowflake_read_conn_id,
            earthmover_kwargs={
                'config_file': "/home/airflow/code/earthmover_edfi_bundles/packages/student_id_wrapper/earthmover.yaml",
                'skip_hashing': True,
                'parameters': {
                    'API_YEAR': str(api_year),
                    'SCHOOL_YEAR': str(api_year),
                    'TEMPORARY_DIRECTORY': "/efs/dask_temp",
                    'INPUT_FILETYPE': self.input_filetype,
                    'EDFI_STUDENT_ID_TYPES': self.student_id_types,  # The IDs used for xwalking differ by assessment
                    'DESCRIPTOR_NAMESPACE': self.descriptor_namespace,
                    'SNOWFLAKE_TENANT_CODE': self.snowflake_tenant_code_override(tenant_code, api_year) if self.snowflake_tenant_code_override else tenant_code,
                    **self.opt_earthmover_kwargs,
                },
                # 'selector': self.endpoints
            },

            validate_edfi_conn_id=self.edfi_conn_id_lambda(tenant_code, api_year) if not NO_ODS_EXISTS else None,
            lightbeam_kwargs={  # Used to validate in both types of runs.
                'config_file': "/home/airflow/airflow/dags/earthmover/lightbeam.yaml",
                'force': True,
                'parameters': {
                    'API_YEAR': str(api_year),
                },
                # 'selector': self.endpoints
            },

            **input_kwargs    
        )

        return tenant_year_taskgroup
    

    # Helper method to render Jinja in any kwargs passed to the factory
    def render_jinja(self, obj: Any, context: dict) -> Any:
            
        if isinstance(obj, dict):
            return {key: self.render_jinja(value, context) for key, value in obj.items()}
        elif isinstance(obj, str):
            return Template(obj).render(context)
        elif isinstance(obj, Iterable):
            return [self.render_jinja(item, context) for item in obj]
        else:
            return obj



class S3EarthbeamDAGFactory(EarthbeamDAGFactory):
    """
    Access data in S3 pre-sharded by tenant and year in separate subdirectories.
    """
    def __init__(self, *args, s3_bucket: str, s3_paths: List[str], **kwargs):
        self.s3_bucket: str = s3_bucket
        self.s3_paths: List[str] = [s3_paths] if isinstance(s3_paths, str) else s3_paths
        super().__init__(*args, **kwargs)

        if len(self.input_vars) != len(self.s3_paths):
            raise ValueError("EarthbeamDAGFactory variables `input_vars` and `s3_paths` must have same cardinality!")

    def build_taskgroup_kwargs(self, tenant_code: str, api_year: str, subtype: Optional[str], earthbeam_dag: 'DAG'):
        ### Format variables with tenant-year grain information
        format_kwargs = {
            'tenant_code': tenant_code,
            'api_year': api_year,
            'subtype': subtype,
        }
        formatted_s3_paths = self.render_jinja(self.s3_paths, format_kwargs)

        local_raw_dir = earthbeam_dag.build_local_raw_dir(tenant_code, api_year, subtype)
        formatted_local_dirs = [
            os.path.join(local_raw_dir, subfolder, os.path.basename(s3_path)) if os.path.splitext(s3_path)[-1]
            else os.path.join(local_raw_dir, subfolder)
            for subfolder, s3_path in zip(self.input_vars, formatted_s3_paths)
        ]
        input_file_mapping = dict(zip(self.input_vars, formatted_local_dirs))
        
        python_kwargs={
            's3_bucket': self.s3_bucket,
            's3_paths': formatted_s3_paths,
            'local_dirs': formatted_local_dirs,
        }

        return {
            'python_kwargs': python_kwargs,
            'input_file_mapping': input_file_mapping,
        }

    @classmethod
    def python_preprocess_callable(cls, s3_bucket: str, s3_paths: List[str], local_dirs: List[str]) -> List[str]:
        """
        
        """
        if len(s3_paths) != len(local_dirs):
            raise Exception("Arguments `s3_paths` and `local_dirs` must be equal in length!")

        copy_check_command = cls.build_copy_check_command(s3_bucket, s3_paths, local_dirs)
        logging.info(f"Running shell command: {copy_check_command}")

        completed_process = subprocess.run(copy_check_command, shell=True)
        if completed_process.returncode == 99:
            raise AirflowSkipException("Task skipped because no files were found!")
        
        return local_dirs
    
    @classmethod
    def build_copy_check_command(cls, s3_bucket: str, s3_paths: List[str], local_dirs: List[str]):
        # Recursive flag only works on directories, not files.
        copy_commands = [
            f"aws s3 cp s3://{s3_bucket}/{path} {dir}" if os.path.splitext(path)[-1]
            else f"aws s3 cp s3://{s3_bucket}/{path} {dir} --recursive"
            for path, dir in zip(s3_paths, local_dirs)
        ]

        list_commands = [
            f'[ "$(ls -A {dir})" ]'
            for dir in local_dirs
        ]

        return "({}) && ({}) || exit 99".format(
            ' && '.join(copy_commands),  # Run all copy commands.
            ' || '.join(list_commands)   # Succeed if any list commands succeed.
        ).replace("\n", "")



class SFTPEarthbeamDAGFactory(EarthbeamDAGFactory):
    """
    Access data on an SFTP to be unzipped and sharded before processing
    """
    def __init__(self, *args, ftp_conn_id: str, remote_path: str, file_patterns: Optional[List[str]] = None, **kwargs):
        self.ftp_conn_id: str = ftp_conn_id
        self.remote_path: str = remote_path
        self.file_patterns: List[str] = file_patterns or []
        super().__init__(*args, **kwargs)

        if self.file_patterns and len(self.input_vars) != len(self.file_patterns):
            raise ValueError("EarthbeamDAGFactory variables `input_vars` and `file_patterns` must have same cardinality!")

    def build_taskgroup_kwargs(self, tenant_code: str, api_year: str, subtype: Optional[str], earthbeam_dag: 'DAG'):
        ### Format variables with tenant-year grain information
        formatted_local_dirs = [
            os.path.join(local_dir, f"tenant_code={tenant_code}", f"api_year={api_year}")
            for local_dir in self.sharded_dirs  # Defined in `build_custom_preprocess()`
        ]
        input_file_mapping = dict(zip(self.input_vars, formatted_local_dirs))

        python_kwargs={
            'path': formatted_local_dirs[0]  # Only check the first directory (presumed mandatory with optional secondaries)
        }

        return {
            'python_kwargs': python_kwargs,
            'input_file_mapping': input_file_mapping,
        }
    
    @classmethod
    def python_preprocess_callable(cls, path: str):
        """
        SFTP-pull returns XComs for all possible filepaths, regardless if they are defined.
        This skips if the XCom returns an empty string.
        """
        if not path or not os.path.exists(path):
            logging.info(f"Filepath not found: {path}")
            raise AirflowSkipException
        
        # Raise if the file is empty as well.
        if not os.path.getsize(path):
            logging.info(f"File is empty: {path}")
            raise AirflowSkipException

        return path
    
    def build_custom_preprocess(self, api_year: str, subtype: Optional[str], earthbeam_dag: 'DAG'):
        # Preprocess ZIP and shard by district IDs.
        preprocess_raw_dir = earthbeam_dag.build_local_raw_dir("_preprocess", api_year, subtype)
        downloaded_dir = os.path.join(preprocess_raw_dir, '_downloaded')
        extracted_dirs = [
            os.path.join(preprocess_raw_dir, '_extracted', env_var)
            for env_var in self.input_vars
        ]
        self.sharded_dirs = [
            os.path.join(preprocess_raw_dir, '_sharded', env_var)
            for env_var in self.input_vars
        ]

        download_sftp_to_disk = earthbeam_dag.build_python_preprocessing_operator(
            ftp.download_all,
            ftp_conn_id=self.ftp_conn_id,
            remote_dir=self.remote_path,
            local_dir=downloaded_dir,
        )

        ### Format variables with subtype-year grain information
        format_kwargs = {
            'api_year': api_year,
            'subtype': subtype,
        }
        formatted_file_patterns = self.render_jinja(self.file_patterns, format_kwargs)

        extract_zips_to_disk = earthbeam_dag.build_python_preprocessing_operator(
            self.extract_match_zips,
            zip_dir=downloaded_dir,
            local_dirs=extracted_dirs,
            file_patterns=formatted_file_patterns
        )

        shard_extracted_to_parquet = earthbeam_dag.build_python_preprocessing_operator(
            EarthbeamDAG.partition_on_tenant_and_year,
            csv_paths=[os.path.join(dir, "*.csv") for dir in extracted_dirs],  # Force to glob wildcard pathing
            output_dirs=self.sharded_dirs,

            tenant_col=self.tenant_col,
            tenant_map=self.tenant_map,
            year_col=self.year_col or self.tenant_col,  # Default to any column if none defined.
            year_map=self.year_map or (lambda _: str(api_year))  # Default to API year if a mapping not defined.
        )

        download_sftp_to_disk >> extract_zips_to_disk >> shard_extracted_to_parquet

        return shard_extracted_to_parquet
    
    @classmethod
    def extract_match_zips(cls,
        zip_dir: str,
        local_dirs: str,
        file_patterns: List[str],
        **context
    ):
        """
        `zip_dir` is the top-level folder under which one or more ZIPs are placed.
        """
        if len(local_dirs) != len(file_patterns):
            raise ValueError("List arguments `local_dirs` and `file_patterns` must be the same length!")

        # Isolate the root directory to create a temporary directory during extraction.
        for local_dir in local_dirs:
            os.makedirs(local_dir, exist_ok=True)

        # Use context manager to delete directory when finished.
        root_dir = os.path.dirname(local_dirs[0])
        with tempfile.TemporaryDirectory(dir=root_dir) as tmp_dir:
            logging.info(f"Extracting files to temporary directory: {tmp_dir}")

            extract_zips(zip_dir, tmp_dir, remove_zips=False)

            # Filter on file patterns, copying the files to their correct directory.
            for base_path, _, files in os.walk(tmp_dir):
                for file in files:
                    for local_dir, file_pattern in zip(local_dirs, file_patterns):
                        if not re.match(file_pattern, file):
                            logging.info(f"! File does not match pattern: {file_pattern} >> {file}")
                            continue

                        logging.info(f"File matches pattern: {file_pattern} >> {file}")
                        unique_filename = f"{os.path.basename(base_path).replace('/', '_')}__{file}"  # Include zip folder name in case of collisions.

                        if not os.path.getsize(os.path.join(base_path, file)):
                            logging.warning("    File is empty. Skipping extraction...")
                            continue

                        os.rename(os.path.join(base_path, file), os.path.join(local_dir, unique_filename))

        return local_dirs



class SharefileEarthbeamDAGFactory(EarthbeamDAGFactory):
    """
    Access data on ShareFile pre-sharded by tenant and year in separate subdirectories.
    """
    RUN_DYNAMIC: bool = True  # Multiple files per assessment can be uploaded to a single folder in ShareFile.

    def __init__(self, *args, sharefile_conn_id: str, remote_path: str, **kwargs):
        self.sharefile_conn_id = sharefile_conn_id
        self.remote_path = remote_path
        super().__init__(*args, **kwargs)

    @classmethod
    def python_preprocess_callable(cls, sharefile_conn_id: str, sharefile_path: str, local_path: str, delete_remote: bool = False):
        """
        TODO: Why did this need to be declared explicitly to work?
        Otherwise: `KeyError: 'tenant_code' not found`
        """
        return sharefile.sharefile_to_disk(sharefile_conn_id, sharefile_path, local_path, delete_remote=delete_remote)

    def build_taskgroup_kwargs(self, tenant_code: str, api_year: str, subtype: Optional[str], earthbeam_dag: 'DAG'):
        ### Format variables with tenant-year grain information
        format_kwargs = {
            'tenant_code': tenant_code,
            'api_year': api_year,
            'subtype': subtype,
        }

        python_kwargs={
            'sharefile_conn_id': self.sharefile_conn_id,
            'sharefile_path': self.render_jinja(self.remote_path, format_kwargs),
            'local_path': earthbeam_dag.build_local_raw_dir(tenant_code, api_year, subtype),
            'delete_remote': False,
        }

        return {
            'python_kwargs': python_kwargs,
            'input_file_var': self.input_vars[0],  # Remove local_raw_dir for dynamic run.
        }
