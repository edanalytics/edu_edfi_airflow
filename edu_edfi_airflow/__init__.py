from edu_edfi_airflow.dags.earthbeam_dag import EarthbeamDAG
from edu_edfi_airflow.dags.edfi_resource_dag import EdFiResourceDAG
from edu_edfi_airflow.dags.lightbeam_delete_dag import LightbeamDeleteDAG
from edu_edfi_airflow.providers.earthbeam.operators import EarthmoverOperator, LightbeamOperator
from edu_edfi_airflow.providers.edfi.hooks.edfi import EdFiHook
from edu_edfi_airflow.providers.edfi.transfers.edfi_to_s3 import EdFiToS3Operator

from edu_edfi_airflow.callables import airflow_util as util


# Reroute deprecated module pathing.
import sys
from ea_airflow_util import LazyModule

rename_mapping = {
    "edu_edfi_airflow.dags.dag_util": {
        "name": "edu_edfi_airflow.callables",
        "child_mapping": {
            "airflow_util": "airflow_util",
        }
    },
    "edu_edfi_airflow.dags.callables": {
        "name": "edu_edfi_airflow.callables",
        "child_mapping": {
            "change_version": "change_version",
            "s3": "s3",
            "snowflake": "snowflake",
        }
    },
}

for old_path, new_path_metadata in rename_mapping.items():
    new_path = new_path_metadata['name']
    child_mapping = new_path_metadata['child_mapping']
    sys.modules[old_path] = LazyModule(old_path, new_path, child_mapping=child_mapping)

    for old_child, new_child in child_mapping.items():
        full_old_path = '.'.join([old_path, old_child])
        full_new_path = '.'.join([new_path, new_child])
        sys.modules[full_old_path] = LazyModule(full_old_path, full_new_path)
