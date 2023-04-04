import logging
import os

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def local_filepath_to_s3(
    local_filepath: str,
    s3_destination_key: str,
    s3_conn_id: str,
    remove_local_filepath: bool = False
):
    """

    :param local_filepath:
    :param s3_destination_key:
    :param s3_conn_id:
    :param remove_local_filepath:
    :return:
    """
    try:
        s3_hook = S3Hook(aws_conn_id=s3_conn_id)
        s3_bucket = s3_hook.get_connection(s3_conn_id).schema

        s3_hook.load_file(
            filename=local_filepath,

            bucket_name=s3_bucket,
            key=s3_destination_key,

            encrypt=True,
            replace=True
        )
    finally:
        if remove_local_filepath:
            logging.info(f"Removing temporary files written to `{local_filepath}`")
            try:
                os.remove(local_filepath)
            except FileNotFoundError:
                pass

    return s3_destination_key
