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

        # If a directory, upload all files to S3.
        if os.path.isdir(local_filepath):
            for root, dirs, files in os.walk(local_filepath):
                for file in files:
                    full_path = os.path.join(root, file)
                    s3_full_path = os.path.join(s3_destination_key, file)

                    s3_hook.load_file(
                        filename=full_path,
                        bucket_name=s3_bucket,
                        key=s3_full_path,
                        encrypt=True,
                        replace=True
                    )

        # Otherwise, upload the single file
        else:
            s3_hook.load_file(
                filename=local_filepath,
                bucket_name=s3_bucket,
                key=s3_destination_key,
                encrypt=True,
                replace=True
            )

    # Regardless, delete the local files if specified.
    finally:
        if remove_local_filepath:
            logging.info(f"Removing temporary files written to `{local_filepath}`")
            try:
                if os.path.isdir(local_filepath):
                    os.rmdir(local_filepath)
                else:
                    os.remove(local_filepath)
            except FileNotFoundError:
                pass

    return s3_destination_key
