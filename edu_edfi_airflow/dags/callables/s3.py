import logging
import os
import shutil

from typing import Iterator, Tuple

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def walk_directory(directory: str) -> Iterator[Tuple[str, str]]:
    """
    Recursively walk a directory, returning subfolders and filenames off of `root`.

    :param directory:
    :return:
    """
    for root, _, files in os.walk(directory):
        subdir = root.replace(directory, '').strip('/')
        for file in files:
            yield subdir, file


def remove_filepath(path: str):
    """

    :param path:
    :return:
    """
    logging.info(f"Removing local filepath: `{path}`")
    try:
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)
    except FileNotFoundError:
        logging.warning("Filepath not found.")
        pass


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
            for subdir, file in walk_directory(local_filepath):
                full_path = os.path.join(local_filepath, subdir, file)
                s3_full_path = os.path.join(s3_destination_key, subdir, file)

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
            remove_filepath

    return s3_destination_key
