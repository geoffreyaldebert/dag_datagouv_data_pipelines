import logging
import boto3
import botocore
from minio import Minio, S3Error
from minio.commonconfig import CopySource
from typing import List, TypedDict, Optional
import os
import io
import json
import magic

from datagouvfr_data_pipelines.config import (
    AIRFLOW_ENV,
    MINIO_URL,
    SECRET_MINIO_DATA_PIPELINE_USER,
    SECRET_MINIO_DATA_PIPELINE_PASSWORD,
)
from datagouvfr_data_pipelines.utils.retry import simple_connection_retry


class File(TypedDict):
    source_path: str
    source_name: str
    dest_path: str
    dest_name: str
    content_type: Optional[str]


class MinIOClient:
    def __init__(
        self,
        bucket: Optional[str] = None,
        user: Optional[str] = None,
        pwd: Optional[str] = None,
        login: bool = True,
        http_client=None,
    ):
        self.url = MINIO_URL
        self.user = user or SECRET_MINIO_DATA_PIPELINE_USER
        self.password = pwd or SECRET_MINIO_DATA_PIPELINE_PASSWORD
        self.bucket = bucket
        self.client = Minio(
            self.url,
            access_key=self.user if login else None,
            secret_key=self.password if login else None,
            http_client=http_client,
            secure=True,
        )
        if self.bucket:
            self.bucket_exists = self.client.bucket_exists(self.bucket)
            if not self.bucket_exists:
                raise ValueError(f"Bucket '{self.bucket}' does not exist.")

    @simple_connection_retry
    def send_files(
        self,
        list_files: List[File],
        ignore_airflow_env: bool = False,
        burn_after_sending: bool = False,
    ):
        """Send list of file to Minio bucket

        Args:
            list_files (List[File]): List of Dictionnaries containing for each
            `source_path` and `source_name` : local file location ;
            `dest_path` and `dest_name` : minio location (inside bucket specified) ;

        Raises:
            Exception: when specified local file does not exists
            Exception: when specified bucket does not exist
        """
        if self.bucket is None:
            raise AttributeError("A bucket has to be specified.")
        for file in list_files:
            is_file = os.path.isfile(
                os.path.join(file["source_path"], file["source_name"])
            )
            if is_file:
                if ignore_airflow_env:
                    dest_path = f"{file['dest_path']}{file['dest_name']}"
                else:
                    dest_path = f"{AIRFLOW_ENV}/{file['dest_path']}{file['dest_name']}"
                print("Sending " + file["source_path"] + file["source_name"])
                print(f"to {self.bucket}/{dest_path}")
                self.client.fput_object(
                    self.bucket,
                    dest_path,
                    os.path.join(file["source_path"], file["source_name"]),
                    content_type=file.get("content_type") or magic.from_file(
                        os.path.join(file["source_path"], file["source_name"]),
                        mime=True,
                    ),
                )
                if burn_after_sending:
                    os.remove(os.path.join(file["source_path"], file["source_name"]))
            else:
                raise Exception(
                    f"file {file['source_path']}{file['source_name']} "
                    "does not exists"
                )

    @simple_connection_retry
    def download_files(
        self,
        list_files: List[File],
        ignore_airflow_env=False,
    ):
        """Retrieve list of files from Minio

        Args:
            list_files (List[File]): List of Dictionnaries containing for each
            `source_path` and `source_name` : Minio location inside specified bucket ;
            `dest_path` and `dest_name` : local file destination ;

        Raises:
            Exception: _description_
        """
        if self.bucket is None:
            raise AttributeError("A bucket has to be specified.")
        for file in list_files:
            if ignore_airflow_env:
                source_path = f"{file['source_path']}{file['source_name']}"
            else:
                source_path = (
                    f"{AIRFLOW_ENV}/{file['source_path']}{file['source_name']}"
                )
            self.client.fget_object(
                self.bucket,
                source_path,
                f"{file['dest_path']}{file['dest_name']}",
            )

    @simple_connection_retry
    def get_file_content(
        self,
        file_path,
        encoding="utf-8",
    ):
        """
        Return the content of a file as a string.
        """
        if self.bucket is None:
            raise AttributeError("A bucket has to be specified.")
        r = self.client.get_object(self.bucket, file_path)
        return r.read().decode(encoding)

    @simple_connection_retry
    def compare_files(
        self,
        file_path_1: str,
        file_path_2: str,
        file_name_1: str,
        file_name_2: str,
    ):
        """Compare two minio files

        Args:
            both path and name from files to compare

        """
        if self.bucket is None:
            raise AttributeError("A bucket has to be specified.")
        s3 = boto3.client(
            "s3",
            endpoint_url=f"https://{self.url}",
            aws_access_key_id=self.user,
            aws_secret_access_key=self.password,
        )

        try:
            print(f"{AIRFLOW_ENV}/{file_path_1}{file_name_1}")
            print(f"{AIRFLOW_ENV}/{file_path_2}{file_name_2}")
            file_1 = s3.head_object(
                Bucket=self.bucket, Key=f"{AIRFLOW_ENV}/{file_path_1}{file_name_1}"
            )
            file_2 = s3.head_object(
                Bucket=self.bucket, Key=f"{AIRFLOW_ENV}/{file_path_2}{file_name_2}"
            )
            print(f"Hash file 1 : {file_1['ETag']}")
            print(f"Hash file 2 : {file_2['ETag']}")
            print(bool(file_1["ETag"] == file_2["ETag"]))

            return bool(file_1["ETag"] == file_2["ETag"])

        except botocore.exceptions.ClientError as e:
            print("Error loading files:", e)
            return None

    @simple_connection_retry
    def get_files_from_prefix(
        self,
        prefix: str,
        ignore_airflow_env=False,
        recursive=False,
    ):
        """Retrieve only the list of files in a Minio pattern

        Args:
            prefix: (str): prefix to search files

        Raises:
            Exception: _description_
        """
        if self.bucket is None:
            raise AttributeError("A bucket has to be specified.")
        list_objects = []
        if not ignore_airflow_env:
            prefix = f"{AIRFLOW_ENV}/{prefix}"
        objects = self.client.list_objects(
            self.bucket, prefix=prefix, recursive=recursive
        )
        for obj in objects:
            # print(obj.object_name)
            list_objects.append(obj.object_name.replace(f"{AIRFLOW_ENV}/", ""))
        return list_objects

    @simple_connection_retry
    def copy_object(
        self,
        path_source: str,
        path_target: str,
        minio_bucket_source: Optional[str] = None,
        minio_bucket_target: Optional[str] = None,
        remove_source_file: bool = False,
    ) -> None:
        """Copy and paste file to another folder.

        Args:
            path_source (str): Path of source file
            path_target (str): Path of target file
            minio_bucket_source (str, optional): Source bucket. Defaults to the bucket specified at init.
            minio_bucket_target (str, optional): Target bucket. Defaults to the bucket specified at init.
            remove_source_file: (bool): Remove or not the source file after the copy. Default is False

        Raises:
            Exception: _description_
        """
        if not minio_bucket_source:
            minio_bucket_source = self.bucket
        if not minio_bucket_target:
            minio_bucket_target = self.bucket

        if (
            self.client.bucket_exists(minio_bucket_source)
            and self.client.bucket_exists(minio_bucket_target)
        ):
            # copy an object from a bucket to another.
            logging.info(
                f"{'Move' if remove_source_file else 'Copy'} {minio_bucket_source}/{AIRFLOW_ENV}/{path_source}"
            )
            self.client.copy_object(
                minio_bucket_source,
                f"{AIRFLOW_ENV}/{path_target}",
                CopySource(minio_bucket_target, f"{AIRFLOW_ENV}/{path_source}"),
            )
            if remove_source_file:
                self.client.remove_object(
                    minio_bucket_source, f"{AIRFLOW_ENV}/{path_source}"
                )
            logging.info(f"> to {minio_bucket_source}/{AIRFLOW_ENV}/{path_target}")
        else:
            raise ValueError(
                f"One bucket does not exist: {minio_bucket_source} or {minio_bucket_target}"
            )

    def copy_many_objects(
        self,
        obj_source_paths: list[str],
        target_directory: str,
        minio_bucket_source: Optional[str] = None,
        minio_bucket_target: Optional[str] = None,
        remove_source_file: bool = False,
    ) -> list[str]:
        """
        Copy multiple objects from a source folder to a target folder in MinIO.

        Args:
            obj_source_paths (list[str]): List of the objects full paths to be copied.
            target_path (str): The target directory where the objects will be copied.
            minio_bucket_source (Optional[str]): The source MinIO bucket name. Defaults to the bucket specified at init.
            minio_bucket_target (Optional[str]): The target MinIO bucket name. Defaults to the bucket specified at init.
            remove_source_file (bool): If True, removes the source files after copying. Defaults to False.

        Returns:
            list[str]: List of the new objects paths.
        """
        files_destination_path = []
        for source_path in obj_source_paths:
            source_file = os.path.basename(source_path)
            target_path = target_directory + source_file
            self.copy_object(
                path_source=source_path,
                path_target=target_path,
                minio_bucket_source=minio_bucket_source,
                minio_bucket_target=minio_bucket_target,
                remove_source_file=remove_source_file,
            )
            files_destination_path.append(target_path)
        return files_destination_path

    @simple_connection_retry
    def get_all_files_names_and_sizes_from_parent_folder(
        self,
        folder: str,
    ):
        """
        returns a dict of {"file_name": file_size, ...} for all files in the folder
        """
        if self.bucket is None:
            raise AttributeError("A bucket has to be specified.")
        objects = {
            o.object_name: o
            for o in self.client.list_objects(self.bucket, prefix=folder)
        }
        files = {k: v.size for k, v in objects.items() if "." in k}
        subfolders = [k for k in objects.keys() if k not in files.keys()]
        for subf in subfolders:
            files.update(
                self.get_all_files_names_and_sizes_from_parent_folder(
                    folder=subf,
                )
            )
        return files

    @simple_connection_retry
    def delete_file(
        self,
        file_path: str,
    ):
        """/!\ USE WITH CAUTION"""
        if self.bucket is None:
            raise AttributeError("A bucket has to be specified.")
        try:
            self.client.stat_object(self.bucket, file_path)
            self.client.remove_object(self.bucket, file_path)
            print(f"File '{file_path}' deleted successfully.")
        except S3Error as e:
            print(e)

    @simple_connection_retry
    def dict_to_bytes_to_minio(
        self,
        dict_top,
        name,
    ):
        if self.bucket is None:
            raise AttributeError("A bucket has to be specified.")
        raw_data = io.BytesIO(json.dumps(dict_top, indent=2).encode("utf-8"))
        self.client.put_object(
            bucket_name=self.bucket,
            object_name=name,
            length=raw_data.getbuffer().nbytes,
            data=raw_data,
        )

    def get_file_url(
        self,
        file_path,
        ignore_airflow_env=False,
    ):
        return (
            f"https://{MINIO_URL}/{self.bucket}/"
            f"{AIRFLOW_ENV + '/' if not ignore_airflow_env else ''}"
            f"{file_path}"
        )
