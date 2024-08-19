import concurrent
import json
import logging
from concurrent.futures._base import as_completed
from typing import Dict

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger()


def _extract_future(task, timeout=1):
    try:
        return task.result(timeout)
    except concurrent.futures.TimeoutError as t:
        logger.exception("Timeout error while processing task", t)
        return None


def body_as_string(response) -> str:
    return response["Body"].read().decode("utf-8")


def body_as_dict(response: str) -> Dict:
    file_content = body_as_string(response)
    if not file_content:
        return {}

    return json.loads(file_content)


def create_s3_client(credentials=None):
    config = Config(
        region_name="ca-central-1",
        signature_version="s3v4",
        retries={"max_attempts": 10},
        s3={"addressing_style": "path"},
    )
    if credentials:
        return boto3.client(
            "s3",
            aws_access_key_id=credentials.get("id_access_token"),
            aws_secret_access_key=credentials.get("id_secret_key"),
            aws_session_token=credentials.get("id_session_token"),
            config=config,
        )
    else:
        return boto3.client(
            "s3",
            config=config,
        )


class S3Adapter:
    generic_key_error_msg = "No such Object Found"
    generic_version_error_msg = "Object with given version does not exist"
    generic_bucket_error_msg = "Bucket does not exist"
    generic_permission_error_msg = "Access denied"

    def __init__(
        self,
        s3_client,
        no_such_key_msg=generic_key_error_msg,
        no_such_bucket_msg=generic_version_error_msg,
        no_such_version_msg=generic_bucket_error_msg,
        access_denied_msg=generic_permission_error_msg,
    ):
        self.s3_client = s3_client
        self.no_such_key_msg = no_such_key_msg
        self.no_such_bucket_msg = no_such_bucket_msg
        self.no_such_version_msg = no_such_version_msg
        self.access_denied_msg = access_denied_msg

    def try_get_object(self, bucket_name, key, version_id=None):
        try:
            if version_id is None:
                return self.s3_client.get_object(Bucket=bucket_name, Key=key)
            else:
                return self.s3_client.get_object(
                    Bucket=bucket_name, Key=key, VersionId=version_id
                )
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise FileNotFoundError(self.no_such_key_msg) from e
            elif e.response["Error"]["Code"] == "NoSuchBucket":
                raise FileNotFoundError(self.no_such_bucket_msg) from e
            elif e.response["Error"]["Code"] == "NoSuchVersion":
                raise FileNotFoundError(self.no_such_version_msg) from e
            else:
                raise e

    def try_list_objects(self, bucket_name, prefix):
        try:
            return self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        except self.s3_client.exceptions.NoSuchBucket as e:
            logger.exception("%s is not configured", bucket_name)
            raise e

    def try_list_object_keys(self, bucket_name, prefix):
        list_objects = self.try_list_objects(bucket_name=bucket_name, prefix=prefix)
        contents = list_objects["Contents"] if "Contents" in list_objects else []
        return [f["Key"] for f in contents]

    def head_object(self, bucket_name, key):
        try:
            return self.s3_client.head_object(Bucket=bucket_name, Key=key)
        except ClientError as e:
            status_code = e.response["ResponseMetadata"]["HTTPStatusCode"]
            if status_code == 404:
                raise FileNotFoundError(self.generic_key_error_msg)
            raise e

    def execute_for_each_key(self, keys, bucket_name, task, callback, threads_count=10):
        with concurrent.futures.ThreadPoolExecutor(threads_count) as executor:
            tasks = [executor.submit(task, bucket_name, k) for k in keys]
            collector = [
                callback(_extract_future(t))
                for t in as_completed(tasks)
                if t is not None
            ]

        return collector

    # body and metadata must be dicts objects
    def try_save_object(self, bucket_name, key, body, metadata=None):
        result = self.s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=json.dumps(body),
            ContentType="application/json",
            Metadata=metadata if metadata else {},
        )
        return result

    def try_delete_object(self, bucket_name, key):
        try:
            self.s3_client.delete_object(Bucket=bucket_name, Key=key)
        except ClientError as e:
            status_code = e.response["ResponseMetadata"]["HTTPStatusCode"]
            if status_code == 404:
                raise FileNotFoundError(self.generic_key_error_msg)

    def try_get_metadata_object(self, bucket_name, key):
        response = self.head_object(bucket_name=bucket_name, key=key)
        return response.get("Metadata", "")