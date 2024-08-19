import os
import json
import pytest
from moto import mock_s3
from unittest.mock import patch
import boto3
from http import HTTPStatus
from lambda_function import build_handler, FeedbackError
from s3_adapter import S3Adapter


TEST_BUCKET_NAME = "test-bucket"
TEST_PREFIX = "feedback"


@pytest.fixture
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture
def s3_client(aws_credentials):
    with mock_s3():
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=TEST_BUCKET_NAME)
        yield s3_client
        clean_bucket(s3_client)
        s3_client.delete_bucket(Bucket=TEST_BUCKET_NAME)


def clean_bucket(s3_client):
    response = s3_client.list_objects_v2(Bucket=TEST_BUCKET_NAME)
    files = response.get("Contents", [])
    if not files:
        return
    files_to_delete = [{"Key": file["Key"]} for file in files]
    s3_client.delete_objects(Bucket=TEST_BUCKET_NAME, Delete={"Objects": files_to_delete})


@pytest.fixture
def mock_env():
    with patch.dict(os.environ, {
        "BUCKET_NAME": TEST_BUCKET_NAME,
        "LOG_LEVEL": "INFO",
        "PREFIX": TEST_PREFIX
    }):
        yield


@pytest.fixture
def s3_adapter(s3_client):
    return S3Adapter(s3_client)


@pytest.fixture
def lambda_handler(mock_env, s3_adapter):
    return build_handler(s3_adapter)


def test_lambda_handler_success(lambda_handler, s3_client):
    initial_feedback = {"helpful": True}
    s3_client.put_object(
        Bucket=TEST_BUCKET_NAME,
        Key=f"{TEST_PREFIX}/12345.json",
        Body=json.dumps({"feedback": initial_feedback}),
    )
    
    event = {
        "pathParameters": {"questionId": "12345"},
        "body": json.dumps({"feedback": {"helpful": True}})
    }
    
    response = lambda_handler(event, None)
    
    assert response["statusCode"] == HTTPStatus.OK.value
    assert json.loads(response["body"])["message"] == "Feedback for questionId 12345 saved successfully."
    
    saved_object = s3_client.get_object(Bucket=TEST_BUCKET_NAME, Key=f"{TEST_PREFIX}/12345.json")
    saved_feedback = json.loads(saved_object["Body"].read().decode("utf-8"))
    
    assert saved_feedback["feedback"]["helpful"] is True


def test_lambda_handler_invalid_feedback(lambda_handler):
    invalid_event = {
        "pathParameters": {"questionId": "12345"},
        "body": json.dumps({"feedback": {"helpful": "yes"}})
    }
    
    with pytest.raises(ValueError, match="Invalid feedback value: Must be a boolean True or False"):
        lambda_handler(invalid_event, None)


def test_lambda_handler_s3_failure(lambda_handler, s3_adapter):
    with patch.object(s3_adapter, "try_save_object", side_effect=boto3.exceptions.S3UploadFailedError):
        event = {
            "pathParameters": {"questionId": "12345"},
            "body": json.dumps({"feedback": {"helpful": True}})
        }
        
        with pytest.raises(FeedbackError, match="Error saving feedback to S3"):
            lambda_handler(event, None)
