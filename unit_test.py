import os
import json
import logging
import boto3
import pytest
from http import HTTPStatus
from moto.s3 import mock_s3
from unittest.mock import patch
from s3_adapter import S3Adapter
from lambda_function import build_handler, FeedbackError


TEST_BUCKET_NAME = "test-bucket"
TEST_PREFIX = "feedback"


@pytest.fixture
def aws_credentials():
    """Mocked AWS credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture
def s3_client(aws_credentials):
    """Creates a mock S3 bucket using moto."""
    with mock_s3():
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=TEST_BUCKET_NAME)
        yield s3_client
        s3_client.delete_bucket(Bucket=TEST_BUCKET_NAME)


@pytest.fixture
def mock_env():
    """Mocks the environment variables."""
    with patch.dict(os.environ, {
        "BUCKET_NAME": TEST_BUCKET_NAME,
        "LOG_LEVEL": "INFO",
        "PREFIX": TEST_PREFIX
    }):
        yield


@pytest.fixture
def s3_adapter(s3_client):
    """Creates an instance of S3Adapter with the mocked S3 client."""
    return S3Adapter(s3_client)


@pytest.fixture
def lambda_handler(mock_env, s3_adapter):
    """Builds the lambda handler using the S3Adapter and mocked environment."""
    return build_handler(s3_adapter)


def test_lambda_handler_success(lambda_handler, s3_client):
    # Initial feedback to save in S3 bucket
    initial_feedback = {"feedback_sender_POST": {"helpful": True}}

    # Put an initial object in the mock S3 bucket
    s3_client.put_object(
        Bucket=TEST_BUCKET_NAME,
        Key=f"{TEST_PREFIX}/12345.json",
        Body=json.dumps(initial_feedback),
    )

    # Simulate event with correct data
    sample_event = {
        "pathParameters": {"questionId": "12345"},
        "body": json.dumps({"feedback_sender_POST": {"helpful": True}})
    }

    # Call the lambda handler
    response = lambda_handler(sample_event, None)

    # Validate the lambda's response
    assert response["statusCode"] == HTTPStatus.OK.value
    assert json.loads(response["body"])["message"] == "Feedback for questionId 12345 saved successfully."

    # Check if the feedback was saved correctly in the mock S3 bucket
    saved_object = s3_client.get_object(Bucket=TEST_BUCKET_NAME, Key=f"{TEST_PREFIX}/12345.json")
    saved_feedback = json.loads(saved_object["Body"].read().decode("utf-8"))

    assert saved_feedback["feedback_sender_POST"]["helpful"] is True


def test_lambda_handler_invalid_feedback(lambda_handler):
    # Event with invalid feedback (incorrect type for "helpful")
    invalid_event = {
        "pathParameters": {"questionId": "12345"},
        "body": json.dumps({"feedback_sender_POST": {"helpful": "yes"}})  # Incorrect type
    }

    # Ensure a ValueError is raised for invalid feedback
    with pytest.raises(ValueError, match="Invalid feedback_sender_POST value: Must be a boolean True or False"):
        lambda_handler(invalid_event, None)


def test_lambda_handler_s3_failure(lambda_handler, s3_adapter):
    # Mock the S3 adapter to simulate a failure during save
    with patch.object(s3_adapter, "try_save_object", side_effect=boto3.exceptions.S3UploadFailedError):
        sample_event = {
            "pathParameters": {"questionId": "12345"},
            "body": json.dumps({"feedback_sender_POST": {"helpful": True}})
        }

        # Ensure a FeedbackError is raised when the S3 save fails
        with pytest.raises(FeedbackError, match="Error saving feedback_sender_POST to S3"):
            lambda_handler(sample_event, None)
