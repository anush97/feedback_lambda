import os
import json
import pytest
import boto3
from http import HTTPStatus
from moto import mock_aws
from unittest.mock import patch
from lambdas.feedback_sender_POST.s3_adapter import S3Adapter
from lambdas.feedback_sender_POST.feedback_sender_POST_handler import (
    build_handler,
    FeedbackError,
    QuestionIdError,
)
from botocore.exceptions import ClientError

TEST_BUCKET_NAME = "test-bucket"
TEST_PREFIX = "feedback"
QUESTION_PREFIX = "question"

@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"

@pytest.fixture
def s3_client(aws_credentials):
    """Mocked S3 client using moto."""
    with mock_aws():
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=TEST_BUCKET_NAME)
        yield s3_client
        clean_bucket(s3_client)
        s3_client.delete_bucket(Bucket=TEST_BUCKET_NAME)

def clean_bucket(s3_client):
    """Clean up all objects from the test S3 bucket."""
    response = s3_client.list_objects_v2(Bucket=TEST_BUCKET_NAME)
    files = response.get("Contents", [])
    if files:
        files_to_delete = [{"Key": file["Key"]} for file in files]
        s3_client.delete_objects(Bucket=TEST_BUCKET_NAME, Delete={"Objects": files_to_delete})

@pytest.fixture
def mock_env():
    """Mock environment variables."""
    with patch.dict(
        os.environ,
        {
            "BUCKET_NAME": TEST_BUCKET_NAME,
            "LOG_LEVEL": "INFO",
            "FEEDBACK_PREFIX": TEST_PREFIX,
            "QUESTION_PREFIX": QUESTION_PREFIX,
        },
    ):
        yield

@pytest.fixture
def s3_adapter(s3_client):
    """Fixture to create an S3Adapter."""
    return S3Adapter(s3_client)

@pytest.fixture
def handler(mock_env, s3_adapter):
    """Fixture to build the Lambda handler."""
    return build_handler(s3_adapter)

def test_lambda_handler_success(handler, s3_client):
    """Test that valid feedback is successfully saved."""
    question_id = "12345"
    initial_data = {"question": "What is the capital of France?", "answer": "Paris"}

    # Put mock data into S3
    s3_client.put_object(
        Bucket=TEST_BUCKET_NAME,
        Key=f"{QUESTION_PREFIX}/{question_id}.json",
        Body=json.dumps(initial_data),
    )

    with patch(
        "lambdas.feedback_sender_POST.feedback_sender_POST_handler.generate_feedback_uuid",
        return_value="mocked-uuid",
    ):
        event = {
            "pathParameters": {"questionId": question_id},
            "body": json.dumps({"helpful": True}),  # Valid feedback
        }

        # Call handler
        response = handler(event, None)
        assert response["statusCode"] == HTTPStatus.OK.value

        response_body = json.loads(response["body"])
        assert response_body["message"] == f"Feedback for questionId {question_id} saved successfully."

        # Check the saved feedback in S3
        saved_object = s3_client.get_object(
            Bucket=TEST_BUCKET_NAME,
            Key=f"{TEST_PREFIX}/feedback_mocked-uuid_{question_id}.json",
        )
        saved_feedback = json.loads(saved_object["Body"].read().decode("utf-8"))

        # Assert saved feedback
        assert saved_feedback["feedback"] == {"helpful": True}

def test_lambda_handler_invalid_feedback(handler, s3_client):
    """Test that invalid feedback data results in a validation error response."""
    question_id = "12345"
    initial_data = {"question": "What is the capital of France?", "answer": "Paris"}

    # Put mock data into S3
    s3_client.put_object(
        Bucket=TEST_BUCKET_NAME,
        Key=f"{QUESTION_PREFIX}/{question_id}.json",
        Body=json.dumps(initial_data),
    )

    # Invalid feedback event (non-boolean value)
    invalid_event = {
        "pathParameters": {"questionId": question_id},
        "body": json.dumps({"helpful": "yes"}),  # Invalid feedback
    }

    # Call handler and capture response
    response = handler(invalid_event, None)

    # Assert validation error response
    assert response["statusCode"] == HTTPStatus.BAD_REQUEST.value
    response_body = json.loads(response["body"])
    assert "errorMessage" in response_body
    assert "1 validation error for Feedback" in response_body["errorMessage"]

def test_lambda_handler_missing_question_id(handler):
    """Test that missing questionId raises an error."""
    event = {"pathParameters": {}, "body": json.dumps({"helpful": True})}

    # Call handler and capture response
    response = handler(event, None)

    # Assert questionId error response
    assert response["statusCode"] == HTTPStatus.NOT_FOUND.value
    assert "errorMessage" in json.loads(response["body"])

def test_lambda_handler_question_id_not_found(handler, s3_adapter):
    """Test that a missing questionId in S3 raises an error."""
    event = {
        "pathParameters": {"questionId": "99999"},
        "body": json.dumps({"helpful": True}),
    }
    error_response = {
        "Error": {"Code": "NoSuchKey", "Message": "The specified key does not exist."}
    }

    # Mock S3 to simulate missing object
    with patch.object(
        s3_adapter,
        "try_get_object",
        side_effect=ClientError(error_response, "GetObject"),
    ):
        response = handler(event, None)

        # Assert questionId not found response
        assert response["statusCode"] == HTTPStatus.NOT_FOUND.value
        assert "errorMessage" in json.loads(response["body"])

def test_save_feedback_to_s3_feedback_error(handler, s3_client, s3_adapter):
    """Test that an error during S3 save raises a FeedbackError."""
    question_id = "12345"
    initial_data = {"question": "What is the capital of France?", "answer": "Paris"}

    # Put mock data into S3
    s3_client.put_object(
        Bucket=TEST_BUCKET_NAME,
        Key=f"{QUESTION_PREFIX}/{question_id}.json",
        Body=json.dumps(initial_data),
    )

    # Mock S3 put_object failure
    with patch.object(
        s3_adapter,
        "try_save_object",
        side_effect=ClientError(
            error_response={
                "Error": {
                    "Code": str(HTTPStatus.INTERNAL_SERVER_ERROR.value),
                    "Message": "Internal Server Error",
                }
            },
            operation_name="PutObject",
        ),
    ):
        event = {
            "pathParameters": {"questionId": question_id},
            "body": json.dumps({"helpful": True}),
        }

        response = handler(event, None)

        # Assert feedback error response
        assert response["statusCode"] == HTTPStatus.BAD_REQUEST.value
        assert "errorMessage" in json.loads(response["body"])
