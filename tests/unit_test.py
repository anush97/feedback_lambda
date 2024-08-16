import os
import json
import pytest
from unittest.mock import patch, MagicMock
from http import HTTPStatus
from lambdas.feedback.feedback_sender_POST import build_handler, FeedbackError
from common.s3.adapter import S3Adapter
from botocore.exceptions import ClientError


# Sample event data
sample_event = {
    "pathParameters": {"questionId": "12345"},
    "body": json.dumps({"feedback": True}),
}


@pytest.fixture
def mock_env():
    bucket_name = "test_bucket_name"
    with patch.dict(os.environ, values={"BUCKET_NAME": bucket_name}):
        yield bucket_name


@pytest.fixture
def mock_s3():
    with patch("common.s3.adapter.S3Adapter") as mock:
        yield mock


@pytest.fixture
def lambda_handler(mock_env, mock_s3):
    mock_s3_adapter_instance = MagicMock()
    mock_s3.return_value = mock_s3_adapter_instance
    return build_handler(mock_s3_adapter_instance)


@pytest.fixture
def s3_response():
    return {
        "ResponseMetadata": {
            "RequestId": "1234567890",
            "HostId": "host123",
            "HTTPStatusCode": 200,
            "HTTPHeaders": {
                "x-amz-id-2": "id-2",
                "x-amz-request-id": "request-id",
                "date": "date",
                "content-length": "Length",
                "content-type": "type",
            },
            "RetryAttempts": 0,
        }
    }


# Test if mock S3 env is as expected
def test_s3_response_fixture(s3_response, mock_s3):
    mock_s3_adapter_instance = MagicMock()
    mock_s3.return_value = mock_s3_adapter_instance
    mock_s3_adapter_instance.try_save_object.return_value = s3_response

    response = mock_s3_adapter_instance.try_save_object()
    assert response == s3_response


# Test success of feedback storage in S3
def test_lambda_handler_success(lambda_handler, mock_s3, mock_env, s3_response):
    mock_s3_adapter_instance = mock_s3.return_value
    mock_s3_adapter_instance.try_save_object.return_value = s3_response

    # Call Lambda handler
    response = lambda_handler(sample_event, None)

    # Assertions
    assert response["statusCode"] == HTTPStatus.OK.value
    assert json.loads(response["body"])["message"] == "Feedback for questionId 12345 saved successfully."

    # Assert that S3 was called with the correct parameters
    mock_s3_adapter_instance.try_save_object.assert_called_with(
        bucket_name=mock_env,
        key="feedback/question_12345.json",
        body=json.dumps({"questionId": "12345", "feedback": True})
    )

    # Assert the saved feedback is correct
    stored_feedback = mock_s3_adapter_instance.try_save_object.call_args[1]["body"]
    assert json.loads(stored_feedback) == {"questionId": "12345", "feedback": True}


# Test failure of feedback storage in S3
def test_lambda_handler_failure(lambda_handler, mock_s3, mock_env):
    # Simulate ClientError being raised by boto3 S3 client
    mock_s3_adapter_instance = mock_s3.return_value
    mock_s3_adapter_instance.try_save_object.side_effect = ClientError(
        error_response={"Error": {"Code": "500", "Message": "Internal Server Error"}},
        operation_name="PutObject"
    )

    # Call Lambda handler
    with pytest.raises(FeedbackError, match="Error saving feedback to S3"):
        lambda_handler(sample_event, None)

    # Assert S3 was called before the error was raised
    mock_s3_adapter_instance.try_save_object.assert_called_with(
        bucket_name=mock_env,
        key="feedback/question_12345.json",
        body=json.dumps({"questionId": "12345", "feedback": True})
    )
