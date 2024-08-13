import os
import json
import pytest
from unittest.mock import patch, MagicMock
from lambda_function import lambda_handler, save_feedback_to_s3
from botocore.exceptions import ClientError


# Fixture to mock environment variables without hardcoding
@pytest.fixture
def mock_env():
    bucket_name = "test_bucket_name"
    with patch.dict(os.environ, {"BUCKET_NAME": bucket_name}):
        yield bucket_name

# Fixture to mock the S3 client
@pytest.fixture
def s3_client_mock():
    with patch('boto3.client') as mock:
        s3 = mock.return_value
        yield s3

# Test successful lambda execution with valid inputs
def test_lambda_handler_success(mock_env, s3_client_mock):
    # Mocking the S3 put_object call to simulate success
    s3_client_mock.put_object.return_value = {}

    # Creating a mock event with questionId in pathParameters and feedback in body
    event = {
        "pathParameters": {"questionId": "123"},
        "body": json.dumps({
            "feedback": 1
        })
    }

    # Invoking the Lambda handler
    response = lambda_handler(event, None)

    # Assertions to verify correct response and S3 interactions
    assert response['statusCode'] == 200
    assert json.loads(response['body'])['message'] == 'Feedback for questionId 123 saved successfully.'
    s3_client_mock.put_object.assert_called_once_with(
        Bucket=mock_env,
        Key="feedback/question_123.json",
        Body=json.dumps({"questionId": "123", "feedback": 1})
    )

# Test lambda handling when feedback is missing
def test_lambda_handler_missing_feedback(mock_env, s3_client_mock):
    event = {
        "pathParameters": {"questionId": "123"},
        "body": json.dumps({})
    }

    response = lambda_handler(event, None)

    assert response['statusCode'] == 400
    assert json.loads(response['body']) == 'Invalid feedback value: Must be an integer 0 or 1'

# Test lambda handling when feedback has an invalid value
def test_lambda_handler_invalid_feedback_value(mock_env, s3_client_mock):
    event = {
        "pathParameters": {"questionId": "123"},
        "body": json.dumps({
            "feedback": 2  # Invalid feedback value
        })
    }

    response = lambda_handler(event, None)

    assert response['statusCode'] == 400
    assert json.loads(response['body']) == 'Invalid feedback value: Must be an integer 0 or 1'

# Test lambda handling when S3 upload fails
def test_lambda_handler_s3_upload_failure(mock_env, s3_client_mock):
    # Mocking an S3 upload failure
    s3_client_mock.put_object.side_effect = ClientError(
        error_response={"Error": {"Code": "500", "Message": "S3 upload failed"}},
        operation_name="PutObject"
    )

    event = {
        "pathParameters": {"questionId": "123"},
        "body": json.dumps({
            "feedback": 1
        })
    }

    response = lambda_handler(event, None)

    assert response['statusCode'] == 502
    assert json.loads(response['body']) == 'Failed to save feedback to S3 due to client error'
    s3_client_mock.put_object.assert_called_once_with(
        Bucket=mock_env,
        Key="feedback/question_123.json",
        Body=json.dumps({"questionId": "123", "feedback": 1})
    )
