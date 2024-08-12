import json
import pytest
from unittest.mock import patch, MagicMock
from lambda_function import lambda_handler, save_feedback_to_s3
import os

# Fixtures to mock environment variables and S3 client
@pytest.fixture
def mock_env():
    with patch.dict(os.environ, {"BUCKET_NAME": "userfeedbackbucket"}):
        yield

@pytest.fixture
def s3_client_mock():
    with patch('boto3.client') as mock:
        s3 = mock.return_value
        yield s3

# Test cases
def test_lambda_handler_success(mock_env, s3_client_mock):
    # Mocking the S3 put_object call
    s3_client_mock.put_object.return_value = {}

    # Creating a mock event
    event = {
        "body": json.dumps({
            "questionId": 123,
            "feedback": 1
        })
    }

    # Invoking the Lambda handler
    response = lambda_handler(event, None)

    # Assertions
    assert response['statusCode'] == 200
    assert json.loads(response['body'])['message'] == 'Feedback for questionId 123 saved successfully.'
    s3_client_mock.put_object.assert_called_once_with(
        Bucket="userfeedbackbucket",
        Key="feedback/question_123.json",
        Body=json.dumps({"questionId": 123, "feedback": 1})
    )

def test_lambda_handler_missing_feedback(mock_env, s3_client_mock):
    event = {
        "body": json.dumps({
            "questionId": 123
        })
    }

    response = lambda_handler(event, None)

    assert response['statusCode'] == 400
    assert json.loads(response['body']) == 'Invalid input: Missing questionId or feedback'

def test_lambda_handler_invalid_feedback_value(mock_env, s3_client_mock):
    event = {
        "body": json.dumps({
            "questionId": 123,
            "feedback": 2  # Invalid feedback value
        })
    }

    response = lambda_handler(event, None)

    assert response['statusCode'] == 400
    assert json.loads(response['body']) == 'Invalid feedback value: Must be an integer 0 or 1'

def test_lambda_handler_s3_upload_failure(mock_env, s3_client_mock):
    # Mocking an S3 upload failure
    s3_client_mock.put_object.side_effect = Exception("S3 upload failed")

    event = {
        "body": json.dumps({
            "questionId": 123,
            "feedback": 1
        })
    }

    response = lambda_handler(event, None)

    assert response['statusCode'] == 500
    assert json.loads(response['body']) == 'Internal server error'
    s3_client_mock.put_object.assert_called_once()

