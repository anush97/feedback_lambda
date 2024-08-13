import os
import json
import pytest
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from lambda_function import build_handler, save_feedback_to_s3

# Sample event for testing
sample_event = {
    'pathParameters': {'questionId': '12345'},
    'body': json.dumps({'feedback': 1})
}

@pytest.fixture
def mock_env():
    """Fixture to mock the AWS environment variables."""
    bucket_name = "test_bucket_name"
    with patch.dict(os.environ, {"BUCKET_NAME": bucket_name}):
        yield bucket_name

@pytest.fixture
def lambda_handler(mock_env):
    """Fixture to create a new lambda handler instance for each test"""
    return build_handler()

@patch('lambda_function.s3')
def test_save_feedback_to_s3_success(mock_s3, mock_env):
    """Test if feedback is saved successfully to S3"""
    mock_s3.put_object.return_value = {}
    try:
        save_feedback_to_s3(mock_env, 'feedback/question_12345.json', '{"feedback": 1}')
    except Exception:
        pytest.fail("save_feedback_to_s3 raised an exception unexpectedly!")

@patch('lambda_function.s3')
def test_save_feedback_to_s3_client_error(mock_s3, mock_env):
    """Test handling of S3 ClientError in save_feedback_to_s3"""
    mock_s3.put_object.side_effect = ClientError({"Error": {"Code": "500", "Message": "Internal Error"}}, 'PutObject')
    with pytest.raises(ClientError):
        save_feedback_to_s3(mock_env, 'feedback/question_12345.json', '{"feedback": 1}')

@patch('lambda_function.s3')
def test_lambda_handler_success(mock_s3, lambda_handler, mock_env):
    """Test successful handling of a valid event"""
    mock_s3.put_object.return_value = {}
    response = lambda_handler(sample_event, None)
    assert response['statusCode'] == 200
    assert json.loads(response['body'])['message'] == 'Feedback for questionId 12345 saved successfully.'
    assert json.loads(response['body'])['s3_key'] == 'feedback/question_12345.json'

@patch('lambda_function.s3')
def test_lambda_handler_client_error(mock_s3, lambda_handler, mock_env):
    """Test handling of S3 ClientError in the lambda handler"""
    mock_s3.put_object.side_effect = ClientError({"Error": {"Code": "500", "Message": "Internal Error"}}, 'PutObject')
    response = lambda_handler(sample_event, None)
    assert response['statusCode'] == 502
    assert json.loads(response['body']) == 'Failed to save feedback to S3 due to client error'

@patch('lambda_function.s3')
def test_lambda_handler_unexpected_error(mock_s3, lambda_handler, mock_env):
    """Test handling of unexpected exceptions in the lambda handler"""
    mock_s3.put_object.side_effect = Exception("Unexpected error")
    response = lambda_handler(sample_event, None)
    assert response['statusCode'] == 500
    assert json.loads(response['body']) == 'Internal server error'
