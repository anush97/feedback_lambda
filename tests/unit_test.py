import os
import json
import pytest
import boto3
from http import HTTPStatus
from moto import mock_s3
from unittest.mock import patch
from lambdas.feedback_sender_POST.s3_adapter import S3Adapter
from lambdas.feedback_sender_POST.feedback_sender_POST import (
    build_handler,
    FeedbackError,
    QuestionIdError,
    generate_feedback_uuid  
)
from botocore.exceptions import ClientError

TEST_BUCKET_NAME = "test-bucket"
TEST_PREFIX = "feedback"
QUESTION_PREFIX = "question"


@pytest.fixture
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"


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
    s3_client.delete_objects(
        Bucket=TEST_BUCKET_NAME, Delete={"Objects": files_to_delete}
    )


@pytest.fixture
def mock_env():
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
    return S3Adapter(s3_client)


@pytest.fixture
def lambda_handler(mock_env, s3_adapter):
    return build_handler(s3_adapter)


def test_lambda_handler_success(lambda_handler, s3_client):
    # Set up initial question data
    question_id = "12345"
    initial_feedback = {"helpful": True}
    s3_client.put_object(
        Bucket=TEST_BUCKET_NAME,
        Key=f"{QUESTION_PREFIX}/{question_id}.json",
        Body=json.dumps({"feedback": initial_feedback}),
    )

    # Generate a UUID for the feedback
    feedback_uuid = generate_feedback_uuid()

    event = {
        "pathParameters": {"questionId": question_id},
        "body": json.dumps({"feedback": {"helpful": True}}),
    }

    response = lambda_handler(event, None)

    # Check that the response is OK and that the feedback was saved successfully
    assert response["statusCode"] == HTTPStatus.OK.value
    assert (
        json.loads(response["body"])["message"]
        == f"Feedback for questionId {question_id} saved successfully."
    )

    # Validate that the feedback was stored in the S3 bucket with the correct key
    saved_object = s3_client.get_object(
        Bucket=TEST_BUCKET_NAME, Key=f"{TEST_PREFIX}/feedback_{feedback_uuid}_{question_id}.json"
    )
    saved_feedback = json.loads(saved_object["Body"].read().decode("utf-8"))

    # Assert that the feedback saved matches the expected feedback
    assert saved_feedback["feedback"]["helpful"] is True


def test_lambda_handler_missing_question_id(lambda_handler):
    event = {"pathParameters": {}, "body": json.dumps({"feedback": {"helpful": True}})}

    with pytest.raises(
        QuestionIdError, match="questionId is missing from pathParameters."
    ):
        lambda_handler(event, None)


def test_lambda_handler_question_id_not_found(lambda_handler, s3_adapter):
    event = {
        "pathParameters": {"questionId": "99999"},
        "body": json.dumps({"feedback": {"helpful": True}}),
    }
    error_response = {
        "Error": {"Code": "NoSuchKey", "Message": "The specified key does not exist."}
    }
    with patch.object(
        s3_adapter,
        "try_get_object",
        side_effect=ClientError(error_response, "GetObject"),
    ):
        with pytest.raises(QuestionIdError, match="questionId 99999 not found in S3."):
            lambda_handler(event, None)


def test_lambda_handler_invalid_feedback(lambda_handler, s3_client):
    question_id = "12345"
    s3_client.put_object(
        Bucket=TEST_BUCKET_NAME,
        Key=f"{QUESTION_PREFIX}/{question_id}.json",
        Body=json.dumps({"feedback": {"helpful": True}}),
    )

    invalid_event = {
        "pathParameters": {"questionId": question_id},
        "body": json.dumps({"feedback": {"helpful": "yes"}}),
    }

    with pytest.raises(
        ValueError, match="Invalid feedback value: Must be a boolean True or False"
    ):
        lambda_handler(invalid_event, None)


def test_save_feedback_to_s3_feedback_error(lambda_handler, s3_client, s3_adapter):
    question_id = "12345"
    s3_client.put_object(
        Bucket=TEST_BUCKET_NAME,
        Key=f"{QUESTION_PREFIX}/{question_id}.json",
        Body=json.dumps({"feedback": {"helpful": True}}),
    )

    feedback_uuid = generate_feedback_uuid()

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
            "body": json.dumps({"feedback": {"helpful": True}}),
        }

        with pytest.raises(FeedbackError, match="Error saving feedback to S3"):
            lambda_handler(event, None)
