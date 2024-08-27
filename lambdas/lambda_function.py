import json
import logging
import os
import boto3
import uuid
from botocore.exceptions import ClientError
from http import HTTPStatus
from typing import Dict, Any, Optional
from pydantic import BaseModel, TypeAdapter, ValidationError, Field
from .s3_adapter import S3Adapter, body_as_dict
from common.decorator import lambda_handler

# Custom exceptions
class FeedbackError(Exception):
    pass

class QuestionIdError(FileNotFoundError):
    pass

# Logger setup
logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

# Pydantic model for feedback validation
class Feedback(BaseModel):
    helpful: bool = Field(strict=True)

# Validate feedback data using Pydantic
def validate_feedback(feedback_data: dict) -> Feedback:
    logger.info(f"Validating feedback: {feedback_data}")
    return TypeAdapter(Feedback).validate_python(feedback_data)

# Generate unique feedback UUID
def generate_feedback_uuid() -> str:
    return str(uuid.uuid4())

# Save feedback data to S3
def save_feedback_to_s3(
    s3_adapter: S3Adapter, s3_bucket: str, s3_key: str, feedback_data: Dict[str, Any]
) -> None:
    try:
        logger.info(f"Saving feedback to S3: bucket={s3_bucket}, key={s3_key}")
        s3_adapter.try_save_object(bucket_name=s3_bucket, key=s3_key, body=feedback_data)
        logger.info("Feedback saved to S3 successfully")
    except ClientError as e:
        logger.error(f"Error saving feedback to S3: {e}")
        raise FeedbackError("Error saving feedback to S3") from e

# Fetch existing question data from S3
def fetch_existing_data(
    s3_adapter: S3Adapter, s3_bucket: str, s3_key: str
) -> Dict[str, Any]:
    logger.info(f"Fetching existing data from S3 with key: {s3_key}")
    try:
        existing_data = s3_adapter.try_get_object(s3_bucket, s3_key)
        return body_as_dict(existing_data)
    except ClientError as e:
        logger.error(f"Error fetching data from S3 for key {s3_key}: {e}")
        raise QuestionIdError(f"Data for key {s3_key} not found in S3.") from e

# Main Lambda handler builder function
def build_handler(s3_adapter: S3Adapter) -> Any:
    s3_bucket = os.environ.get("BUCKET_NAME")
    feedback_prefix = os.environ.get("FEEDBACK_PREFIX", "")
    question_prefix = os.environ.get("QUESTION_PREFIX", "")

    @lambda_handler(
        error_status=(
            (QuestionIdError, HTTPStatus.NOT_FOUND.value),
            (FeedbackError, HTTPStatus.BAD_REQUEST.value),
            (ValidationError, HTTPStatus.BAD_REQUEST.value),
        ),
        logging_fn=logger.error,
    )
    def handler(event: Dict[str, Any], context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        logger.info(f"Received event: {json.dumps(event)}")

        # Extract questionId from pathParameters
        question_id: str = event["pathParameters"].get("questionId")
        if not question_id:
            logger.error("questionId is missing from pathParameters.")
            raise QuestionIdError("questionId is missing from pathParameters.")

        # Generate feedback UUID
        feedback_uuid = generate_feedback_uuid()

        # Retrieve existing question data from S3
        question_s3_key = f"{question_prefix}/{question_id}.json"
        logger.info(f"Getting question data from S3 with key: {question_s3_key}")
        dict_data = fetch_existing_data(s3_adapter, s3_bucket, question_s3_key)

        # Validate feedback from the event body
        feedback_data = event.get("body", {})
        logger.info(f"Feedback data after loading JSON: {feedback_data}")

        try:
            feedback = validate_feedback(feedback_data)
            logger.info(f"Feedback successfully validated: {feedback}")
        except ValidationError as e:
            logger.error(f"Validation error: {e}")
            raise e

        # Add feedback to existing question data
        dict_data["feedback"] = feedback.dict()

        # Save feedback with question data to S3
        feedback_s3_key = f"{feedback_prefix}/feedback_{feedback_uuid}_{question_id}.json"
        logger.info(f"Saving feedback with question data to S3 with key: {feedback_s3_key}")
        save_feedback_to_s3(s3_adapter, s3_bucket, feedback_s3_key, dict_data)

        return {
            "statusCode": HTTPStatus.OK.value,
            "body": json.dumps({"message": f"Feedback for questionId {question_id} saved successfully."}),
        }

    return handler

# Initialize the Lambda handler if not in test mode
if not bool(os.environ.get("TEST_FLAG", False)):
    region = os.environ.get("AWS_REGION", "ca-central-1")
    s3_client = boto3.client("s3", region_name=region)
    s3_adapter = S3Adapter(s3_client)
    handler = build_handler(s3_adapter)
