import json
import logging
import os
import boto3
import uuid
from botocore.exceptions import ClientError
from http import HTTPStatus
from typing import Dict, Any, Optional
from pydantic import BaseModel, TypeAdapter, ValidationError
from .s3_adapter import S3Adapter, body_as_dict
from common.decorator import lambda_handler  # Assuming this decorator combines error handling and JSON loading

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
    helpful: bool

# Function to validate feedback data using Pydantic
def validate_feedback(feedback_data: dict) -> Feedback:
    logger.info(f"Validating feedback: {feedback_data}")
    return TypeAdapter(Feedback).validate_python(feedback_data)

# Main Lambda handler builder function
def build_handler(s3_adapter: S3Adapter) -> Any:
    s3_bucket: Optional[str] = os.environ.get("BUCKET_NAME")
    feedback_prefix: Optional[str] = os.environ.get("FEEDBACK_PREFIX", "")
    question_prefix: Optional[str] = os.environ.get("QUESTION_PREFIX", "")

    @lambda_handler(
        error_status=(
            (QuestionIdError, HTTPStatus.NOT_FOUND.value),
            (FeedbackError, HTTPStatus.BAD_REQUEST.value),
        ),
        logging_fn=logger.error,
    )
    def handler(
        event: Dict[str, Any], context: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        logger.info(f"Received event: {json.dumps(event)}")

        # Extract questionId from the event's pathParameters
        question_id: str = event["pathParameters"].get("questionId")
        if not question_id:
            logger.error("questionId is missing from pathParameters.")
            raise QuestionIdError("questionId is missing from pathParameters.")

        # Generate unique feedback UUID
        feedback_uuid: str = generate_feedback_uuid()

        # Construct the S3 key for retrieving the existing question data
        question_s3_key: str = f"{question_prefix}/{question_id}.json"
        logger.info(f"Getting question data from S3 with key: {question_s3_key}")

        # Fetch existing question data from S3
        dict_data = fetch_existing_data(s3_adapter, s3_bucket, question_s3_key)

        # Get and validate feedback from the event body
        feedback_data = event.get("body", {})
        logger.info(f"Feedback data after loading JSON: {feedback_data}")

        try:
            # Attempt to validate the feedback
            feedback = validate_feedback(feedback_data)
            logger.info(f"Feedback successfully validated: {feedback}")
        except ValidationError as e:
            logger.error(f"Validation error: {e}")
            # Raise the validation error to be caught in tests
            raise e

        # Add validated feedback to the existing question data
        dict_data["feedback"] = feedback.dict()

        # Construct the S3 key for saving the feedback data with the UUID and questionId
        feedback_s3_key: str = (
            f"{feedback_prefix}/feedback_{feedback_uuid}_{question_id}.json"
        )
        logger.info(
            f"Saving feedback with question data to S3 with key: {feedback_s3_key}"
        )

        # Save the feedback with question data back to the history bucket
        save_feedback_to_s3(s3_adapter, s3_bucket, feedback_s3_key, dict_data)

        return {
            "statusCode": HTTPStatus.OK.value,
            "body": json.dumps(
                {
                    "message": f"Feedback for questionId {question_id} saved successfully."
                }
            ),
        }

    return handler
