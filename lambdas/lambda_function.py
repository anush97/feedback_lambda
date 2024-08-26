import json
import logging
import os
import boto3
import uuid
from botocore.exceptions import ClientError
from http import HTTPStatus
from typing import Dict, Any, Optional
from pydantic import BaseModel, TypeAdapter
from .s3_adapter import S3Adapter, body_as_dict
from common import decorator


class FeedbackError(Exception):
    pass


class QuestionIdError(FileNotFoundError):
    pass


logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))


# Pydantic model for feedback validation
class Feedback(BaseModel):
    helpful: bool


def validate_feedback(feedback_data: dict) -> Feedback:
    return TypeAdapter(Feedback).validate_python(feedback_data)


# Function to generate unique feedback UUIDs
def generate_feedback_uuid() -> str:
    return str(uuid.uuid4())


# Function to save feedback data to S3
def save_feedback_to_s3(
    s3_adapter: S3Adapter, s3_bucket: str, s3_key: str, feedback_data: Dict[str, Any]
) -> None:
    try:
        logger.info(f"Saving feedback to S3: bucket={s3_bucket}, key={s3_key}")
        s3_adapter.try_save_object(
            bucket_name=s3_bucket, key=s3_key, body=feedback_data
        )
        logger.info("Feedback saved to S3 successfully")
    except ClientError as e:
        logger.error(f"Error saving feedback to S3: {e}")
        raise FeedbackError("Error saving feedback to S3") from e

@load_json_body  
def build_handler(s3_adapter: S3Adapter) -> Any:
    s3_bucket: Optional[str] = os.environ.get("BUCKET_NAME")
    feedback_prefix: Optional[str] = os.environ.get("FEEDBACK_PREFIX", "")
    question_prefix: Optional[str] = os.environ.get("QUESTION_PREFIX", "")

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

        # Retrieve and parse the existing question data from the S3 bucket
        try:
            existing_data: Optional[Dict[str, Any]] = s3_adapter.try_get_object(
                s3_bucket, question_s3_key
            )
            if existing_data is None:
                logger.error(
                    f"No such object found in S3: bucket={s3_bucket}, key={question_s3_key}"
                )
                raise QuestionIdError(
                    f"No such object found in S3: bucket={s3_bucket}, key={question_s3_key}"
                )
        except ClientError as e:
            logger.error(
                f"Error fetching data from S3 for questionId {question_id}: {e}"
            )
            raise QuestionIdError(f"questionId {question_id} not found in S3.") from e

        dict_data: Dict[str, Any] = body_as_dict(existing_data)

        # Get and validate feedback from the event body
        feedback_data = event.get("body", {})
        feedback = validate_feedback(feedback_data)
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


if not bool(os.environ.get("TEST_FLAG", False)):
    region: str = os.environ.get("AWS_REGION", "ca-central-1")
    s3_client = boto3.client("s3", region_name=region)
    s3_adapter = S3Adapter(s3_client)
    handler = build_handler(s3_adapter)
