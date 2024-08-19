import json
import logging
import os
import boto3
from botocore.exceptions import ClientError
from http import HTTPStatus
from typing import Dict, Any, Optional
from lambdas.feedback_sender_POST.s3_adapter import S3Adapter, body_as_dict


class FeedbackError(Exception):
    pass


class QuestionIdError(Exception):
    """Custom exception for questionId-related errors."""
    pass


logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))


def validate_feedback(feedback: Dict[str, Any]) -> None:
    if "helpful" not in feedback or not isinstance(feedback["helpful"], bool):
        logger.error("Invalid feedback value: Must be a boolean True or False")
        raise ValueError("Invalid feedback value: Must be a boolean True or False")


def save_feedback_to_s3(s3_adapter: S3Adapter, s3_bucket: str, s3_key: str, feedback_data: Dict[str, Any]) -> None:
    try:
        logger.info(f"Saving feedback to S3: bucket={s3_bucket}, key={s3_key}, data={feedback_data}")
        s3_adapter.try_save_object(
            bucket_name=s3_bucket, key=s3_key, body=feedback_data
        )
        logger.info("Feedback saved to S3 successfully")
    except ClientError as e:
        logger.error(f"Error saving feedback to S3: {e}")
        raise FeedbackError("Error saving feedback to S3") from e


def build_handler(s3_adapter: S3Adapter) -> Any:
    s3_bucket: Optional[str] = os.environ.get("BUCKET_NAME")
    env_prefix: Optional[str] = os.environ.get("PREFIX", "")
    s3_prefix: str = f"{env_prefix}/" if env_prefix else ""

    def handler(event: Dict[str, Any], context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        logger.info(f"Received event: {json.dumps(event)}")
        
        # Extract questionId from pathParameters
        question_id: str = event["pathParameters"].get("questionId")
        if not question_id:
            logger.error("questionId is missing from pathParameters.")
            raise QuestionIdError("questionId is missing from pathParameters.")
        
        # Construct S3 key
        s3_key: str = f"{s3_prefix}{question_id}.json"

        # Check if the questionId exists in S3
        try:
            existing_data: Optional[Dict[str, Any]] = s3_adapter.try_get_object(s3_bucket, s3_key)
        except ClientError as e:
            logger.error(f"Error fetching data from S3 for questionId {question_id}: {e}")
            raise QuestionIdError(f"questionId {question_id} not found in S3.") from e

        # Parse the existing data from S3
        dict_data: Dict[str, Any] = body_as_dict(existing_data)
        
        # Extract and validate feedback from event body
        feedback: Dict[str, Any] = json.loads(event.get("body", "{}")).get("feedback")
        validate_feedback(feedback)
        
        # Update the feedback in the existing data
        dict_data["feedback"] = feedback

        # Save the updated feedback back to S3
        save_feedback_to_s3(s3_adapter, s3_bucket, s3_key, dict_data)

        return {
            "statusCode": HTTPStatus.OK.value,
            "body": json.dumps({
                "message": f"Feedback for questionId {question_id} saved successfully."
            }),
        }

    return handler


if not bool(os.environ.get("TEST_FLAG", False)):
    region: str = os.environ.get("AWS_REGION", "ca-central-1")
    s3_client = boto3.client("s3", region_name=region)
    s3_adapter = S3Adapter(s3_client)
    lambda_handler = build_handler(s3_adapter)
