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


class QuestionNotFoundError(Exception):
    pass



logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

BUCKET_NAME = os.environ.get("BUCKET_NAME", "")
PREFIX = os.environ.get("PREFIX", "")

def validate_feedback(feedback: Dict[str, Any]) -> None:
    if "helpful" not in feedback or type(feedback["helpful"]) is not bool:
        logger.error("Invalid feedback value: Must be a boolean True or False")
        raise ValueError("Invalid feedback value: Must be a boolean True or False")


def save_feedback_to_s3(s3_adapter: S3Adapter, bucket: str, key: str, feedback_data: Dict[str, Any]) -> None:
    try:
        logger.info(f"Saving feedback to S3: bucket={bucket}, key={key}, data={feedback_data}")
        s3_adapter.try_save_object(bucket_name=bucket, key=key, body=feedback_data)
        logger.info("Feedback saved to S3 successfully")
    except ClientError as e:
        logger.error(f"Error saving feedback to S3: {e}")
        raise FeedbackError("Error saving feedback to S3") from e


def verify_question_exists(s3_adapter: S3Adapter, bucket: str, key: str) -> None:
    try:
        s3_adapter.try_get_object(bucket, key)
    except FileNotFoundError:
        logger.error(f"Question with key {key} not found in S3")
        raise QuestionNotFoundError(f"Question with key {key} not found in S3")


def build_lambda_handler(s3_adapter: S3Adapter) -> Any:
    bucket: str = BUCKET_NAME
    prefix: str = f"{PREFIX}/" if PREFIX else ""

    def lambda_handler(event: Dict[str, Any], context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        logger.info(f"Received event: {json.dumps(event)}")

        # Extract question ID and construct S3 key
        question_id = event["pathParameters"]["questionId"]
        s3_key = f"{prefix}{question_id}.json"
        
        # Verify question exists before proceeding
        verify_question_exists(s3_adapter, bucket, s3_key)

        # Validate and process feedback
        feedback = json.loads(event.get("body", "{}")).get("feedback", {})
        validate_feedback(feedback)
        
        existing_data = body_as_dict(s3_adapter.try_get_object(bucket, s3_key))
        existing_data["feedback"] = feedback
        
        # Save updated feedback to S3
        save_feedback_to_s3(s3_adapter, bucket, s3_key, existing_data)
        
        # Return success response
        return {
            "statusCode": HTTPStatus.OK.value,
            "body": json.dumps({"message": f"Feedback for questionId {question_id} saved successfully."}),
        }

    return lambda_handler


# Initialize Lambda handler if not in testing mode
if not bool(os.environ.get("TEST_FLAG", False)):
    region = os.environ.get("AWS_REGION", "ca-central-1")
    s3_client = boto3.client("s3", region_name=region)
    s3_adapter = S3Adapter(s3_client)
    lambda_handler = build_lambda_handler(s3_adapter)
