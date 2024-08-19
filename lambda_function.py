import json
import logging
import os
import boto3
from botocore.exceptions import ClientError
from http import HTTPStatus
from s3_adapter import S3Adapter, body_as_dict


class FeedbackError(Exception):
    pass

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

def validate_feedback(feedback) -> dict:
    if "helpful" not in feedback or type(feedback["helpful"]) is not bool:
        logger.error(
            "Invalid feedback_sender_POST value: Must be a boolean True or False"
        )
        raise ValueError(
            "Invalid feedback_sender_POST value: Must be a boolean True or False"
        )

def save_feedback_to_s3(s3_adapter, s3_bucket: str, s3_key: str, feedback_data: dict):
    try:
        logger.info(
            f"Saving feedback_sender_POST to S3: bucket={s3_bucket}, key={s3_key}, data={feedback_data}"
        )
        s3_adapter.try_save_object(
            bucket_name=s3_bucket, key=s3_key, body=feedback_data
        )
        logger.info("Feedback saved to S3 successfully")
    except ClientError as e:
        logger.error(f"Error saving feedback_sender_POST to S3: {e}")
        raise FeedbackError("Error saving feedback_sender_POST to S3") from e


def build_handler(s3_adapter):
    s3_bucket = os.environ.get("BUCKET_NAME")
    env_prefix = os.environ.get("PREFIX", "")
    s3_prefix = f"{env_prefix}/" if env_prefix else ""

    def handler(event: dict, context: dict):
        logger.info(f"Received event: {json.dumps(event)}")
        question_id = event["pathParameters"]["questionId"]
        feedback = json.loads(event.get("body", "{}")).get("feedback_sender_POST")
        validate_feedback(feedback)

        s3_key = f"{s3_prefix}{question_id}.json"
        data = s3_adapter.try_get_object(s3_bucket, s3_key)
        dict_data = body_as_dict(data)
        dict_data["feedback"] = feedback
        save_feedback_to_s3(s3_adapter, s3_bucket, s3_key, dict_data)
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
    region = os.environ.get("AWS_REGION", "ca-central-1")
    s3_client = boto3.client("s3", region_name=region)
    s3_adapter = S3Adapter(s3_client)
    lambda_handler = build_handler(s3_adapter)
