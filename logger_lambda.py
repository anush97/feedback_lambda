import json
import boto3
import os
from aws_lambda_powertools import Logger, Tracer

# Initialize Logger and Tracer
logger = Logger(service="feedback_handler")
tracer = Tracer(service="feedback_handler")

# Boto3 S3 client
s3 = boto3.client('s3')

@logger.inject_lambda_context
@tracer.capture_lambda_handler
def lambda_handler(event, context):
    logger.info("Received event", extra={"event": event})

    try:
        # Parse the incoming event from API Gateway
        event_data = json.loads(event.get('body', '{}'))
        question_id = event_data.get('questionId')
        feedback = event_data.get('feedback')

        # Input validation
        if question_id is None or feedback is None:
            logger.error("Invalid input: Missing questionId or feedback")
            return {
                'statusCode': 400,
                'body': json.dumps('Invalid input: Missing questionId or feedback')
            }

        if not isinstance(feedback, int) or feedback not in [0, 1]:
            logger.error("Invalid feedback value: Must be an integer 0 or 1")
            return {
                'statusCode': 400,
                'body': json.dumps('Invalid feedback value: Must be an integer 0 or 1')
            }

        # Create the JSON structure for storing
        feedback_data = json.dumps({
            'questionId': question_id,
            'feedback': feedback
        })

        # S3 bucket name from environment variables
        s3_bucket = os.environ.get('BUCKET_NAME')
        if not s3_bucket:
            logger.error("S3 bucket name not configured in environment variables")
            return {
                'statusCode': 500,
                'body': json.dumps('Internal server error: S3 bucket name not configured')
            }

        s3_key = f"feedback/question_{question_id}.json"

        # Save the feedback to S3
        save_feedback_to_s3(s3_bucket, s3_key, feedback_data)
        
        logger.info(f'Feedback for questionId {question_id} saved successfully in {s3_key}.')
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Feedback for questionId {question_id} saved successfully.',
                's3_key': s3_key
            })
        }
        
    except boto3.exceptions.S3UploadFailedError as s3_error:
        logger.exception("S3 Upload Failed")
        return {
            'statusCode': 502,
            'body': json.dumps('Failed to save feedback to S3')
        }
    except Exception as e:
        logger.exception("Unexpected error in lambda_handler")
        return {
            'statusCode': 500,
            'body': json.dumps('Internal server error')
        }

@tracer.capture_method
def save_feedback_to_s3(s3_bucket: str, s3_key: str, feedback_data: str):
    """Saves feedback data to the specified S3 bucket with a key based on question ID."""
    try:
        s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=feedback_data)
        logger.info("Feedback saved to S3 successfully", extra={"s3_bucket": s3_bucket, "s3_key": s3_key})
    except Exception as e:
        logger.exception("Error saving feedback to S3")
        raise e
