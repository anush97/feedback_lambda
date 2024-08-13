import json
import boto3
import os
from aws_lambda_powertools import Logger, Tracer
from botocore.exceptions import ClientError

# 1. Environment variables and logger initialization at the module level
logger = Logger()
tracer = Tracer()
s3 = boto3.client('s3')
s3_bucket = os.environ.get('BUCKET_NAME')

# 2. Handler creation inside build_handler() function
def build_handler():
    @logger.inject_lambda_context
    @tracer.capture_lambda_handler
    def handler(event, context):
        logger.info("Received event", extra={"event": event})

        try:
            # Extract questionId from path parameters
            question_id = event['pathParameters']['questionId']
            feedback = int(json.loads(event.get('body', '{}')).get('feedback'))

            # Input validation for feedback value
            if feedback not in [0, 1]:
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
            
        except ClientError as client_error:
            logger.exception("ClientError occurred during S3 operation")
            return {
                'statusCode': 502,
                'body': json.dumps('Failed to save feedback to S3 due to client error')
            }
        except Exception as e:
            logger.exception("Unexpected error in lambda_handler")
            return {
                'statusCode': 500,
                'body': json.dumps('Internal server error')
            }

    return handler

@tracer.capture_method
def save_feedback_to_s3(s3_bucket: str, s3_key: str, feedback_data: str):
    """Saves feedback data to the specified S3 bucket with a key based on question ID."""
    try:
        s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=feedback_data)
        logger.info("Feedback saved to S3 successfully", extra={"s3_bucket": s3_bucket, "s3_key": s3_key})
    except ClientError as e:
        logger.exception("Error saving feedback to S3")
        raise e

# The Lambda function handler is assigned by calling build_handler()
lambda_handler = build_handler()
