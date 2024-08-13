import json
import logging
import os
import boto3
from botocore.exceptions import ClientError

# Initialize logger and S3 client 
logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))
s3 = boto3.client('s3')

def save_feedback_to_s3(s3_bucket: str, s3_key: str, feedback_data: str):
    """Saves feedback data to the specified S3 bucket with a key based on question ID."""
    try:
        s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=feedback_data)
        logger.info("Feedback saved to S3 successfully", extra={"s3_bucket": s3_bucket, "s3_key": s3_key})
    except ClientError as e:
        logger.error(f"Error saving feedback to S3: {e}")
        raise e  

def build_handler():
    s3_bucket = os.environ.get('BUCKET_NAME')

    def handler(event: dict, context):
        logger.info(f"Received event: {json.dumps(event)}")

        try:
            # Extract questionId from path parameters
            question_id = event['pathParameters']['questionId']
            
            # Extract and validate feedback from the request body
            feedback = json.loads(event.get('body', '{}')).get('feedback')


            # Prepare feedback data to be saved
            feedback_data = json.dumps({
                'questionId': question_id,
                'feedback': feedback
            })

            # Generate S3 key for storing feedback
            s3_key = f"feedback/question_{question_id}.json"

            # Save feedback data to S3
            save_feedback_to_s3(s3_bucket, s3_key, feedback_data)
            
            # Return success response
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': f'Feedback for questionId {question_id} saved successfully.',
                    's3_key': s3_key
                })
            }
            
        except ClientError as client_error:
            logger.error(f"S3 ClientError: {client_error}")
            return {
                'statusCode': 502,
                'body': json.dumps('Failed to save feedback to S3 due to client error')
            }
        except Exception as e:
            logger.error(f"Unexpected error in lambda_handler: {e}")
            return {
                'statusCode': 500,
                'body': json.dumps('Internal server error')
            }

    return handler

# Conditionally initialize the Lambda handler based on the TEST_FLAG environment variable
if not bool(os.environ.get("TEST_FLAG", False)):
    lambda_handler = build_handler()
