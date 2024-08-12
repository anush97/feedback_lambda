import json
import logging
import os
import boto3

# Configure logger
logger = logging.getLogger("feedback_handler")
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

def save_feedback_to_s3(s3_bucket: str, question_id: int, feedback_data: str):
    """Saves feedback data to the specified S3 bucket with a key based on question ID."""
    s3 = boto3.client('s3')
    
    # Generate S3 key directly within the function
    s3_key = f"feedback/question_{question_id}.json"

    try:
        s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=feedback_data)
        logger.info("Feedback saved to S3 successfully")
    except boto3.exceptions.S3UploadFailedError as s3_error:
        logger.error(f"S3 Upload Failed: {s3_error}")
        raise s3_error
    except Exception as e:
        logger.error(f"Unexpected error saving feedback to S3: {e}")
        raise e

def lambda_handler(event: dict, context):
    logger.info("Received event: %s", json.dumps(event))

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
        #s3_bucket = os.environ['BUCKET_NAME']
        s3_bucket = "userfeedbackbucket"
        # Save the feedback to S3
        save_feedback_to_s3(s3_bucket, question_id, feedback_data)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Feedback for questionId {question_id} saved successfully.',
                's3_key': f"feedback/question_{question_id}.json"
            })
        }
        
    except boto3.exceptions.S3UploadFailedError as s3_error:
        logger.error(f"S3 Upload Failed: {s3_error}")
        return {
            'statusCode': 502,
            'body': json.dumps('Failed to save feedback to S3')
        }
    except Exception as e:
        logger.error(f"Error in lambda_handler: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Internal server error')
        }