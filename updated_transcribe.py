import json
import logging
import os
import uuid
from datetime import datetime

from common import event_parser
from common.admin.dynamodb_mapper import (
    create_dynamodb_client,
    DynamoDBMapper,
)
from common.call_access_restriction_utils import (
    get_user_groups,
    CallAccessRestrictionQueryParameter,
)
from common.config import validate_env_variables
from common.decorator import lambda_handler, User
from common.elasticsearch import (
    ElasticSearchV2,
    create_es_client,
    ElasticsearchFailedRequestError,
)
from common.errors import (
    AccessDeniedError,
    ValidationError,
    ConfigurationError,
    SQSError,
)
from common.sqs_adapter import create_sqs_client, SQSAdapter
from .on_request_job_publisher import OnRequestJobPublisher
from .on_request_job_updater import OnRequestJobUpdater

logger = logging.getLogger("transcribe-on-request-post-logger")
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))


def validate_user_access(es_client: ElasticSearchV2, user_groups: list) -> bool:
    """Validate if the user has access to transcribe calls."""
    try:
        # Construct an Elasticsearch query to check user's group access
        query = {
            "query": {"bool": {"should": [{"terms": {"user_group": user_groups}}]}},
            "size": 1,  # Limit to 1 to quickly check if access exists
        }
        logger.info(f"Validating user access with query: {json.dumps(query, indent=2)}")

        # Query Elasticsearch for access validation
        response = es_client.search_documents(index="access-rights", query=query)
        logger.info(f"Elasticsearch access validation response: {json.dumps(response, indent=2)}")

        # If hits are found, user group has access
        if response["hits"]["total"]["value"] > 0:
            logger.info("User group has access to transcribe calls.")
            return True

        # Log and return False if the user doesn't have access
        logger.warning("User does not have the rights to transcribe calls.")
        return False

    except Exception as e:
        logger.error(f"Error during user group access validation: {e}")
        return False


def validate_calls_id_es(
    es_client: ElasticSearchV2,
    es_index: str,
    call_ids: list,
    call_access_restriction_query: dict,
) -> None:
    call_ids_set = set(call_ids)
    query = {
        "_source": ["_id"],
        "query": {
            "bool": {
                "must": [
                    {"range": {"created_at_": {"gte": "now-1y"}}},
                    {"terms": {"_id": call_ids}},
                    {"match": {"transcribed": False}},
                    call_access_restriction_query,
                ]
            }
        },
        "size": len(call_ids),
    }
    
    logger.info(f"Validating call IDs with query: {json.dumps(query, indent=2)}")

    try:
        es_response = es_client.search_documents(index=es_index, query=query)
        logger.info(f"Elasticsearch response for call ID validation: {json.dumps(es_response, indent=2)}")

        es_call_ids = {record["_id"] for record in es_response["hits"]["hits"]}

        if es_call_ids != call_ids_set:
            invalid_call_ids = list(call_ids_set - es_call_ids)
            logger.warning(f"Invalid call_ids: {invalid_call_ids}")
            raise ValidationError(f"Invalid call_ids: {invalid_call_ids}")

    except AccessDeniedError as e:
        logger.error("Access denied when querying Elasticsearch for call IDs: %s", e)
        raise AccessDeniedError("Access to Elasticsearch was denied.")

    except Exception as e:
        logger.error("An error occurred while querying Elasticsearch for call IDs: %s", e)
        raise ValidationError(f"Elasticsearch query failed with error: {str(e)}")


def generate_job_id() -> str:
    now = datetime.utcnow()
    return f"job_{str(uuid.uuid4())[:8]}_{now.isoformat(timespec='seconds')}"


def build_handler(create_dynamodb_client_fn, create_es_client_fn, create_sqs_client_fn):
    validate_env_variables(
        "ELASTICSEARCH_INDEX",
        "ELASTICSEARCH_HOST",
        "TRANSCRIBE_ON_REQUEST_STATUS_TABLE",
        "DAYS_TO_EXPIRE",
        "AUDIO_SOURCE_BUCKET",
        "AUDIO_SOURCE_PREFIX",
        "SQS_QUEUE_URL",
    )
    transcribe_on_request_status_table = os.environ[
        "TRANSCRIBE_ON_REQUEST_STATUS_TABLE"
    ]

    days_to_expire = int(os.environ["DAYS_TO_EXPIRE"])
    host = os.environ["ELASTICSEARCH_HOST"]
    es_index = os.environ["ELASTICSEARCH_INDEX"]

    @lambda_handler(
        logging_fn=logger,
        error_status=(
            (AccessDeniedError, 403),
            (ValidationError, 400),
            (ConfigurationError, 500),
            (SQSError, 500),
            (ElasticsearchFailedRequestError, 500),
        ),
        require_user=True,
        sanitize=True,
    )
    def handler(event, context, user: User):
        logger.info(f"Event Received: {json.dumps(event)}")
        body = event_parser.extract_body(event)

        try:
            # Extract user credentials from the event
            credentials = event_parser.extract_credentials(event)
            # Get user groups based on the user's email
            user_groups = get_user_groups(user.email)
            logger.info(f"User groups: {user_groups}")

            # Create the Elasticsearch client
            es_client = create_es_client_fn(
                host=host, auth=credentials, use_ssl=True
            )

            # Validate if the user has access to transcribe calls
            if not validate_user_access(es_client, user_groups):
                logger.warning("User does not have access to transcribe calls.")
                raise AccessDeniedError("User group does not have access to transcribe calls.")

        except AccessDeniedError as e:
            logger.error(f"Access denied when creating Elasticsearch client or validating access: {e}")
            raise AccessDeniedError("403 Forbidden: Access to Elasticsearch denied.")
        except ValueError as e:
            logger.warning(
                f"Credentials missing or invalid. Continuing without credentials: {e}"
            )
        except Exception as e:
            logger.error(f"Unexpected error during Elasticsearch client creation or access validation: {e}")
            raise ConfigurationError(
                f"Unexpected error during Elasticsearch setup: {str(e)}"
            )

        # Get credentials from the Transcribe Manager Cognito Group
        logger.info("Setting up DynamoDB and SQS clients.")
        dynamodb_client = create_dynamodb_client_fn(credentials=credentials)
        dynamodb_mapper = DynamoDBMapper(dynamodb_client=dynamodb_client, logger=logger)
        sqs_client = create_sqs_client_fn(credentials=credentials)
        sqs_adapter = SQSAdapter(sqs_client=sqs_client, logger=logger)

        call_ids = body
        call_access_restriction_query = CallAccessRestrictionQueryParameter(
            user_groups
        ).create_query({})

        # Validate call IDs against Elasticsearch
        try:
            validate_calls_id_es(
                es_client, es_index, call_ids, call_access_restriction_query
            )
        except AccessDeniedError as e:
            logger.error(f"Access denied during Elasticsearch validation: {e}")
            raise AccessDeniedError("403 Forbidden: Access to Elasticsearch denied.")
        except ElasticsearchFailedRequestError as e:
            logger.error(f"Failed request to Elasticsearch during validation: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during Elasticsearch validation: {e}")
            raise ValidationError(
                f"Unexpected error during call ID validation: {str(e)}"
            )

        job_id = generate_job_id()

        # Update the DynamoDB table with the job_id and call_ids
        logger.info("Updating DynamoDB with job ID and call IDs.")
        job_updater = OnRequestJobUpdater(
            dynamodb_mapper=dynamodb_mapper,
            dynamodb_status_table=transcribe_on_request_status_table,
            logger=logger,
        )
        job_updater(
            job_id=job_id,
            call_ids=call_ids,
            user_email=user.email,
            days_to_expire=days_to_expire,
        )

        # Publish the job to the SQS queue
        logger.info("Publishing the job to the SQS queue.")
        job_publisher = OnRequestJobPublisher(
            es_client=es_client, sqs_adapter=sqs_adapter, logger=logger
        )
        job_publisher(call_ids=call_ids, job_id=job_id, user_email=user.email)

        # Successful job creation of all requested call_ids
        logger.info(f"Successfully created job with ID: {job_id}")
        return 201, call_ids

    return handler


if not bool(os.environ.get("TEST_FLAG", False)):
    handler = build_handler(create_dynamodb_client, create_es_client, create_sqs_client)
