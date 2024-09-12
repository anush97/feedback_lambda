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
from common.elasticsearch import ElasticSearchV2, create_es_client
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
    es_response = es_client.search_documents(index=es_index, query=query)

    es_call_ids = {record["_id"] for record in es_response["hits"]["hits"]}

    if es_call_ids != call_ids_set:
        invalid_call_ids = list(call_ids_set - es_call_ids)
        logger.info("Requested call_ids: %s", call_ids)
        logger.info("Invalid call_ids: %s", invalid_call_ids)
        raise ValidationError(f"Invalid call_ids: {invalid_call_ids}")


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
    es_client = create_es_client_fn(host=host, use_ssl=True)

    @lambda_handler(
        logging_fn=logger,
        error_status=(
            (AccessDeniedError, 403),
            (ValidationError, 400),
            (ConfigurationError, 500),
            (SQSError, 500),
        ),
        require_user=True,
        sanitize=True,
    )
    def handler(event, context, user: User):
        logger.info(f"Event Received: {json.dumps(event)}")
        body = event_parser.extract_body(event)

        # Get credentials from the Transcribe Manager Cognito Group
        credentials = event_parser.extract_credentials(event)
        dynamodb_client = create_dynamodb_client_fn(credentials=credentials)
        dynamodb_mapper = DynamoDBMapper(dynamodb_client=dynamodb_client, logger=logger)
        sqs_client = create_sqs_client_fn(credentials=credentials)
        sqs_adapter = SQSAdapter(sqs_client=sqs_client, logger=logger)

        call_ids = body
        user_groups = get_user_groups(user.email)
        call_access_restriction_parameters = CallAccessRestrictionQueryParameter(
            user_groups
        )
        filters = {}
        call_access_restriction_query = call_access_restriction_parameters.create_query(
            filters
        )

        validate_calls_id_es(
            es_client, es_index, call_ids, call_access_restriction_query
        )

        job_id = generate_job_id()

        # Update the DynamoDB table with the job_id and call_ids
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
        job_publisher = OnRequestJobPublisher(
            es_client=es_client, sqs_adapter=sqs_adapter, logger=logger
        )
        job_publisher(call_ids=call_ids, job_id=job_id, user_email=user.email)

        # Successful job creation of all requested call_ids
        return 201, call_ids

    return handler


if not bool(os.environ.get("TEST_FLAG", False)):
    handler = build_handler(create_dynamodb_client, create_es_client, create_sqs_client)
