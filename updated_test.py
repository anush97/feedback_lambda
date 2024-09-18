import json
import os
from unittest import mock
from unittest.mock import MagicMock

import pytest

from common.errors import (
    ConfigurationError,
    AccessDeniedError,
)
from common.models.admin import PermissionGroup
from functions.transcribe_on_request_POST.transcribe_on_request_POST_handler import (
    build_handler,
)
from tests.assert_utils.assert_dict import assert_same_dict
from tests.fixtures.sqs import get_all_messages
from tests.unit.lambda_assertions import (
    assert_status_code,
    assert_body,
    assert_error_message,
)

GROUP_1_NAME = "Test group 1"
ELASTICSEARCH_INDEX = "test-index"
ELASTICSEARCH_HOST = "test-host"
DAYS_TO_EXPIRE = "60"
TRANSCRIBE_ON_REQUEST_STATUS_TABLE = "test-transcribe-on-request-status"
TRANSCRIBE_ON_REQUEST_LOCAL_INDEX = "test-transcribe-on-request-local-index"
AUDIO_SOURCE_BUCKET = "test-audio-source-bucket"
AUDIO_SOURCE_PREFIX = "test-audio-source-prefix"
SQS_QUEUE_URL = "test-sqs-queue-url"
AUTHORIZER = {
    "id_session_token": "mock_id_session_token",
    "id_secret_key": "mock_id_secret_key",
    "identity_id": "ca-central-1:abc123",
    "principalId": "abc123",
    "integrationLatency": 440,
    "id_access_token": "mock_id_access_token",
    "username": "IFCAzureId_test.user@intact.net",
}

@pytest.fixture
def event_with_user():
    return {
        "headers": {
            "Accept-Encoding": "identity",
            "cookie": {
                "CQ:access_token": "test_access_token",
                "CQ:identity": "eyJraWQiOiI2b0xxQnZWZWlJY1FsYmhoMmpBa2NONE5Dd0NxcHlOVVd3Ym9uWVpnUE9VPSIsImFsZyI6IlJTMjU2In0.eyJhdF9oYXNoIjoiS0dVU05tQklLV0dKeWs0WGd0RGxDZyIsInN1YiI6ImZjMmZhZGIzLTI5ZmEtNDk1Yy04YzA4LTE5YmEwZDc2NzQxNyIsImNvZ25pdG86Z3JvdXBzIjpbImNhLWNlbnRyYWwtMV9paHJ6UThqRTVfSUZDQXp1cmVJZCJdLCJlbWFpbF92ZXJpZmllZCI6ZmFsc2UsImlzcyI6Imh0dHBzOi8vY29nbml0by1pZHAuY2EtY2VudHJhbC0xLmFtYXpvbmF3cy5jb20vY2EtY2VudHJhbC0xX2locnpROGpFNSIsImNvZ25pdG86dXNlcm5hbWUiOiJJRkNBenVyZUlkX3Rlc3QudXNlckBpbnRhY3QubmV0IiwiZ2l2ZW5fbmFtZSI6IlRlc3QiLCJhdWQiOiIyb29hYWE4Y3BybGUyMGo5YThqbzgydnV1byIsImlkZW50aXRpZXMiOlt7InVzZXJJZCI6InRlc3QudXNlckBpbnRhY3QubmV0IiwicHJvdmlkZXJOYW1lIjoiSUZDQXp1cmVJZCIsInByb3ZpZGVyVHlwZSI6IlNBTUwiLCJpc3N1ZXIiOiJodHRwczovL3N0cy53aW5kb3dzLm5ldC9iODgwZWVjYS1mMWZiLTRjOTEtYmZmNi04MmU4NDM1MGE2ZTYvIiwicHJpbWFyeSI6InRydWUiLCJkYXRlQ3JlYXRlZCI6IjE2MDY0MjMxNTE2MTIifV0sInRva2VuX3VzZSI6ImlkIiwiYXV0aF90aW1lIjoxNjE0MDk3MDYzLCJleHAiOjI2MTQxMTM2MjEsImlhdCI6MTYxNDExMDAyMSwiZmFtaWx5X25hbWUiOiJVc2VyIiwiZW1haWwiOiJ0ZXN0LnVzZXJAaW50YWN0Lm5ldCJ9.P0614vsawvrNeAeS0CBObNbNbOTjfN7hgfXX7xMA-QsrZUMKEtXHpwqaHSN1ldFxq5aLMMOXahy5VgeSPDRkX3eppixlpGcoFuYz_nmK_XMZ3DWpo78N73ykgBf6HOSjpFjClczT9g3maur3YBtru9lngYwTMFtBHXlj_T0qBV_ngxJGWiqj2WPvba3p4r1Bs14hb-uNkSBQqLChihkW-iR3og9uhpRQf0khNq1Zl4PsOYoqUkKfR9zEetOfpWOidnrnbttLndVcTkHQEwLAS3wnL1jWS2KkL-iUNYSKvZbMLqdPC1byYBd4C3cE9m_c0QA_8-EyBcEZm4x2Mf0eCw",
            },
        },
        "body": ["7654321", "1234567"],
        "requestContext": {"authorizer": AUTHORIZER},
    }

@pytest.fixture(scope="function", autouse=True)
def set_env_variables():
    os.environ["ELASTICSEARCH_INDEX"] = ELASTICSEARCH_INDEX
    os.environ["ELASTICSEARCH_HOST"] = ELASTICSEARCH_HOST
    os.environ["DAYS_TO_EXPIRE"] = DAYS_TO_EXPIRE
    os.environ["TRANSCRIBE_ON_REQUEST_STATUS_TABLE"] = (
        TRANSCRIBE_ON_REQUEST_STATUS_TABLE
    )
    os.environ["AUDIO_SOURCE_BUCKET"] = AUDIO_SOURCE_BUCKET
    os.environ["AUDIO_SOURCE_PREFIX"] = AUDIO_SOURCE_PREFIX
    os.environ["SQS_QUEUE_URL"] = SQS_QUEUE_URL

@pytest.fixture(autouse=True, scope="function")
def setup_resources(dynamodb, sqs):
    dynamodb.create_table(
        TableName=TRANSCRIBE_ON_REQUEST_STATUS_TABLE,
        AttributeDefinitions=[
            {"AttributeName": "jobId", "AttributeType": "S"},
            {"AttributeName": "callId", "AttributeType": "S"},
            {"AttributeName": "lastUpdate", "AttributeType": "N"},
        ],
        KeySchema=[
            {"AttributeName": "jobId", "KeyType": "HASH"},  # Partition key
            {"AttributeName": "callId", "KeyType": "RANGE"},  # Sort key
        ],
        LocalSecondaryIndexes=[
            {
                "IndexName": TRANSCRIBE_ON_REQUEST_LOCAL_INDEX,
                "KeySchema": [
                    {"AttributeName": "lastUpdate", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
        BillingMode="PAY_PER_REQUEST",
    )

    sqs.create_queue(QueueName=SQS_QUEUE_URL)

def test_build_handler(
    create_dynamodb_client_function,
    create_es_client_function,
    create_sqs_client_function,
):
    result = build_handler(
        create_dynamodb_client_fn=create_dynamodb_client_function,
        create_es_client_fn=create_es_client_function,
        create_sqs_client_fn=create_sqs_client_function,
    )
    assert result

@pytest.mark.parametrize(
    "env_variable",
    [
        "ELASTICSEARCH_HOST",
        "ELASTICSEARCH_INDEX",
        "TRANSCRIBE_ON_REQUEST_STATUS_TABLE",
        "DAYS_TO_EXPIRE",
        "AUDIO_SOURCE_BUCKET",
        "AUDIO_SOURCE_PREFIX",
        "SQS_QUEUE_URL",
    ],
)
def test_missing_env_variable(
    env_variable,
    create_dynamodb_client_function,
    create_es_client_function,
    create_sqs_client_function,
):
    del os.environ[env_variable]

    with pytest.raises(ConfigurationError):
        build_handler(
            create_dynamodb_client_fn=create_dynamodb_client_function,
            create_es_client_fn=create_es_client_function,
            create_sqs_client_fn=create_sqs_client_function,
        )

@mock.patch(
    "functions.transcribe_on_request_POST.transcribe_on_request_POST_handler.get_user_groups"
)
@mock.patch(
    "functions.transcribe_on_request_POST.transcribe_on_request_POST_handler.event_parser.extract_credentials"
)
def test_handler_valid_call_id(
    mock_extract_credentials,
    mock_get_user_groups,
    event_with_user,
    create_dynamodb_client_function,
    create_es_client_function,
    dynamodb,
    create_sqs_client_function,
    sqs,
):
    # Mock user credentials
    mock_extract_credentials.return_value = {
        "access_key": "mock_access_key",
        "secret_key": "mock_secret_key",
        "token": "mock_token",
    }

    es_query_response = {
        "hits": {
            "total": {"value": 1, "relation": "eq"},
            "hits": [
                {
                    "_index": "call-details-000001",
                    "_type": "_doc",
                    "_id": "7654321",
                    "_score": 4.227655,
                    "_source": {
                        "original_contact_id": "9149195935190001661",
                        "extension": "1534749",
                        "customer_phone_number": "8676959617",
                        "agent_email": "anand.mistry@belairdirect.com",
                        "distributor_number": "BNA",
                        "created_at_": "2024-01-10T15:07:03.979127+00:00",
                        "start_datetime": "2024-01-10T15:02:12.507261+00:00",
                        "end_datetime": "2021-11-25T16:27:28.300000+00:00",
                        "video_recorded": False,
                        "call_direction": "1",
                        "language": "F",
                        "organization_unit": "- Ammar Shabbir",
                        "duration": 600,
                        "agent_full_name": "Anand Mistry",
                        "filename_prefix": "10-01-2024_10-02-12_sid_7654321_dbsid_719",
                        "call_context": "ACQ",
                        "company_number": "010",
                        "line_of_business": "RE",
                        "total_hold_time": 0,
                        "agent_pbxid": "34749",
                        "region": "YT",
                        "queue_id": "1877122",
                    },
                },
                {
                    "_index": "call-details-000001",
                    "_type": "_doc",
                    "_id": "1234567",
                    "_score": 4.227655,
                    "_source": {
                        "original_contact_id": "9149193817430001661",
                        "extension": "1546752",
                        "customer_phone_number": "5147228029",
                        "agent_email": "alyssa.ficher@belairdirect.com",
                        "distributor_number": "BEL",
                        "created_at_": "2024-01-10T15:07:02.782256+00:00",
                        "start_datetime": "2024-01-10T15:02:13.157913+00:00",
                        "end_datetime": "2021-11-25T16:27:28.300000+00:00",
                        "video_recorded": False,
                        "call_direction": "1",
                        "language": "F",
                        "organization_unit": "- Ammar Shabbir",
                        "duration": 600,
                        "agent_full_name": "Alyssa Ficher",
                        "filename_prefix": "10-01-2024_10-02-13_sid_1234567_dbsid_719",
                        "call_context": "LOY",
                        "company_number": "010",
                        "line_of_business": "AUP",
                        "total_hold_time": 0,
                        "agent_pbxid": "46752",
                        "region": "CAN",
                        "queue_id": "",
                    },
                },
            ],
        },
    }
    request_mock = MagicMock(return_value=es_query_response)

    mock_get_user_groups.return_value = [
        PermissionGroup(
            id=GROUP_1_NAME,
            name=GROUP_1_NAME,
            description="Test group description",
            distributors=["BEL", "BNA"],
            linesOfBusiness=["RE", "AUP"],
        )
    ]
    create_es_client_function.return_value.search_documents = request_mock
    handler = build_handler(
        create_dynamodb_client_fn=create_dynamodb_client_function,
        create_es_client_fn=create_es_client_function,
        create_sqs_client_fn=create_sqs_client_function,
    )
    response = handler(event_with_user, {})

    assert_status_code(response, 201)
    assert_body(response, event_with_user["body"])

    dynamo_items = dynamodb.scan(TableName=TRANSCRIBE_ON_REQUEST_STATUS_TABLE)
    assert len(dynamo_items["Items"]) == 2

    sqs_messages = get_all_messages(sqs_client=sqs, queue_url=SQS_QUEUE_URL)

    for index, value in enumerate(es_query_response["hits"]["hits"]):
        # Validate the item in DynamoDB
        assert dynamo_items["Items"][index]["callId"] == {"S": value["_id"]}

        # Validate if messages were published to SQS
        sqs_message = json.loads(sqs_messages[index])["Records"][0]
        assert sqs_message["sid"] == value["_id"]
        assert "on_request_job_id" in sqs_message
        assert (
            sqs_message["wav_url"]
            == f"s3://{AUDIO_SOURCE_BUCKET}/{AUDIO_SOURCE_PREFIX}/{value['_source']['filename_prefix']}.wav"
        )
        assert sqs_message["on_request_job_user"] == "test.user@intact.net"
        assert_same_dict(
            sqs_message,
            value["_source"],
            ["on_request_job_id", "wav_url", "on_request_job_user", "sid"],
        )

@mock.patch(
    "functions.transcribe_on_request_POST.transcribe_on_request_POST_handler.get_user_groups"
)
@mock.patch(
    "functions.transcribe_on_request_POST.transcribe_on_request_POST_handler.event_parser.extract_credentials"
)
def test_handler_invalid_call_id(
    mock_extract_credentials,
    mock_get_user_groups,
    event_with_user,
    create_dynamodb_client_function,
    create_es_client_function,
    create_sqs_client_function,
):
    # Mock user credentials
    mock_extract_credentials.return_value = {
        "access_key": "mock_access_key",
        "secret_key": "mock_secret_key",
        "token": "mock_token",
    }

    # Mock user groups to simulate user with rights
    mock_get_user_groups.return_value = [
        PermissionGroup(
            id=GROUP_1_NAME,
            name=GROUP_1_NAME,
            description="Test group description",
            distributors=["BEL", "BNA"],
            linesOfBusiness=["RE", "AUP"],
        )
    ]

    # Mock Elasticsearch search_documents to return a response indicating invalid call_ids
    request_mock = MagicMock(
        return_value={
            "hits": {
                "total": {"value": 1, "relation": "eq"},
                "hits": [
                    {
                        "_index": "call-details-000001",
                        "_type": "_doc",
                        "_id": "7654321",
                        "_source": {},  # This ID is valid
                    }
                ],
            },
        }
    )
    create_es_client_function.return_value.search_documents = request_mock

    # Build the handler
    handler = build_handler(
        create_dynamodb_client_fn=create_dynamodb_client_function,
        create_es_client_fn=create_es_client_function,
        create_sqs_client_fn=create_sqs_client_function,
    )

    # Invoke the handler
    response = handler(event_with_user, {})

    # Assert that the response status is 400 (Bad Request)
    assert_status_code(response, 400)

    # Assert the specific error message related to invalid call_ids
    assert "Invalid call_ids: ['1234567']" in response["body"]
