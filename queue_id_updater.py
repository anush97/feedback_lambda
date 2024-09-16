"""assign queue_id to Internal calls"""

import logging
import os

import boto3

from common.elasticsearch import create_es_client
from common.s3_adapter import S3Adapter, body_as_dict


def create_get_queue_id_query(original_contact_id):
    get_queue_id_value = {
        "_source": ["original_contact_id", "call_direction", "queue_id"],
        "query": {
            "bool": {
                "must": [
                    {"term": {"call_direction": "1"}},
                    {"term": {"original_contact_id": original_contact_id}},
                ]
            }
        },
    }
    return get_queue_id_value


def create_update_queue_id_query(queue_id_value):
    update_queue_id_to_internal_call = {
        "script": {
            "source": "ctx._source.queue_id = params.queue_id;",
            "lang": "painless",
            "params": {"queue_id": queue_id_value},
        }
    }
    return update_queue_id_to_internal_call


class QueueIdUpdater:
    def __init__(
        self,
        es_client,
        es_index_name: str,
        s3_adapter: S3Adapter,
        bucket_name: str,
        purpose: str,
        extra_metadata_prefix: str,
        number_of_days: str,
        logger: logging.Logger,
    ):
        self.es_client = es_client
        self.es_index_name = es_index_name
        self.s3_adapter = s3_adapter
        self.bucket_name = bucket_name
        self.purpose = purpose
        self.extra_metadata_prefix = extra_metadata_prefix
        self.number_of_days = number_of_days
        self.logger = logger

    def __call__(self, event, context):
        self.logger.info("Event received: %s", event)

        empty_queue_id_calls_list = self.get_internal_calls_with_empty_queue_id()

        for queue_id_call_detail in empty_queue_id_calls_list:
            es_index_name = queue_id_call_detail["_index"]
            original_contact_id = queue_id_call_detail["_source"]["original_contact_id"]
            self.logger.info("Handling call %s", original_contact_id)

            queue_id_value = self.get_queue_id_value_from_inbound_call(
                es_index_name, original_contact_id
            )
            if queue_id_value:
                self.logger.info(
                    "Updating queue_id value for call %s", original_contact_id
                )
                call_id = queue_id_call_detail["_id"]
                self.update_queue_id(es_index_name, call_id, queue_id_value)

                self.logger.info(
                    "Updating extra metadata file for call %s", original_contact_id
                )
                file_prefix = queue_id_call_detail["_source"]["filename_prefix"]
                self.update_extra_metadata(file_prefix, queue_id_value)
            else:
                self.logger.info(
                    "No matching inbound call found for call %s", original_contact_id
                )

        return "queue_id_updated_successfully"

    def create_get_empty_queue_id_query(self):
        return {
            "_source": ["queue_id", "original_contact_id", "filename_prefix"],
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "created_at_": {"gte": f"now-{self.number_of_days}d"}
                            }
                        },
                        {"term": {"call_direction": "3"}},
                        {"term": {"queue_id": ""}},
                    ]
                }
            },
        }

    def get_internal_calls_with_empty_queue_id(self):
        """
        get 4 days older empty queue_id internal calls
        """

        res = self.es_client.request(
            verb="GET",
            endpoint=f"{self.es_index_name}/_search",
            body=self.create_get_empty_queue_id_query(),
        )

        result_hits = res["hits"]
        self.logger.info(
            "Number of hits in response: %s", result_hits["total"]["value"]
        )
        return result_hits["hits"]

    def get_queue_id_value_from_inbound_call(
        self, es_index_name: str, original_contact_id: str
    ):
        """
        matching inbound calls with original contactId
        """

        res = self.es_client.request(
            verb="GET",
            endpoint=f"{es_index_name}/_search",
            body=create_get_queue_id_query(original_contact_id),
        )

        self.logger.debug("ES client response: %s", res)

        hit_count = res["hits"]["total"]["value"]
        if hit_count > 0:
            queue_id_value = res["hits"]["hits"][0]["_source"]["queue_id"]
        else:
            queue_id_value = None
        return queue_id_value

    def update_queue_id(self, es_index_name: str, call_id: str, queue_id_value: str):
        """
        updating internal calls id with queue_id
        """
        response = self.es_client.request(
            verb="POST",
            endpoint=f"{es_index_name}/_update/{call_id}",
            body=create_update_queue_id_query(queue_id_value),
        )
        return response

    def update_extra_metadata(self, file_prefix: str, queue_id_value: str):
        extra_metadata_key = (
            f"{self.extra_metadata_prefix}/{self.purpose}-{file_prefix}.json"
        )
        """update extra metadata json"""

        try:
            extra_metadata_json = body_as_dict(
                self.s3_adapter.try_get_object(self.bucket_name, extra_metadata_key)
            )
        except FileNotFoundError as e:
            self.logger.error(
                f"Couldn't find file {extra_metadata_key}, skipping updating extra_metadata"
            )
            self.logger.error(f"Error: {e}")
            return

        extra_metadata_json["queue_id"] = queue_id_value
        self.s3_adapter.try_save_object(
            self.bucket_name, extra_metadata_key, extra_metadata_json
        )


def build_handler(s3_adapter: S3Adapter):
    host = os.environ.get("ELASTICSEARCH_HOST", None)
    logger = logging.getLogger("queue_id_handler")
    logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))
    es_client = create_es_client(host=host, use_ssl=True, logger=logger)
    bucket_name = os.environ.get("AUDIO_METADATA_BUCKET")
    purpose = os.environ.get("PURPOSE")
    extra_metadata_key_prefix = os.environ.get("EXTRA_METADATA_PREFIX")
    number_of_days = os.environ.get("NUMBER_OF_DAYS", "4")

    queue_id_updater = QueueIdUpdater(
        es_client=es_client,
        es_index_name=os.environ.get("ELASTICSEARCH_CALL_DETAILS_INDEX", None),
        s3_adapter=s3_adapter,
        bucket_name=bucket_name,
        purpose=purpose,
        extra_metadata_prefix=extra_metadata_key_prefix,
        number_of_days=number_of_days,
        logger=logger,
    )

    return queue_id_updater


if not bool(os.environ.get("TEST_FLAG", False)):
    s3_adapter = S3Adapter(boto3.client("s3"))
    handler = build_handler(s3_adapter)
