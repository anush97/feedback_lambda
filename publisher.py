import os
from typing import List

from common.elasticsearch import ElasticSearchV2
from common.sqs_adapter import SQSAdapter
from common.std_ext import NullObject
from .on_request_mapping import (
    OnRequestEventModel,
    CallMetadataList,
    CallMetadata,
)


class OnRequestJobPublisher:
    def __init__(
        self, es_client: ElasticSearchV2, sqs_adapter: SQSAdapter, logger=None
    ):
        self.es_client = es_client
        self.sqs_adapter = sqs_adapter
        self.es_index = os.environ["ELASTICSEARCH_INDEX"]
        if logger is None:
            logger = NullObject()
        self.logger = logger
        self.audio_source_bucket = os.environ["AUDIO_SOURCE_BUCKET"]
        self.audio_source_prefix = os.environ["AUDIO_SOURCE_PREFIX"]
        self.sqs_queue = os.environ["SQS_QUEUE_URL"]

    def __get_metadata_from_es(self, call_ids: List[str]) -> CallMetadataList:
        query = self.__prepare_es_query(call_ids)
        es_response = self.es_client.search_documents(index=self.es_index, query=query)
        self.logger.debug("Elasticsearch response: %s", es_response)

        call_metadata_es_list = (
            {
                **es_doc["_source"],
                "sid": es_doc["_id"],
                "wav_url": self.__build_wav_url(es_doc["_source"]["filename_prefix"]),
            }
            for es_doc in es_response["hits"]["hits"]
        )

        return CallMetadataList.model_validate(call_metadata_es_list)

    def __prepare_es_query(self, call_ids: list[str]) -> dict:
        source_fields = list(CallMetadata.model_fields.keys())
        query = {
            "_source": source_fields,
            "query": {
                "bool": {
                    "must": [
                        {"range": {"created_at_": {"gte": "now-1y"}}},
                        {"ids": {"values": call_ids}},
                        {"match": {"transcribed": False}},
                    ]
                }
            },
            "size": len(call_ids),
        }
        self.logger.debug(f"ES query: {query}")
        return query

    def __build_wav_url(self, filename_prefix: str) -> str:
        return f"s3://{self.audio_source_bucket}/{self.audio_source_prefix}/{filename_prefix}.wav"

    def __publish_batch_calls_to_sqs(
        self, on_request_job_events: list[OnRequestEventModel]
    ):
        sqs_message_list = [
            on_request_job_event.to_sqs_message()
            for on_request_job_event in on_request_job_events
        ]
        self.logger.debug("SQS message list: %s", sqs_message_list)
        self.sqs_adapter.send_message_batch(
            queue_url=self.sqs_queue, messages=sqs_message_list
        )

    def __call__(self, call_ids: List[str], job_id: str, user_email: str) -> None:
        call_metadata_list = self.__get_metadata_from_es(call_ids)

        on_request_jobs = self.__create_on_request_events(
            call_metadata_list, job_id, user_email
        )

        self.logger.info(
            "Publishing SQS messages to queue %s for the job %s,  call ids %s and user %s",
            self.sqs_queue,
            job_id,
            call_ids,
            user_email,
        )
        self.__publish_batch_calls_to_sqs(on_request_jobs)

    def __create_on_request_events(
        self, call_metadata_list: CallMetadataList, job_id: str, user_email: str
    ) -> list[OnRequestEventModel]:
        return [
            OnRequestEventModel(
                on_request_job_id=job_id,
                on_request_job_user=user_email,
                call_metadata=call_metadata,
            )
            for call_metadata in call_metadata_list.root
        ]
