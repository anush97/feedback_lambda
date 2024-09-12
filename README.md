import json
import logging
import os

import boto3

from common.config import extract_tags_from_env
from common.decorator import lambda_handler
from common.enums import PipelineType
from common.s3_adapter import S3Adapter
from common.s3_ext import s3_uri_splitter
from common.std_ext import NullObject
from common.transcribe import TranscribeExecutor


class TranscribeHandler:
    def __init__(
        self,
        transcribe_executor: TranscribeExecutor,
        s3_adapter: S3Adapter,
        dest_bucket: str,
        purpose: str,
        logger=None,
    ):
        if logger is None:
            logger = NullObject()

        self.transcribe_executor = transcribe_executor
        self.s3_adapter = s3_adapter
        self.dest_bucket = dest_bucket
        self.purpose = purpose
        self.logger = logger

    def __call__(self, event, context):
        """Function entry"""
        self.logger.info(f"Event Received: {json.dumps(event)}")
        metadata = event["Input"]
        wav_url = metadata["wav_url"]
        s3_object_information = s3_uri_splitter(wav_url)
        language = metadata["call_metadata"]["language"]
        base_file_name = s3_object_information.object_key
        contains_call_type = metadata["contains_call_type"]
        output_key = f"{self.purpose}-{base_file_name}.json"
        pipeline_type = PipelineType.from_str(metadata.get("pipeline_type", "batch"))

        if (
            self.s3_adapter.s3_object_exists(self.dest_bucket, output_key)
            and contains_call_type
        ):
            self.logger.info("SKIP transcription job")
            # Shortcut the execution here instead of calling transcribe
            return {
                "TranscriptionJobName": "PASS",
                "metadata": metadata,
                "TranscriptFileUri": output_key,
            }

        response, transcribe_settings = self.transcribe_executor(
            audio_source_path=wav_url,
            language=language,
            output_key=output_key,
            deffer_execution=False,
            pipeline_type=pipeline_type,
        )

        # Append transcription information to extra_metadata
        metadata["extra_metadata"]["transcription"] = transcribe_settings

        # Return 200 statusCode for Success.
        response_string = {
            "TranscriptionJobName": response["TranscriptionJob"][
                "TranscriptionJobName"
            ],
            "metadata": metadata,
            "output_key": output_key,
        }
        return response_string


def build_handler(s3_adapter: S3Adapter, transcribe_client):
    logger = logging.getLogger("call-details-logger")
    logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))
    transcripts_bucket = os.environ["S3_TRANSCRIPTS_BUCKET"]
    number_of_speakers = int(os.environ["NUMBER_OF_SPEAKERS"])
    number_of_alternatives = int(os.environ["NUMBER_OF_ALTERNATIVES"])
    data_access_role_arn = os.environ["DATA_ACCESS_ROLE_ARN"]
    purpose = os.environ["PURPOSE"]
    tags = extract_tags_from_env()

    transcribe_executor = TranscribeExecutor(
        s3_adapter=s3_adapter,
        transcribe_client=transcribe_client,
        dest_bucket=transcripts_bucket,
        number_of_alternatives=number_of_alternatives,
        number_of_speakers=number_of_speakers,
        data_access_role_arn=data_access_role_arn,
        purpose=purpose,
        logger=logger,
        tags=tags,
    )

    transcribe_handler = TranscribeHandler(
        transcribe_executor=transcribe_executor,
        s3_adapter=s3_adapter,
        dest_bucket=transcripts_bucket,
        purpose=purpose,
        logger=logger,
    )

    return lambda_handler(logger=logger)(transcribe_handler)


if not int(os.environ.get("TEST_FLAG", 0)):
    _s3_adapter = S3Adapter(boto3.client("s3"))
    _transcribe_client = boto3.client("transcribe")
    handler = build_handler(
        s3_adapter=_s3_adapter, transcribe_client=_transcribe_client
    )
