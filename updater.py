from typing import Callable

from common.admin.dynamodb_mapper import (
    DynamoDBMapper,
    create_epoch_time_to_live,
    convert_datetime_to_epoch_time,
)
from common.models.transcribe_on_request import (
    TranscribeOnRequestJob,
    TranscribeJobStatus,
)
from common.std_ext import NullObject


class OnRequestJobUpdater:
    def __init__(
        self, dynamodb_mapper: DynamoDBMapper, dynamodb_status_table: str, logger=None
    ):
        self.dynamodb_mapper = dynamodb_mapper
        self.dynamodb_status_table = dynamodb_status_table
        if logger is None:
            logger = NullObject()
        self.logger = logger

    def __create_transcribe_job(
        self, job_id: str, user_email: str, last_update: int, expire_at: int
    ) -> Callable[[str], TranscribeOnRequestJob]:
        return lambda call_id: TranscribeOnRequestJob(
            callId=call_id,
            jobId=job_id,
            userId=user_email,
            lastUpdate=last_update,
            expireAt=expire_at,
            status=TranscribeJobStatus.IN_PROGRESS.value,
        )

    def __call__(
        self, job_id: str, call_ids: list[str], user_email: str, days_to_expire: int
    ) -> None:
        epoch_time = convert_datetime_to_epoch_time()
        epoch_ttl = create_epoch_time_to_live(days_to_expire=days_to_expire)

        jobs = map(
            self.__create_transcribe_job(job_id, user_email, epoch_time, epoch_ttl),
            call_ids,
        )
        dynamodb_items = map(
            lambda job: self.dynamodb_mapper.serialize(job.model_dump()), jobs
        )
        self.logger.info("Creating job %s with call_ids %s", job_id, call_ids)
        self.dynamodb_mapper.write_batch(self.dynamodb_status_table, dynamodb_items)
