import json
from typing import Optional

from pydantic import BaseModel, RootModel


class CallMetadata(BaseModel):
    sid: Optional[str] = None
    original_contact_id: str
    duration: int
    total_hold_time: int
    start_datetime: str
    end_datetime: str
    agent_pbxid: str
    extension: str
    agent_full_name: str
    agent_email: str
    language: str
    region: str
    distributor_number: str
    call_context: str
    line_of_business: str
    video_recorded: bool = False
    customer_phone_number: str
    call_direction: str
    organization_unit: str
    queue_id: Optional[str] = ""
    company_number: str
    wav_url: Optional[str] = None
    filename_prefix: str
    created_at_: str


class CallMetadataList(RootModel):
    root: list[CallMetadata]


class OnRequestEventModel(BaseModel):
    on_request_job_id: Optional[str] = None
    on_request_job_user: str
    call_metadata: CallMetadata

    def to_sqs_message(self) -> dict:
        merged_dicts = {
            **self.call_metadata.model_dump(),
            **self.model_dump(include={"on_request_job_id", "on_request_job_user"}),
            "wav_url": self.call_metadata.wav_url,
        }
        return {
            "body": json.dumps({"Records": [merged_dicts]}),
            "attributes": {},
        }
