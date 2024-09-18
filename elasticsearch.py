import copy
import json
import logging
import warnings
from typing import Dict
from requests.exceptions import HTTPError

import boto3
from requests import Session, Response
from requests_aws4auth import AWS4Auth

from common.errors import AccessDeniedError
from common.search_query_template import OrderField
from common.std_ext import NullObject

# Set log level to INFO
log_level = os.environ.get("LOG_LEVEL", "INFO")
logging.root.setLevel(logging.getLevelName(log_level))
_logger = logging.getLogger(__name__)

AWS_REGION = "ca-central-1"
ES_HEADERS = {"Content-Type": "application/json"}

def append_order_by(query_dict: dict, order_field: OrderField) -> dict:
    query_sort = {
        order_field.field: {
            "order": order_field.direction,
            "missing": order_field.missing,
        }
    }
    query_dict["sort"] = [query_sort]
    return query_dict

def offset_paginator_factory(limit=10, strategy="offset"):
    def append_pagination_with_from_size(query_dict, offset=0):
        query_dict["size"] = limit
        query_dict["from"] = offset
        return query_dict

    if strategy == "offset":
        return append_pagination_with_from_size

class ElasticsearchFailedRequestError(Exception):
    pass

class ElasticSearchV2:
    def __init__(
        self, host: str, auth: AWS4Auth | Dict | None = None, use_ssl: bool = True, logger=None
    ):
        if logger is None:
            logger = NullObject()

        protocol = "https" if use_ssl else "http"
        self.es_url = f"{protocol}://{host}"
        self.logger = logger
        # Ensure that user credentials are provided and valid, but continue for tests
        self.auth = self.__ensure_auth(auth)
        self.session = self.__create_session(self.auth)

    def __ensure_auth(self, auth: AWS4Auth | Dict | None) -> AWS4Auth | Dict | None:
        """Ensure valid authentication for Elasticsearch."""
        if not auth or isinstance(auth, NullObject):
            self.logger.warning("User credentials are required but were not provided. Continuing without credentials for testing purposes.")
            return None
        else:
            self.logger.info("User credentials provided for Elasticsearch.")
            return auth

    def __create_session(self, auth: AWS4Auth | Dict | None = None) -> Session:
        session = Session()
        session.headers = ES_HEADERS
        session.auth = auth
        return session

    def __request(self, verb: str, endpoint: str, body: Dict = None) -> Response:
        if body is not None:
            body = json.dumps(body)

        self.logger.info("Elasticsearch request: %s %s/%s", verb, self.es_url, endpoint)
        self.logger.info("Elasticsearch body: %s", body)

        try:
            response = self.session.request(
                method=verb, url=f"{self.es_url}/{endpoint}", data=body
            )
            response.raise_for_status()
        except HTTPError as http_err:
            self.logger.error(f"HTTP error occurred: {http_err}")
            if response.status_code == 403:
                raise AccessDeniedError(
                    "403 Forbidden: Access to Elasticsearch denied."
                )
            else:
                raise ElasticsearchFailedRequestError(
                    f"HTTP error {response.status_code} from Elasticsearch: {response.text}"
                )
        except Exception as e:
            self.logger.error(f"Elasticsearch error: {e}")
            raise ElasticsearchFailedRequestError(
                f"Error with Elasticsearch server: {str(e)}"
            )

        return response

    def request(self, verb: str, endpoint: str, body: Dict = None) -> Dict:
        """Generic request function."""
        response = self.__request(verb, endpoint, body)
        return json.loads(response.text)

    def get_document(self, index: str, _id: str) -> Dict:
        """Retrieve a document from Elasticsearch by ID."""
        endpoint = f"{index}/_doc/{_id}"
        response = self.__request(verb="GET", endpoint=endpoint)
        return json.loads(response.text)

    def search_documents(self, index: str, query: Dict) -> Dict:
        """Search for documents in Elasticsearch based on a query."""
        endpoint = f"{index}/_search"
        response = self.__request(verb="GET", endpoint=endpoint, body=query)
        return json.loads(response.text)

    def add_document(self, index: str, _id: str, document: Dict) -> Dict:
        """Create a full document in Elasticsearch."""
        endpoint = f"{index}/_doc/{_id}"
        response = self.__request(
            verb="PUT",
            endpoint=endpoint,
            body=document,
        )
        return json.loads(response.text)

    def update_document(
        self, index: str, _id: str, document: Dict, max_retries: int = 3
    ) -> Dict:
        """Overwrite or create a full document in Elasticsearch."""
        endpoint = f"{index}/_update/{_id}?retry_on_conflict={max_retries}"
        response = self.__request(verb="POST", endpoint=endpoint, body=document)
        return json.loads(response.text)

    def update_partial_document(
        self, index: str, _id: str, partial_document: Dict, max_retries: int = 3
    ) -> Dict:
        """Update a partial section of a document in Elasticsearch."""
        endpoint = f"{index}/_update/{_id}?retry_on_conflict={max_retries}"
        updated_document = {"doc": partial_document}
        response = self.__request(verb="POST", endpoint=endpoint, body=updated_document)
        return json.loads(response.text)

    def update_partial_document_by_query(
        self, index: str, _id: str, update_query: Dict, max_retries: int = 3
    ) -> Dict:
        """Update a partial section of a document using a script in Elasticsearch."""
        endpoint = f"{index}/_update/{_id}?retry_on_conflict={max_retries}"
        response = self.__request(verb="POST", endpoint=endpoint, body=update_query)
        return json.loads(response.text)

    def update_documents_by_query(
        self, index: str, update_query: Dict, max_retries: int = 3
    ) -> Dict:
        """Update multiple documents in Elasticsearch by an update query."""
        endpoint = f"{index}/_update_by_query/?retry_on_conflict={max_retries}"
        response = self.__request(verb="POST", endpoint=endpoint, body=update_query)
        return json.loads(response.text)

def create_es_client(
    host: str,
    auth: AWS4Auth | Dict | None = None,
    use_ssl: bool = True,
    logger=None,
) -> ElasticSearchV2:
    """Creates an Elasticsearch client."""

    # Use boto3 to get AWS credentials if auth is not provided
    if not auth:
        credentials = boto3.Session().get_credentials()
        auth = AWS4Auth(
            credentials.access_key,
            credentials.secret_key,
            AWS_REGION,
            "es",
            session_token=credentials.token,
        )

    es_client = ElasticSearchV2(host=host, auth=auth, use_ssl=use_ssl, logger=logger)
    return es_client
