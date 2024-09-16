import copy
import json
import logging
import os
import warnings
from typing import Dict
from requests.exceptions import HTTPError

import boto3
from requests import Session, Response
from requests_aws4auth import AWS4Auth

from common.errors import AccessDeniedError, ElasticsearchFailedRequestError
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

class ElasticSearch:
    def __init__(
        self, host, index, requests, auth, results_map, protocol="https", logger=None
    ):
        warnings.warn("Deprecated Class, use ElasticSearchV2", DeprecationWarning)
        self.host = host
        self.index = index  # Use alias as index
        self.requests = requests
        self.auth = auth
        if logger is None:
            logger = NullObject()
        self.logger = logger
        self.url = f"{protocol}://{self.host}/{self.index}"
        self.results_map = results_map
        self.response = None
        self.results = []
        self.total = 0
        self.headers = ES_HEADERS

    def query(self, query):
        self.logger.info(f"Query: {json.dumps(query, indent=2)}")
        # Make the signed HTTP request
        es_response = self.requests.get(
            f"{self.url}/_search",
            auth=self.auth,
            headers=self.headers,
            data=json.dumps(query),
        )
        self.response = json.loads(es_response.text)
        to_log = copy.deepcopy(self.response)

        if "hits" in self.response and "hits" in self.response["hits"]:
            to_log["hits"]["hits"] = "omitted"
            hits = map(self.results_map, self.response["hits"]["hits"])
            hits = filter(lambda x: isinstance(x, dict), hits)
            self.results = list(hits)
            self.total = self.response["hits"]["total"]["value"]

        self.logger.info(f"ElasticSearch results: {to_log}")

class ElasticSearchV2:
    def __init__(
        self, host: str, auth: AWS4Auth | Dict = None, use_ssl: bool = True, logger=None
    ):
        if logger is None:
            logger = NullObject()

        protocol = "https" if use_ssl else "http"
        self.es_url = f"{protocol}://{host}"
        self.logger = logger
        # Ensure that user credentials are provided and valid
        self.auth = self.__ensure_auth(auth)
        self.session = self.__create_session(self.auth)

    def __ensure_auth(self, auth: AWS4Auth | Dict) -> AWS4Auth | Dict:
        """Ensure valid authentication for Elasticsearch."""
        if not auth or isinstance(auth, NullObject):
            self.logger.error("User credentials are required and were not provided.")
            raise ValueError("Elasticsearch user credentials are required.")
        else:
            self.logger.info("User credentials provided for Elasticsearch.")
            return auth

    def __create_session(self, auth: AWS4Auth | Dict = None) -> Session:
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

    def validate_user_access(self, user_groups: list) -> bool:
        """Validate if the user has access to transcribe calls."""
        try:
            # Construct an Elasticsearch query to check user's group access
            query = {
                "query": {"bool": {"should": [{"terms": {"user_group": user_groups}}]}},
                "size": 1,  # Limit to 1 to quickly check if access exists
            }
            # Query Elasticsearch for access validation
            response = self.__request(
                verb="GET", endpoint=f"{self.es_url}/access-rights/_search", body=query
            )
            es_response = json.loads(response.text)

            # If hits are found, user group has access
            if es_response["hits"]["total"]["value"] > 0:
                return True

            # Log and return False if the user doesn't have access
            self.logger.error("User does not have the rights to transcribe calls.")
            return False

        except Exception as e:
            self.logger.error(f"Error during user group access validation: {e}")
            return False

    # Important Functions Preserved
    def request(self, verb: str, endpoint: str, body: Dict = None) -> Dict:
        """Generic request function."""
        response = self.__request(verb, endpoint, body)
        return json.loads(response.text)

    def get_document(self, index: str, _id: str) -> Dict:
        endpoint = f"{index}/_doc/{_id}"
        response = self.__request(verb="GET", endpoint=endpoint)
        return json.loads(response.text)

    def search_documents(self, index: str, query: Dict) -> Dict:
        endpoint = f"{index}/_search"
        response = self.__request(verb="GET", endpoint=endpoint, body=query)
        return json.loads(response.text)

    def add_document(self, index: str, _id: str, document: Dict) -> Dict:
        """Create a full document."""
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
        """Overwrite or Create a full document."""
        endpoint = f"{index}/_update/{_id}?retry_on_conflict={max_retries}"
        response = self.__request(verb="POST", endpoint=endpoint, body=document)
        return json.loads(response.text)

    def update_partial_document(
        self, index: str, _id: str, partial_document: Dict, max_retries: int = 3
    ) -> Dict:
        """Update a partial section of a document."""
        endpoint = f"{index}/_update/{_id}?retry_on_conflict={max_retries}"
        updated_document = {"doc": partial_document}
        response = self.__request(verb="POST", endpoint=endpoint, body=updated_document)
        return json.loads(response.text)

    def update_partial_document_by_query(
        self, index: str, _id: str, update_query: Dict, max_retries: int = 3
    ) -> Dict:
        """Update a partial section of a document using a script."""
        endpoint = f"{index}/_update/{_id}?retry_on_conflict={max_retries}"
        response = self.__request(verb="POST", endpoint=endpoint, body=update_query)
        return json.loads(response.text)

    def update_documents_by_query(
        self, index: str, update_query: Dict, max_retries: int = 3
    ) -> Dict:
        """Update multiple documents by an update query."""
        endpoint = f"{index}/_update_by_query/?retry_on_conflict={max_retries}"
        response = self.__request(verb="POST", endpoint=endpoint, body=update_query)
        return json.loads(response.text)

def create_es_client(
    host: str,
    user_groups: list,
    auth: AWS4Auth | Dict,
    use_ssl: bool = True,
    logger=None,
) -> ElasticSearchV2:
    """Creates an Elasticsearch client and performs early access validation."""
    es_client = ElasticSearchV2(host=host, auth=auth, use_ssl=use_ssl, logger=logger)

    # Perform early access validation
    if not es_client.validate_user_access(user_groups):
        raise AccessDeniedError("User group does not have access to transcribe calls.")

    return es_client
