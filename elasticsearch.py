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

def create_es_client(
    host: str,
    auth: AWS4Auth | Dict | None = None,
    use_ssl: bool = True,
    logger=None,
) -> ElasticSearchV2:
    """Creates an Elasticsearch client."""
    
    es_client = ElasticSearchV2(host=host, auth=auth, use_ssl=use_ssl, logger=logger)
    return es_client
