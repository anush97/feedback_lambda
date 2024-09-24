class ElasticSearchV2:
    def __init__(
        self,
        host: str,
        auth: AWS4Auth | Dict | None = None,
        use_ssl: bool = True,
        logger=None,
    ):
        if logger is None:
            logger = NullObject()

        protocol = "https" if use_ssl else "http"
        self.es_url = f"{protocol}://{host}"
        self.logger = logger
        # Ensure auth is properly handled
        self.auth = self.__ensure_auth(auth)
        self.session = self.__create_session(self.auth)

    def __ensure_auth(self, auth: AWS4Auth | Dict | None) -> AWS4Auth | Dict | None:
        """Ensure valid authentication for Elasticsearch."""
        if isinstance(auth, dict):
            # If auth is a dict, assume it's custom headers (e.g., tokens) for simpler auth.
            self.logger.info("Using a dictionary for authentication headers.")
            return auth
        elif not auth or isinstance(auth, NullObject):
            # Handle case where no auth is provided
            self.logger.warning(
                "User credentials are required but were not provided. Continuing without credentials for testing purposes."
            )
            return None
        else:
            self.logger.info("User credentials provided for Elasticsearch.")
            return auth

    def __create_session(self, auth: AWS4Auth | Dict | None = None) -> Session:
        """Create and configure a requests session."""
        session = Session()
        session.headers = ES_HEADERS
        # If auth is a dict, assume it's custom headers (like API tokens) and add them to session headers
        if isinstance(auth, dict):
            self.logger.info("Adding custom authentication headers to the session.")
            session.headers.update(auth)  # Add the dictionary content to headers
        else:
            session.auth = auth  # Use AWS4Auth if available
        return session

    def __request(self, verb: str, endpoint: str, body: Dict = None) -> Response:
        """Make a request to Elasticsearch."""
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
