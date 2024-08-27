import json
from functools import wraps
from http import HTTPStatus
import logging
from pydantic import ValidationError  # Import ValidationError to handle it explicitly

# Logger setup
log_level = os.environ.get("LOG_LEVEL", "INFO")
logging.root.setLevel(logging.getLevelName(log_level))


# JSON body loader decorator
def load_json_body(fn):
    @wraps(fn)
    def wrapped(event, *args):
        if isinstance(event.get("body"), str):
            context = args[0]
            try:
                event["body"] = json.loads(event["body"])
            except Exception as exception:
                if hasattr(context, "serverless_sdk"):
                    context.serverless_sdk.capture_exception(exception)
                return {"statusCode": HTTPStatus.BAD_REQUEST.value, "body": json.dumps({"error": "BAD REQUEST"})}
        return fn(event, *args)
    return wrapped


# Combined lambda_handler decorator with error handling and load_json_body
def lambda_handler(
    error_status=None,
    logging_fn=None,
):
    if error_status is None:
        error_status = [(Exception, HTTPStatus.INTERNAL_SERVER_ERROR.value)]

    status_code_map = dict(error_status)

    def decorator(fn):
        nonlocal logging_fn
        if logging_fn is None:
            logging_fn = logging.getLogger(fn.__name__).error

        @load_json_body
        @wraps(fn)
        def wrapped(event, *args, **kwargs):
            try:
                # Call the handler function
                response = fn(event, *args, **kwargs)
                if isinstance(response, tuple) and len(response) == 2:
                    status_code, body = response
                else:
                    status_code, body = HTTPStatus.OK.value, response
            except ValidationError as e:  # Explicitly catch ValidationError
                logging_fn(f"Validation Error: {repr(e)}", exc_info=True)
                status_code = HTTPStatus.BAD_REQUEST.value  # Set status code to 400
                body = {"errorMessage": str(e)}
            except Exception as e:
                # Handle all other exceptions
                error_type = next((cls for cls in type(e).__mro__ if cls in status_code_map), Exception)
                status_code = status_code_map.get(error_type, HTTPStatus.INTERNAL_SERVER_ERROR.value)
                logging_fn(f"Error: {repr(e)}", exc_info=True)
                body = {"errorMessage": str(e) if status_code != HTTPStatus.INTERNAL_SERVER_ERROR else "An internal error occurred."}

            return {
                "statusCode": status_code,
                "body": json.dumps(body),
            }

        return wrapped

    return decorator
