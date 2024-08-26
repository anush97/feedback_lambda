import json
from functools import wraps

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
                return {"statusCode": 400, "body": json.dumps({"error": "BAD REQUEST"})}
        return fn(event, *args)

    return wrapped
