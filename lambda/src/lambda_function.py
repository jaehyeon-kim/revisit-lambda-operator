import time
import functools
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.middleware_factory import lambda_handler_decorator

logger = Logger()


@lambda_handler_decorator
def middleware_before_after(handler, event, context):
    logger.info("Function started")
    response = handler(event, context)
    logger.info("Function ended")
    return response


@logger.inject_lambda_context(correlation_id_path="correlation_id")
@middleware_before_after
def lambda_handler(event: dict, context: LambdaContext):
    num_iter = event.get("n", 10)
    to_fail = event.get("to_fail", False)
    logger.info(f"num_iter - {num_iter}, fail - {to_fail}")
    try:
        for n in range(num_iter):
            logger.info(f"iter - {n + 1}...")
            time.sleep(1)
        if to_fail:
            raise Exception
    except Exception as e:
        logger.exception("Function invocation failed...")
        raise RuntimeError("Unable to finish loop") from e
