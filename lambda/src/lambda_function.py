import time
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

logger = Logger()


@logger.inject_lambda_context
def lambda_handler(event: dict, context: LambdaContext):
    correlation_id = event["correlation_id"]
    num_iter = event.get("n", 10)
    to_fail = event.get("to_fail", False)
    logger.set_correlation_id(correlation_id)
    logger.info(f"num_iter - {num_iter}, fail - {to_fail}")
    try:
        for n in range(num_iter):
            logger.info(f"iter - {n + 1}...")
            if to_fail and (n + 1) == num_iter:
                raise Exception
            time.sleep(1)
    except Exception as e:
        logger.exception("Function invocation failed...")
        raise RuntimeError("Unable to finish loop") from e
