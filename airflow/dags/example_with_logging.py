import os
import json
from datetime import datetime

from airflow import DAG
from lambda_operator import CustomLambdaFunctionOperator

LAMBDA_FUNCTION_NAME = os.getenv("LAMBDA_FUNCTION_NAME", "example-lambda-function")


def _set_payload(n: int = 10, to_fail: bool = True):
    return json.dumps({"n": n, "to_fail": to_fail})


with DAG(
    dag_id="example_with_logging",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    max_active_runs=2,
    concurrency=5,
    tags=["logging"],
    catchup=False,
) as dag:
    [
        CustomLambdaFunctionOperator(
            task_id="sync_w_error",
            function_name=LAMBDA_FUNCTION_NAME,
            invocation_type="RequestResponse",
            payload=_set_payload(),
            aws_conn_id=None,
        ),
        CustomLambdaFunctionOperator(
            task_id="async_w_error",
            function_name=LAMBDA_FUNCTION_NAME,
            invocation_type="Event",
            payload=_set_payload(),
            aws_conn_id=None,
        ),
        CustomLambdaFunctionOperator(
            task_id="sync_wo_error",
            function_name=LAMBDA_FUNCTION_NAME,
            invocation_type="RequestResponse",
            payload=_set_payload(to_fail=False),
            aws_conn_id=None,
        ),
        CustomLambdaFunctionOperator(
            task_id="async_wo_error",
            function_name=LAMBDA_FUNCTION_NAME,
            invocation_type="Event",
            payload=_set_payload(to_fail=False),
            aws_conn_id=None,
        ),
        CustomLambdaFunctionOperator(
            task_id="async_timeout_error",
            function_name=LAMBDA_FUNCTION_NAME,
            invocation_type="Event",
            payload=_set_payload(n=40, to_fail=False),
            aws_conn_id=None,
        ),
    ]
