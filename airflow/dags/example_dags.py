import os
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.aws_lambda import AwsLambdaInvokeFunctionOperator
from lambda_operator import CustomLambdaFunctionOperator

LAMBDA_FUNCTION_NAME = os.getenv("LAMBDA_FUNCTION_NAME", "example-lambda-function")


def _set_payload(n: int = 10, to_fail: bool = True):
    return json.dumps({"n": n, "to_fail": to_fail})


with DAG(
    dag_id="example_without_logging",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    dagrun_timeout=timedelta(minutes=16),
    max_active_runs=2,
    concurrency=2,
    tags=["logging"],
    catchup=False,
) as dag:
    sync_w_error = AwsLambdaInvokeFunctionOperator(
        task_id="sync_w_error",
        function_name=LAMBDA_FUNCTION_NAME,
        invocation_type="RequestResponse",
        payload=_set_payload(),
        aws_conn_id=None,
    )

    async_w_error = AwsLambdaInvokeFunctionOperator(
        task_id="async_w_error",
        function_name=LAMBDA_FUNCTION_NAME,
        invocation_type="Event",
        payload=_set_payload(),
        aws_conn_id=None,
    )

    [sync_w_error, async_w_error]


with DAG(
    dag_id="example_with_logging",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    dagrun_timeout=timedelta(minutes=16),
    max_active_runs=2,
    concurrency=2,
    tags=["logging"],
    catchup=False,
) as dag:
    sync_w_error = CustomLambdaFunctionOperator(
        task_id="sync_w_error",
        function_name=LAMBDA_FUNCTION_NAME,
        invocation_type="RequestResponse",
        payload=_set_payload(),
        aws_conn_id=None,
    )

    async_w_error = CustomLambdaFunctionOperator(
        task_id="async_w_error",
        function_name=LAMBDA_FUNCTION_NAME,
        invocation_type="Event",
        payload=_set_payload(),
        aws_conn_id=None,
    )

    [sync_w_error, async_w_error]
