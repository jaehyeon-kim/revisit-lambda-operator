import time
import json
import functools
from typing import TYPE_CHECKING, Optional
from uuid import uuid4

import boto3
from airflow.providers.amazon.aws.operators.aws_lambda import AwsLambdaInvokeFunctionOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class CustomLambdaFunctionOperator(AwsLambdaInvokeFunctionOperator):
    def __init__(
        self,
        *,
        function_name: str,
        log_type: Optional[str] = None,
        qualifier: Optional[str] = None,
        invocation_type: Optional[str] = None,
        client_context: Optional[str] = None,
        payload: Optional[str] = None,
        aws_conn_id: str = "aws_default",
        correlation_id: str = str(uuid4()),
        **kwargs,
    ):
        super().__init__(
            function_name=function_name,
            log_type=log_type,
            qualifier=qualifier,
            invocation_type=invocation_type,
            client_context=client_context,
            payload=json.dumps(
                {**json.loads((payload or "{}")), **{"correlation_id": correlation_id}}
            ),
            aws_conn_id=aws_conn_id,
            **kwargs,
        )
        self.correlation_id = correlation_id

    def log_processor(func):
        @functools.wraps(func)
        def wrapper_decorator(self, *args, **kwargs):
            payload = func(self, *args, **kwargs)
            function_timeout = self.get_function_timeout()
            self.process_log_events(function_timeout)
            return payload

        return wrapper_decorator

    @log_processor
    def execute(self, context: "Context"):
        return super().execute(context)

    def get_function_timeout(self):
        resp = boto3.client("lambda").get_function_configuration(FunctionName=self.function_name)
        return resp["Timeout"]

    def process_log_events(self, function_timeout: int):
        start_time = 0
        for _ in range(function_timeout):
            response_iterator = self.get_response_iterator(
                self.function_name, self.correlation_id, start_time
            )
            for page in response_iterator:
                for event in page["events"]:
                    start_time = event["timestamp"]
                    message = json.loads(event["message"])
                    print(message)
                    if message["level"] == "ERROR":
                        raise RuntimeError("ERROR found in log")
                    if message["message"] == "Function ended":
                        return
            time.sleep(1)
        raise RuntimeError("Lambda function end message not found after function timeout")

    @staticmethod
    def get_response_iterator(function_name: str, correlation_id: str, start_time: int):
        paginator = boto3.client("logs").get_paginator("filter_log_events")
        return paginator.paginate(
            logGroupName=f"/aws/lambda/{function_name}",
            filterPattern=f'"{correlation_id}"',
            startTime=start_time + 1,
        )
