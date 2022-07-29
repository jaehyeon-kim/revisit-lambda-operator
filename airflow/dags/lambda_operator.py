import time
import json
import functools
from typing import TYPE_CHECKING, Optional, Any
from uuid import uuid4

import boto3
from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook
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
            payload=payload,
            aws_conn_id=aws_conn_id,
            **kwargs,
        )
        self.correlation_id = correlation_id

    def _log_processor(func):
        @functools.wraps(func)
        def wrapper_decorator(self, *args, **kwargs):
            payload = func(self, *args, **kwargs)
            self._process_log_events()
            return payload

        return wrapper_decorator

    @_log_processor()
    def execute(self, context: "Context"):
        return super().execute(context)

    def _process_log_events(self):
        paginator = boto3.client("logs").get_paginator("filter_log_events")
        timeout = self._get_function_timeout()
        status, start_time = None, 0
        for _ in range(timeout):
            response_iterator = paginator.paginate(
                logGroupName=f"/aws/lambda/{self.function_name}",
                filterPattern=f'"correlation_id" "{self.correlation_id}"',
                startTime=start_time + 1,
                # PaginationConfig={"MaxItems": 3},
            )
            status, start_time = self._parse_log_events(response_iterator)
            if status == "succeeded":
                break
            time.sleep(1)
        if status != "succeeded":
            raise RuntimeError("Lambda function end message not found after function timeout")

    def _get_function_timeout(self):
        lambda_client = boto3.client("lambda")
        resp = lambda_client.get_function_configuration(FunctionName=self.function_name)
        return resp["Timeout"]

    def _parse_log_events(self, response_iterator: Any):
        status = None
        timestamp = 0
        for page in response_iterator:
            for event in page["events"]:
                timestamp = event["timestamp"]
                message = json.loads(event["message"])
                print(message)
                if message["level"] == "ERROR":
                    raise RuntimeError("ERROR found in log")
                if message["message"] == "Function ended":
                    status = "succeeded"
                    break
        return status, timestamp
