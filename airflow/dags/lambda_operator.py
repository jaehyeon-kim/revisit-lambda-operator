import os
import time
import json
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

    def execute(self, context: "Context"):
        hook = LambdaHook(aws_conn_id=self.aws_conn_id)
        success_status_codes = [200, 202, 204]
        self.log.info(
            "Invoking AWS Lambda function: %s with payload: %s", self.function_name, self.payload
        )
        response = hook.invoke_lambda(
            function_name=self.function_name,
            invocation_type=self.invocation_type,
            log_type=self.log_type,
            client_context=self.client_context,
            payload=self.payload,
            qualifier=self.qualifier,
        )
        self.log.info("Lambda response metadata: %r", response.get("ResponseMetadata"))
        if response.get("StatusCode") not in success_status_codes:
            raise ValueError(
                "Lambda function did not execute", json.dumps(response.get("ResponseMetadata"))
            )
        payload_stream = response.get("Payload")
        payload = payload_stream.read().decode()
        if "FunctionError" in response:
            raise ValueError(
                "Lambda function execution resulted in error",
                {"ResponseMetadata": response.get("ResponseMetadata"), "Payload": payload},
            )
        self.log.info("Lambda function invocation succeeded: %r", response.get("ResponseMetadata"))
        self._process_log_events()
        return payload

    def _get_function_timeout(self):
        lambda_client = boto3.client("lambda")
        resp = lambda_client.get_function_configuration(FunctionName=self.function_name)
        return resp["Timeout"]

    def _process_log_events(self):
        paginator = boto3.client("logs").get_paginator("filter_log_events")

        timeout = self._get_function_timeout()
        status = None
        start_time = 0
        for _ in range(timeout):
            response_iterator = paginator.paginate(
                logGroupName=f"/aws/lambda/{self.function_name}",
                filterPattern=f'"correlation_id" "{self.correlation_id}"',
                startTime=start_time + 1,
                # PaginationConfig={"MaxItems": 3},
            )
            status, start_time = self._parse_events(response_iterator)
            if status == "succeeded":
                break
            time.sleep(1)
        if status != "succeeded":
            raise RuntimeError("Lambda function end message not found after function timeout")

    def _parse_events(self, response_iterator: Any):
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
