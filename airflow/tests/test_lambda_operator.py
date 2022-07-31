import json
import pytest
from unittest.mock import MagicMock
from dags.lambda_operator import CustomLambdaFunctionOperator


@pytest.fixture
def log_events():
    def _(case):
        events = [
            {
                "timestamp": 1659296879605,
                "message": '{"correlation_id":"2850fda4-9005-4375-aca8-88dfdda222ba","level":"INFO","message":"Function started","location":"middleware_before_after:12","timestamp":"2022-07-31 19:47:59,605+0000","service":"airflow","cold_start":true,"function_name":"example-lambda-function","function_memory_size":"128","function_arn":"arn:aws:lambda:ap-southeast-2:000000000000:function:example-lambda-function","function_request_id":"200ec70e-6c4b-45c3-9ca1-77b0c9856760","xray_trace_id":"1-62e6dc6f-172fb3b7528eb5b73d999a45"}\n',
            },
            {
                "timestamp": 1659296879605,
                "message": '{"correlation_id":"2850fda4-9005-4375-aca8-88dfdda222ba","level":"INFO","message":"num_iter - 10, fail - False","location":"lambda_handler:23","timestamp":"2022-07-31 19:47:59,605+0000","service":"airflow","cold_start":true,"function_name":"example-lambda-function","function_memory_size":"128","function_arn":"arn:aws:lambda:ap-southeast-2:000000000000:function:example-lambda-function","function_request_id":"200ec70e-6c4b-45c3-9ca1-77b0c9856760","xray_trace_id":"1-62e6dc6f-172fb3b7528eb5b73d999a45"}\n',
            },
            {
                "timestamp": 1659296879605,
                "message": '{"correlation_id":"2850fda4-9005-4375-aca8-88dfdda222ba","level":"INFO","message":"iter - 1...","location":"lambda_handler:26","timestamp":"2022-07-31 19:47:59,605+0000","service":"airflow","cold_start":true,"function_name":"example-lambda-function","function_memory_size":"128","function_arn":"arn:aws:lambda:ap-southeast-2:000000000000:function:example-lambda-function","function_request_id":"200ec70e-6c4b-45c3-9ca1-77b0c9856760","xray_trace_id":"1-62e6dc6f-172fb3b7528eb5b73d999a45"}\n',
            },
        ]
        if case == "success":
            events.append(
                {
                    "timestamp": 1659296889620,
                    "message": '{"correlation_id":"2850fda4-9005-4375-aca8-88dfdda222ba","level":"INFO","message":"Function ended","location":"middleware_before_after:14","timestamp":"2022-07-31 19:48:09,619+0000","service":"airflow","cold_start":true,"function_name":"example-lambda-function","function_memory_size":"128","function_arn":"arn:aws:lambda:ap-southeast-2:000000000000:function:example-lambda-function","function_request_id":"200ec70e-6c4b-45c3-9ca1-77b0c9856760","xray_trace_id":"1-62e6dc6f-172fb3b7528eb5b73d999a45"}\n',
                }
            )
        elif case == "error":
            events.append(
                {
                    "timestamp": 1659296889629,
                    "message": '{"correlation_id":"2850fda4-9005-4375-aca8-88dfdda222ba","level":"ERROR","message":"Function invocation failed...","location":"lambda_handler:31","timestamp":"2022-07-31 19:48:09,628+0000","service":"airflow","cold_start":true,"function_name":"example-lambda-function","function_memory_size":"128","function_arn":"arn:aws:lambda:ap-southeast-2:000000000000:function:example-lambda-function","function_request_id":"0beb985f-1150-4db1-93c2-adc9aac3a0c3","exception":"Traceback (most recent call last):\\n  File \\"/var/task/lambda_function.py\\", line 29, in lambda_handler\\n    raise Exception\\nException","exception_name":"Exception","xray_trace_id":"1-62e6dc6f-30b8e51d000de0ee5a22086b"}\n',
                },
            )
        return [{"events": events}]

    return _


def test_process_log_events_success(log_events):
    success_resp = log_events("success")
    operator = CustomLambdaFunctionOperator(
        task_id="sync_w_error",
        function_name="",
        invocation_type="RequestResponse",
        payload=json.dumps({"n": 1, "to_fail": True}),
        aws_conn_id=None,
    )
    operator.get_response_iterator = MagicMock(return_value=success_resp)
    assert operator.process_log_events(1) == None


def test_process_log_events_fail_with_error(log_events):
    fail_resp = log_events("error")
    operator = CustomLambdaFunctionOperator(
        task_id="sync_w_error",
        function_name="",
        invocation_type="RequestResponse",
        payload=json.dumps({"n": 1, "to_fail": True}),
        aws_conn_id=None,
    )
    operator.get_response_iterator = MagicMock(return_value=fail_resp)
    with pytest.raises(RuntimeError) as e:
        operator.process_log_events(1)
    assert "ERROR found in log" == str(e.value)


def test_process_log_events_fail_by_timeout(log_events):
    fail_resp = log_events(None)
    operator = CustomLambdaFunctionOperator(
        task_id="sync_w_error",
        function_name="",
        invocation_type="RequestResponse",
        payload=json.dumps({"n": 1, "to_fail": True}),
        aws_conn_id=None,
    )
    operator.get_response_iterator = MagicMock(return_value=fail_resp)
    with pytest.raises(RuntimeError) as e:
        operator.process_log_events(1)
    assert "Lambda function end message not found after function timeout" == str(e.value)
