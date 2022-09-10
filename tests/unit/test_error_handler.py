import json
import logging
import os
from http import HTTPStatus
from typing import Any, Dict
from unittest import mock
from unittest.mock import MagicMock

import pytest

from aws_lambda_powertools.utilities.error_handler.constants import DEFAULT_ERROR_MESSAGE, ErrorDestinationEnum
from aws_lambda_powertools.utilities.error_handler.error_destination_interface import ErrorDestinationInterface
from aws_lambda_powertools.utilities.error_handler.error_handler import error_handler
from aws_lambda_powertools.utilities.error_handler.exception import ErrorHandlerException
from aws_lambda_powertools.utilities.error_handler.exception_destination import ExceptionDestination
from aws_lambda_powertools.utilities.error_handler.handler_factory import ErrorHandlerFactory
from aws_lambda_powertools.utilities.error_handler.http_response import DEFAULT_HTTP_ERROR_MESSAGE, HttpResponse
from aws_lambda_powertools.utilities.error_handler.sqs_destination import ERROR_HANDLER_DLQ_URL, SqsDestination
from aws_lambda_powertools.utilities.typing import LambdaContext

TRACE = "my trace"


def get_logger() -> object:
    return logging.getLogger("tests")


def generate_context() -> LambdaContext:
    context = LambdaContext()
    context._aws_request_id = "1"
    context._function_name = "test_function_name"
    return context


def test_error_handler_interface_instantiate():
    with pytest.raises(TypeError) as ex:
        ErrorDestinationInterface(lambda_context=generate_context(), exception=Exception(), logger=None)
    assert ex.match("instantiate abstract class ErrorDestinationInterface with abstract methods")


def test_factory_singleton():
    first = ErrorHandlerFactory()
    second = ErrorHandlerFactory()
    third = ErrorHandlerFactory()
    fourth = ErrorHandlerFactory()
    fifth = ErrorHandlerFactory()
    assert (first is second) and (third is fourth) and (first is third) and (third is fifth)
    assert first._error_handlers[ErrorDestinationEnum.SQS] is second._error_handlers[ErrorDestinationEnum.SQS]


@mock.patch.dict(os.environ, {ERROR_HANDLER_DLQ_URL: "FAKE_SQS"})
def test_sqs_destination_build_error_message():
    sqs: SqsDestination = SqsDestination(
        lambda_context=generate_context(), exception=Exception("I failed"), logger=get_logger(), trace=TRACE
    )
    assert sqs._build_error_message() == {
        "error": "Exception('I failed')",
        "lambda_name": "test_function_name",
        "request_id": "1",
        "traceback": TRACE,
    }


@mock.patch.dict(os.environ, {ERROR_HANDLER_DLQ_URL: "FAKE_SQS"})
def test_factory_initialization_sqs():
    error_handler_factory = ErrorHandlerFactory()
    assert ErrorDestinationEnum.SQS in error_handler_factory._error_handlers.keys()

    mock_lambda_context = MagicMock()
    mock_lambda_context.function_name = "name"
    mock_lambda_context.aws_request_id = "id"
    sqs_error_handler = ErrorHandlerFactory().get_handler(
        handler_type=ErrorDestinationEnum.SQS,
        logger=get_logger(),
        exc=Exception("I failed"),
        context=mock_lambda_context,
        trace=TRACE,
    )
    assert isinstance(sqs_error_handler, SqsDestination)


def test_factory_initialization_http():
    error_handler_factory = ErrorHandlerFactory()
    assert ErrorDestinationEnum.HTTP_RESPONSE in error_handler_factory._error_handlers.keys()

    http_error_handler = ErrorHandlerFactory().get_handler(
        handler_type=ErrorDestinationEnum.HTTP_RESPONSE,
        logger=get_logger(),
        exc=Exception("I failed"),
        context=generate_context(),
        trace=TRACE,
    )
    assert isinstance(http_error_handler, HttpResponse)


def test_factory_initialization_assertion():
    error_handler_factory = ErrorHandlerFactory()
    assert ErrorDestinationEnum.HTTP_RESPONSE in error_handler_factory._error_handlers.keys()

    sqs_error_handler = ErrorHandlerFactory().get_handler(
        handler_type=ErrorDestinationEnum.RAISE_EXCEPTION,
        logger=get_logger(),
        exc=Exception("I failed"),
        context=generate_context(),
        trace=TRACE,
    )
    assert isinstance(sqs_error_handler, ExceptionDestination)


@error_handler(logger_factory=get_logger, destination=ErrorDestinationEnum.RAISE_EXCEPTION)
def handler_exception_type(event: Dict[str, Any], context: LambdaContext) -> Dict[str, Any]:
    raise Exception("test")


def test_raise_exception_handler():
    with pytest.raises(ErrorHandlerException, match=f"{DEFAULT_ERROR_MESSAGE}. original_error=.*"):
        handler_exception_type(event={}, context=generate_context())


@error_handler(logger_factory=get_logger, destination=ErrorDestinationEnum.SQS)
def handler_sqs_type(event: Dict[str, Any], context: LambdaContext) -> Dict[str, Any]:
    raise Exception("test")


def test_sqs_handler_missing_env_var(mocker):
    with pytest.raises(
        ErrorHandlerException, match="missing environment variable for SQS DLQ destination error handler.*"
    ):
        handler_sqs_type(event={}, context=generate_context())


@mock.patch.dict(os.environ, {ERROR_HANDLER_DLQ_URL: "FAKE_SQS"})
def test_sqs_handler(mocker):
    class MockClient:
        @staticmethod
        def send_message(QueueUrl: str, MessageBody: bytes, MessageAttributes: Dict) -> Dict:
            return {}

    sqs = mocker.patch("boto3.client")
    sqs.return_value = MockClient()

    handler_sqs_type(event={}, context=generate_context())


@mock.patch.dict(os.environ, {ERROR_HANDLER_DLQ_URL: "FAKE_SQS"})
def test_sqs_handler_failed_sqs_send_message_catch_exception(mocker):
    # in this test we make bob3 sqs send_message raise an exception and we want to verify
    # that the handler catches the error - so no exception is raised at all
    class MockClient:
        @staticmethod
        def send_message(QueueUrl: str, MessageBody: bytes, MessageAttributes: Dict) -> Dict:
            raise Exception("fail")

    sqs = mocker.patch("boto3.client")
    sqs.return_value = MockClient()

    handler_sqs_type(event={}, context=generate_context())


@error_handler(logger_factory=get_logger, destination=ErrorDestinationEnum.HTTP_RESPONSE)
def handler_http_type(event: Dict[str, Any], context: LambdaContext) -> Dict[str, Any]:
    raise Exception("test")


def test_http_handler(mocker):
    response: Dict = handler_http_type(event={}, context=generate_context())
    assert response["statusCode"] == HTTPStatus.INTERNAL_SERVER_ERROR
    assert response["headers"]["Content-Type"] == "application/json"
    body_dict = json.loads(response["body"])
    assert body_dict["error"] == DEFAULT_HTTP_ERROR_MESSAGE


class CustomErrorHandler(ErrorDestinationInterface):
    def send_error_to_destination(self) -> Any:
        return "test_passed"


@error_handler(logger_factory=get_logger, destination=ErrorDestinationEnum.CUSTOM, custom_handler=CustomErrorHandler)
def handler_custom_type(event: Dict[str, Any], context: LambdaContext) -> Dict[str, Any]:
    raise Exception("test")


def test_custom_handler(mocker):
    response: Dict = handler_custom_type(event={}, context=generate_context())
    assert response == "test_passed"


@error_handler(logger_factory=get_logger, destination=ErrorDestinationEnum.SQS, custom_handler=CustomErrorHandler)
def handler_invalid_definition_custom_type(event: Dict[str, Any], context: LambdaContext) -> Dict[str, Any]:
    raise Exception("test")


def test_invalid_definition_custom_handler(mocker):
    with pytest.raises(
        ErrorHandlerException,
        match="custom destination must be used in conjunction only with ErrorDestinationEnum.CUSTOM",
    ):
        handler_invalid_definition_custom_type(event={}, context=generate_context())
