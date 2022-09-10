from enum import Enum


class ErrorDestinationEnum(str, Enum):
    SQS = "SQS"
    HTTP_RESPONSE = "HTTP_RESPONSE"
    RAISE_EXCEPTION = "RAISE_EXCEPTION"
    CUSTOM = "CUSTOM"


DEFAULT_ERROR_MESSAGE = "lambda function raised an unhandled exception"
