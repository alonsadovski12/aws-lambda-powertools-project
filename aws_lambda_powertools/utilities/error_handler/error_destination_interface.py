#pylint: disable=no-name-in-module,unused-argument
from abc import ABCMeta, abstractmethod
from typing import Any, Dict

from aws_lambda_context import LambdaContext


class ErrorDestinationInterface(metaclass=ABCMeta):

    def __init__(self, lambda_context: LambdaContext, exception: Exception, trace: str, logger: object):
        self._lambda_name = lambda_context.function_name
        self._request_id = lambda_context.aws_request_id
        self._logger = logger
        self._exception = exception
        self._trace = trace

    def _build_error_message(self) -> Dict[str, str]:
        return {'error': repr(self._exception), 'lambda_name': self._lambda_name, 'request_id': self._request_id, 'traceback': self._trace}

    @abstractmethod
    def send_error_to_destination(self) -> Any:
        """ sends an error message to a destination corresponding the class instance """
        raise NotImplementedError
