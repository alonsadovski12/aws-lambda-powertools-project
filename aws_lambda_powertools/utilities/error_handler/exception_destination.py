#pylint: disable=no-name-in-module,unused-argument,useless-super-delegation
import json
from typing import Any

from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.error_handler.constants import DEFAULT_ERROR_MESSAGE
from aws_lambda_powertools.utilities.error_handler.error_destination_interface import ErrorDestinationInterface
from aws_lambda_powertools.utilities.error_handler.exception import ErrorHandlerException


class ExceptionDestination(ErrorDestinationInterface):
    """ This destination will log the unhandled exception and raise ErrorHandlerException that contains the exception details
    """

    def __init__(
        self,
        lambda_context: LambdaContext,
        exception: Exception,
        trace: str,
        logger: object,
    ):
        super().__init__(lambda_context, exception, trace, logger)

    def send_error_to_destination(self) -> Any:
        original_error = self._build_error_message()
        error_str = f'{DEFAULT_ERROR_MESSAGE}. original_error={json.dumps(original_error)}'
        self._logger.error(error_str)
        raise ErrorHandlerException(error_str)
