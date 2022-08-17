#pylint: disable=no-name-in-module,unused-argument,line-too-long,useless-super-delegation
import json
from http import HTTPStatus
from typing import Any

from aws_lambda_powertools.utilities.error_handler.constants import DEFAULT_ERROR_MESSAGE
from aws_lambda_powertools.utilities.error_handler.error_destination_interface import ErrorDestinationInterface

DEFAULT_HTTP_ERROR_MESSAGE = 'internal server error'


class HttpResponse(ErrorDestinationInterface):

    def send_error_to_destination(self) -> Any:
        original_error: str = json.dumps(self._build_error_message())
        self._logger.error(
            f'{DEFAULT_ERROR_MESSAGE}. returning HTTP response, status_code={HTTPStatus.INTERNAL_SERVER_ERROR}, original_error={original_error}'
        )
        return {
            'statusCode': HTTPStatus.INTERNAL_SERVER_ERROR,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                'error': DEFAULT_HTTP_ERROR_MESSAGE,
            }),
        }
