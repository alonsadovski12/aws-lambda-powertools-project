#pylint: disable=no-name-in-module,unused-argument,line-too-long
import json
import os
from typing import Any

import boto3
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.error_handler.constants import DEFAULT_ERROR_MESSAGE
from aws_lambda_powertools.utilities.error_handler.error_destination_interface import ErrorDestinationInterface
from aws_lambda_powertools.utilities.error_handler.exception import ErrorHandlerException

ERROR_HANDLER_DLQ_URL = 'ERROR_HANDLER_DLQ_URL'


class SqsDestination(ErrorDestinationInterface):

    def __init__(self, lambda_context: LambdaContext, exception: Exception, trace: str, logger: object):
        super().__init__(lambda_context, exception, trace, logger)

        # check environment variables
        self.sqs_url = os.environ.get(ERROR_HANDLER_DLQ_URL, '')
        if not self.sqs_url:
            error_str = f'missing environment variable for SQS DLQ destination error handler, missing {ERROR_HANDLER_DLQ_URL}'
            self._logger.error(error_str)
            raise ErrorHandlerException(error_str)
        logger.debug(f'setting SQS DLQ url to {ERROR_HANDLER_DLQ_URL}')

    def send_error_to_destination(self) -> Any:
        original_error = self._build_error_message()
        self._logger.error(
            f'{DEFAULT_ERROR_MESSAGE}. sending unhandled exception to SQS, destination={self.sqs_url}, original_error={original_error}')
        try:
            client = boto3.client('sqs')
            client.send_message(
                QueueUrl=self.sqs_url,
                MessageBody=json.dumps(original_error),
                MessageAttributes={
                    'request_id': {
                        'DataType': 'String',
                        'StringValue': self._request_id,
                    },
                    'sender': {
                        'DataType': 'String',
                        'StringValue': 'error_handler',
                    }
                },
            )
        except Exception as exc:  # generic as possible since can fail on unknown issues - permissions etc.
            error_str = f'failed to send error event to SQS, boto_exception={str(exc)}, destination={self.sqs_url}'
            self._logger.error(error_str)
            return
        self._logger.info(f'sent error to SQS successfully, request_id={self._request_id}, destination={self.sqs_url}')
