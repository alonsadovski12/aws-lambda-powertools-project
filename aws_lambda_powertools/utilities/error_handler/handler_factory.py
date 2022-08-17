from aws_lambda_context import LambdaContext

from aws_lambda_powertools.utilities.error_handler.constants import ErrorDestinationEnum
from aws_lambda_powertools.utilities.error_handler.error_destination_interface import ErrorDestinationInterface
from aws_lambda_powertools.utilities.error_handler.exception import ErrorHandlerException
from aws_lambda_powertools.utilities.error_handler.exception_destination import ExceptionDestination
from aws_lambda_powertools.utilities.error_handler.http_response import HttpResponse
from aws_lambda_powertools.utilities.error_handler.sqs_destination import SqsDestination

#pylint: disable=too-many-arguments


class Singleton(type):
    """Singleton enforcer. Can be extracted if additional classes need to be singletons."""
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class ErrorHandlerFactory(metaclass=Singleton):

    def __init__(self):
        self._error_handlers = {}
        # this can be extracted to error_handler for lazy-registration in the future
        self._register_handler(ErrorDestinationEnum.SQS, SqsDestination)
        self._register_handler(ErrorDestinationEnum.HTTP_RESPONSE, HttpResponse)
        self._register_handler(ErrorDestinationEnum.RAISE_EXCEPTION, ExceptionDestination)

    def _register_handler(self, handler_type: ErrorDestinationEnum, handler_class: object) -> None:
        """
        Allows registration of error handlers in this factory.

        Args:
            handler_type: The name of the error handler.
            handler_class: class name of the error handler. Will not be initialized.

        """
        self._error_handlers[handler_type] = handler_class

    def register_custom_handler(self, handler_class: object) -> None:
        """
        Allows registration of a CUSTOM error handler in this factory.

        Args:
            handler_class: class name of the error handler. Will not be initialized.

        """
        self._register_handler(ErrorDestinationEnum.CUSTOM, handler_class)

    def get_handler(self, handler_type: ErrorDestinationEnum, logger: object, exc: Exception, context: LambdaContext,
                    trace: str) -> ErrorDestinationInterface:
        """
        Args:
            context: current Lambda context. Will be passed to the handler IMPL.
            exc: caught exception. Will be passed to the handler IMPL.
            logger: logger factory that will return a logger class instance
            trace: traceback of the exception
            handler_type: the type of handler IMPL, from the _error_handlers map, to instantiate.

        Returns:
            An initialized instance of the handler class.

        Raises: ErrorHandlerException in case of invalid arguments

        """
        handler_class = self._error_handlers.get(handler_type)
        if handler_class is None:
            raise ErrorHandlerException(f'{handler_type} is not a valid error handler type')
        if logger is None or exc is None:
            raise ErrorHandlerException('logger and/or exception are None')
        return handler_class(logger=logger, exception=exc, lambda_context=context, trace=trace)
