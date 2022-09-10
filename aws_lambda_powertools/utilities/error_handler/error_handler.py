# pylint: disable=no-name-in-module,unused-argument,no-value-for-parameter,too-many-arguments
import traceback
from typing import Any, Callable, Dict, Optional

from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.middleware_factory import lambda_handler_decorator

from aws_lambda_powertools.utilities.error_handler.constants import ErrorDestinationEnum
from aws_lambda_powertools.utilities.error_handler.exception import ErrorHandlerException
from aws_lambda_powertools.utilities.error_handler.handler_factory import ErrorHandlerFactory


def check_params(destination: ErrorDestinationEnum, logger_factory: Callable[[], object], custom_handler: Optional[str]) -> None:
    if not isinstance(destination, ErrorDestinationEnum):
        raise ErrorHandlerException('destination is not a ErrorDestinationEnum, unable to initialize error handler')
    if not isinstance(logger_factory, Callable):
        raise ErrorHandlerException('logger factory is not a Callable, unable to initialize error handler')
    if custom_handler is not None and destination != ErrorDestinationEnum.CUSTOM:
        raise ErrorHandlerException('custom destination must be used in conjunction only with ErrorDestinationEnum.CUSTOM')


@lambda_handler_decorator
def error_handler(
    handler: Callable,
    event: Dict[str, Any],
    context: LambdaContext,
    destination: ErrorDestinationEnum,
    logger_factory: Callable[[], object],
    custom_handler: Optional[str] = None,
) -> Any:
    """ This utility is used for catching unhandled exceptions that your handler code has missed
        and allows to gracefully handle them.
        Error Handler utility offers several handlers, each one will handle your failure differently.
        Each handler logs the exception and adds the following metadata: exception message, traceback, lambda name and AWS request ID.

    Parameters:
        handler (Callable): lambda handler
        event (Dict[str, Any]): lambda event
        context (LambdaContext): lambda context
        destination (ErrorDestinationEnum): error handler type
        logger_factory (Callable[[], object]): [description]   needs to get kwargs that are Dict,
                    not just string, have info/error function. can. you can use the platform's infra logging logger
        custom_handler: Optional[str]: only applicable if destination is set to ErrorDestinationEnum.CUSTOM.
                        This is the class name to initialize when handling with the exception.
                        Class is required to extend ErrorDestinationInterface
    Raises:
        ErrorHandlerException: an exception that can occur during handling of the original uncaught exception

    Returns:
        Any:
    """
    # sanity checks
    check_params(destination, logger_factory, custom_handler)
    if destination == ErrorDestinationEnum.CUSTOM:
        ErrorHandlerFactory().register_custom_handler(custom_handler)

    try:
        return handler(event, context)
    except Exception as exc:
        trace = traceback.format_exc()
        logger = logger_factory()
        logger.error(f'caught unhandled exception, calling error handler, destination={destination}')
        return ErrorHandlerFactory().get_handler(destination, logger, exc, context, trace).send_error_to_destination()
