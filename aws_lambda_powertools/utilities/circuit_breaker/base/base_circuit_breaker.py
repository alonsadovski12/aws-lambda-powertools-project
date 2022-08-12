from abc import ABC, abstractmethod
from enum import Enum, auto
from functools import wraps
from inspect import isgeneratorfunction
from time import sleep, time
from typing import Callable, List

from aws_lambda_powertools.logging import Logger
from aws_lambda_powertools.utilities.circuit_breaker.circuit_breaker_exceptions import CircuitBreakerException

LOGGER = Logger(__name__)

class State(Enum):
    CLOSED = auto()
    HALF_OPEN = auto()
    OPEN = auto()


class BaseCircuitBreaker(ABC):
    FAILURE_THRESHOLD = 5
    RECOVERY_TIMEOUT = 30
    SECONDS_TO_SECONDS_FACTOR = 1000

    def __init__(self,
                 name: str,
                 failure_threshold: int = None,
                 recovery_timeout: int = None,
                 expected_exception: List[Exception] = None,
                 fallback_function: Callable = None,
                 monitor=None,
                 ):
        """
        Construct a circuit breaker.
        :param failure_threshold: break open after this many failures
        :param recovery_timeout: close after this many seconds
        :param expected_exception: list of Exception types.
        :param name: name for this circuitbreaker
        :param fallback_function: called when the circuit is opened
           :return: Circuitbreaker instance
           :rtype: Circuitbreaker
        """
        self._last_failure = None
        self._failure_count = 0
        self._failure_threshold = failure_threshold or self.FAILURE_THRESHOLD
        self._recovery_timeout_in_milli = (recovery_timeout or self.RECOVERY_TIMEOUT) * self.SECONDS_TO_SECONDS_FACTOR
        self._circuit_breaker_monitor = monitor

        # create a list of exceptions. if None create a list of Exceptions as default
        self._expected_exception: List[Exception] = expected_exception if expected_exception else [Exception]
        self._fallback_function = fallback_function
        self._name = name
        self._state = State.CLOSED
        self._opened = self.current_milli_time()
        self.logger = LOGGER
    
    def _threshold_occurred(self) -> bool:
        return self._failure_count >= self._failure_threshold

    def __call__(self, wrapped):
        return self.decorate(wrapped)

    def __enter__(self):
        return None

    def __exit__(self, exc_type, exc_value, _traceback):
        if exc_type and self.is_expected_failure(exc_type):
            # exception was raised and is our concern
            self._last_failure = exc_value
            self._call_failed()
        else:
            self._call_succeeded()
        return False  # return False to raise exception if any

    def decorate(self, function):
        """
        Applies the circuit breaker to a function
        """
        if self._name is None:
            self._name = function.__name__

        self._circuit_breaker_monitor.register(self)

        if isgeneratorfunction(function):
            call = self.call_generator
        else:
            call = self.call

        @wraps(function)
        def wrapper(*args, **kwargs):
            if self.opened:
                if self.fallback_function:
                    return self.fallback_function(*args, **kwargs)
                raise CircuitBreakerException(self)
            return call(function, *args, **kwargs)

        return wrapper
    
    def retry_until_close(self, function): 
        if self._name is None:
            self._name = function.__name__

        self._circuit_breaker_monitor.register(self)

        if isgeneratorfunction(function):
            call = self.call_generator
        else:
            call = self.call

        @wraps(function)
        def wrapper(*args, **kwargs):
            try:
                return call(function, *args, **kwargs)
            except Exception as ex:
                self.logger.error(f'failed to execute function, count={self._failure_count}')
                sleep((self._recovery_timeout_in_milli / self.SECONDS_TO_SECONDS_FACTOR) / self._failure_threshold )
                while not self._threshold_occurred():
                    try:
                        return call(function, *args, **kwargs)
                    except:
                        self.logger.error(f'failed to execute function, count={self._failure_count}')
                        sleep((self._recovery_timeout_in_milli / self.SECONDS_TO_SECONDS_FACTOR) / self._failure_threshold )
                if self.fallback_function:
                    return self.fallback_function(*args, **kwargs)
                raise CircuitBreakerException(self)
        
        return wrapper

    def call(self, func, *args, **kwargs):
        """
        Calls the decorated function and applies the circuit breaker
        rules on success or failure
        :param func: Decorated function
        """
        with self:
            return func(*args, **kwargs)

    def call_generator(self, func, *args, **kwargs):
        """
        Calls the decorated generator function and applies the circuit breaker
        rules on success or failure
        :param func: Decorated generator function
        """
        with self:
            for el in func(*args, **kwargs):
                yield el

    def is_expected_failure(self, curr_exception: Exception) -> bool:
        return any(issubclass(curr_exception, exc) for exc in self._expected_exception)

        
    def current_milli_time(self) -> int:
        return round(time() * self.SECONDS_TO_SECONDS_FACTOR)

    def __str__(self, *args, **kwargs):
        return self._name

    @abstractmethod
    def _call_succeeded(self):
        """
        Close circuit after successful execution and reset failure count
        """
        pass

    @abstractmethod
    def _call_failed(self):
        """
        Count failure and open circuit, if threshold has been reached
        """
        pass

    @property
    @abstractmethod
    def state(self):
        pass

    @property
    @abstractmethod
    def open_remaining(self):
        """
        Number of seconds remaining, the circuit breaker stays in OPEN state
        :return: int
        """
        pass

    @property
    @abstractmethod
    def failure_count(self):
        pass

    @property
    @abstractmethod
    def closed(self):
        pass

    @property
    @abstractmethod
    def opened(self):
        pass

    @property
    @abstractmethod
    def name(self):
        pass

    @property
    @abstractmethod
    def last_failure(self):
        pass

    @property
    @abstractmethod
    def fallback_function(self):
        pass
