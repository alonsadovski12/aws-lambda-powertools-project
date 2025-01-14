from datetime import datetime, timedelta
from typing import Callable, List, Optional

from aws_lambda_powertools.logging import Logger
from aws_lambda_powertools.utilities.circuit_breaker.base.base_circuit_breaker import BaseCircuitBreaker, State
from aws_lambda_powertools.utilities.circuit_breaker.circuit_breaker_monitor import CircuitBreakerMonitor

logger = Logger(__name__)


class InMemoryCircuitBreaker(BaseCircuitBreaker):
    """
    Provides implementation of CircuitBreaker design pattern, and stores the circuit breaker in local memory

    Parameters:
        name: str
            CircuitBreaker name
        failure_threshold: int
            How many failures needs to occur before opening the CircuitBreaker. defaults: 5
        recovery_timeout: int
            How much time must be pass after opening the circuit breaker until sending the next request. default: 30
        expected_exception: List[Exception]
            Which exceptions are okay to get when running the business logic, those exception will be caught and treat
            as the business logic finished successfully. defaults: None
        fallback_function: Callable
            Called when the circuit is opened. defaults: None
        monitor: CircuitBreakerMonitor
            Circuit Breaker monitor class. defaults = CircuitBreakerMonitor(),
        logger: Logger
            The logger which will write logs from this object. Defaults: aws_lambda_powertools.logging.logger
    Example
    -----------
    def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:  #pylint: disable=unused-argument
        remote_call()

    @InMemoryCircuitBreaker(failure_threshold=2, recovery_timeout=30, name=CB_NAME)
    def remote_call():
        http = urllib3.PoolManager()

        url = os.environ.get('URL') + '/api/hello'
        resp = http.request('GET', url)
        print(f'responded with status: {resp.status}')
        if resp.status != 200:
            raise ValueError('connection error')
    """

    def __init__(
        self,
        name: str,
        failure_threshold: Optional[int] = None,
        recovery_timeout: Optional[int] = None,
        expected_exception: Optional[List[Exception]] = None,
        fallback_function: Optional[Callable] = None,
        monitor: Optional[CircuitBreakerMonitor] = None,
        logger: Logger = logger,
    ):
        super().__init__(name, failure_threshold, recovery_timeout, expected_exception, fallback_function, monitor)
        self.logger = logger

    @property
    def state(self):
        self.logger.info("Retrieving circuit breaker state")
        if self._state == State.OPEN and self.open_remaining <= 0:
            self.logger.info("The state has switched to half-open")
            return State.HALF_OPEN
        self.logger.info(f"The state is: {self._state}")
        return self._state

    @property
    def open_until(self):
        """
        The approximate datetime when the circuit breaker will try to recover
        :return: datetime
        """
        self.logger.info(
            f"Approximate time when the circuit breaker will return to half open: "
            f"{datetime.utcnow() + timedelta(seconds=self.open_remaining)}"
        )

        return datetime.utcnow() + timedelta(seconds=self.open_remaining)

    @property
    def open_remaining(self):
        """
        Number of seconds remaining, the circuit breaker stays in OPEN state
        :return: int
        """
        remain = (self._opened + self._recovery_timeout_in_milli) - self.current_milli_time()
        self.logger.info(f"Remaining milliseconds until switch to half-open {remain}")
        return remain

    @property
    def failure_count(self):
        self.logger.debug(f"Failure count:{self._failure_count}")
        return self._failure_count

    @property
    def closed(self):
        self.logger.info("Changing state to closed")
        return self.state == State.CLOSED

    @property
    def opened(self):
        self.logger.info("Changing state to open")
        return self.state == State.OPEN

    @property
    def name(self):
        self.logger.debug(f"Name:{self._name}")
        return self._name

    @property
    def last_failure(self):
        self.logger.debug(f"Last failure:{self._last_failure}")
        return self._last_failure

    @property
    def fallback_function(self):
        return self._fallback_function

    def _call_succeeded(self):
        """
        Close circuit after successful execution and reset failure count
        """
        self.logger.info("The requested call succeeded, state is: closed")
        self._state = State.CLOSED
        self._last_failure = None
        self._failure_count = 0

    def _call_failed(self):
        """
        Count failure and open circuit, if threshold has been reached
        """
        self.logger.info("The requested call failed")
        self._failure_count += 1
        if self._threshold_occurred():
            self.logger.warning(f"Failure count is above the threshold {self._failure_threshold}. moving state to open")
            self._state = State.OPEN
            self._opened = self.current_milli_time()


def circuit(
    name,
    failure_threshold=None,
    recovery_timeout=None,
    expected_exception=None,
    fallback_function=None,
    cls=InMemoryCircuitBreaker,
):
    # if the decorator is used without parameters, the
    # wrapped function is provided as first argument
    if callable(failure_threshold):
        return cls(name=name).decorate(failure_threshold)
    else:
        return cls(
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            expected_exception=expected_exception,
            name=name,
            fallback_function=fallback_function,
        )
