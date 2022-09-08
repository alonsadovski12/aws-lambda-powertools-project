from time import sleep

from unittest.mock import Mock, patch
from pytest import raises

from aws_lambda_powertools.utilities.circuit_breaker.base.base_circuit_breaker import CircuitBreakerException, State
from aws_lambda_powertools.utilities.circuit_breaker.in_memory_circuit_breaker import InMemoryCircuitBreaker
from aws_lambda_powertools.utilities.circuit_breaker.circuit_breaker_monitor import CircuitBreakerMonitor

TABLE_NAME = 'AlonsadovskiHelloDemocircuit-ServiceCircuitBreaker01F9D601-1K8M53UDO7S6D'
NUM = 0
THRESH3 = 0
RAISE_EXCEPTION = False

def pseudo_remote_call():
    global NUM
    NUM = NUM + 1
    return True


@InMemoryCircuitBreaker(name='circuit_success')
def circuit_success():
    return pseudo_remote_call()


@InMemoryCircuitBreaker(failure_threshold=1, name="circuit_failure")
def circuit_failure():
    raise IOError()


@InMemoryCircuitBreaker(failure_threshold=1, name="circuit_generator_failure")
def circuit_generator_failure():
    pseudo_remote_call()
    yield 1
    raise IOError()


@InMemoryCircuitBreaker(failure_threshold=1, name="threshold_1")
def circuit_threshold_1():
    pseudo_remote_call()
    raise IOError('Connection refused')


@InMemoryCircuitBreaker(failure_threshold=2, recovery_timeout=3, name="threshold_2")
def circuit_threshold_2_timeout_1():
    global RAISE_EXCEPTION
    if RAISE_EXCEPTION:
        raise IOError('Connection refused')
    return True


@InMemoryCircuitBreaker(failure_threshold=3, recovery_timeout=1, name="threshold_3")
def circuit_threshold_3_timeout_1():
    global THRESH3
    THRESH3 = THRESH3 + 1
    if THRESH3 == 1:
        return True
    raise IOError('Connection refused')


def test_circuit_pass_through():
    assert circuit_success() is True


def test_circuitbreaker_monitor():
    assert CircuitBreakerMonitor.all_closed() is True
    assert len(list(CircuitBreakerMonitor.get_circuits())) == 6
    assert len(list(CircuitBreakerMonitor.get_closed())) == 6
    assert len(list(CircuitBreakerMonitor.get_open())) == 0

    with raises(IOError):
        circuit_failure()

    assert CircuitBreakerMonitor.all_closed() is False
    assert len(list(CircuitBreakerMonitor.get_circuits())) == 6
    assert len(list(CircuitBreakerMonitor.get_closed())) == 5
    assert len(list(CircuitBreakerMonitor.get_open())) == 1


@patch('tests.unit.test_circuit_breaker.test_functional.pseudo_remote_call', return_value=True)
def test_threshold_hit_prevents_consequent_calls(mock_remote):
    # type: (Mock) -> None
    mock_remote.side_effect = IOError('Connection refused')
    circuitbreaker = CircuitBreakerMonitor.get('threshold_1')

    assert circuitbreaker.closed

    with raises(IOError):
        circuit_threshold_1()

    assert circuitbreaker.opened

    with raises(CircuitBreakerException):
        circuit_threshold_1()

    mock_remote.assert_called_once_with()


@patch('tests.unit.test_circuit_breaker.test_functional.pseudo_remote_call', return_value=True)
def test_circuitbreaker_recover_half_open(mock_remote):
    # type: (Mock) -> None
    circuitbreaker = CircuitBreakerMonitor.get('threshold_3')

    # initial state: closed
    assert circuitbreaker.closed
    assert circuitbreaker.state == State.CLOSED

    # no exception -> success
    assert circuit_threshold_3_timeout_1()

    # from now all subsequent calls will fail
    # 1. failed call -> original exception
    with raises(IOError):
        circuit_threshold_3_timeout_1()
    assert circuitbreaker.closed
    assert circuitbreaker.failure_count == 1

    # 2. failed call -> original exception
    with raises(IOError):
        circuit_threshold_3_timeout_1()
    assert circuitbreaker.closed
    assert circuitbreaker.failure_count == 2

    # 3. failed call -> original exception
    with raises(IOError):
        circuit_threshold_3_timeout_1()

    # Circuit breaker opens, threshold has been reached
    assert circuitbreaker.opened
    assert circuitbreaker.state == State.OPEN
    assert circuitbreaker.failure_count == 3
    assert circuitbreaker.open_remaining <= 1000

    # 4. failed call -> not passed to function -> CircuitBreakerException
    with raises(CircuitBreakerException):
        circuit_threshold_3_timeout_1()
    assert circuitbreaker.opened
    assert circuitbreaker.failure_count == 3
    assert circuitbreaker.open_remaining <= 1000

    # 5. failed call -> not passed to function -> CircuitBreakerException
    with raises(CircuitBreakerException):
        circuit_threshold_3_timeout_1()
    assert circuitbreaker.opened
    assert circuitbreaker.failure_count == 3
    assert circuitbreaker.open_remaining <= 1000

    # wait for 1 second (recover timeout)
    sleep(1)

    # circuit half-open -> next call will be passed through
    assert not circuitbreaker.closed
    assert circuitbreaker.open_remaining < 0
    assert circuitbreaker.state == State.HALF_OPEN

    # State half-open -> function is executed -> original exception
    with raises(IOError):
        circuit_threshold_3_timeout_1()
    assert circuitbreaker.opened
    assert circuitbreaker.failure_count == 4
    assert circuitbreaker.open_remaining <= 1000

    # State open > not passed to function -> CircuitBreakerException
    with raises(CircuitBreakerException):
        circuit_threshold_3_timeout_1()


@patch('tests.unit.test_circuit_breaker.test_functional.pseudo_remote_call', return_value=True)
def test_circuitbreaker_reopens_after_successful_calls(mock_remote):
    global RAISE_EXCEPTION
    RAISE_EXCEPTION = False
    # type: (Mock) -> None
    circuitbreaker = CircuitBreakerMonitor.get('threshold_2')

    assert str(circuitbreaker) == 'threshold_2'

    # initial state: closed
    assert circuitbreaker.closed
    assert circuitbreaker.state == State.CLOSED
    assert circuitbreaker.failure_count == 0

    # successful call -> no exception
    assert circuit_threshold_2_timeout_1()

    # from now all subsequent calls will fail
    RAISE_EXCEPTION = True

    # 1. failed call -> original exception
    with raises(IOError):
        circuit_threshold_2_timeout_1()
    assert circuitbreaker.closed
    assert circuitbreaker.failure_count == 1

    # 2. failed call -> original exception
    with raises(IOError):
        circuit_threshold_2_timeout_1()

    # Circuit breaker opens, threshold has been reached
    assert circuitbreaker.opened
    assert circuitbreaker.state == State.OPEN
    assert circuitbreaker.failure_count == 2
    assert circuitbreaker.open_remaining <= 3000

    # 4. failed call -> not passed to function -> CircuitBreakerException
    with raises(CircuitBreakerException):
        circuit_threshold_2_timeout_1()
    assert circuitbreaker.opened
    assert circuitbreaker.failure_count == 2
    assert circuitbreaker.open_remaining <= 3000

    # from now all subsequent calls will succeed
    RAISE_EXCEPTION = False

    # but recover timeout has not been reached -> still open
    # 5. failed call -> not passed to function -> CircuitBreakerException
    with raises(CircuitBreakerException):
        circuit_threshold_2_timeout_1()
    assert circuitbreaker.opened
    assert circuitbreaker.failure_count == 2
    assert circuitbreaker.open_remaining <= 3000

    # wait for 3 second (recover timeout)
    sleep(3)

    # circuit half-open -> next call will be passed through
    assert not circuitbreaker.closed
    assert circuitbreaker.failure_count == 2
    assert circuitbreaker.open_remaining < 0
    assert circuitbreaker.state == State.HALF_OPEN

    # successful call
    assert circuit_threshold_2_timeout_1()

    # circuit closed and reset'ed
    assert circuitbreaker.closed
    assert circuitbreaker.state == State.CLOSED
    assert circuitbreaker.failure_count == 0



@patch("tests.unit.test_circuit_breaker.test_functional.pseudo_remote_call", return_value=True)
def test_circuitbreaker_handles_generator_functions(mock_remote):
    # type: (Mock) -> None
    circuitbreaker = CircuitBreakerMonitor.get("circuit_generator_failure")
    assert circuitbreaker.closed

    with raises(IOError):
        list(circuit_generator_failure())

    assert circuitbreaker.opened

    with raises(CircuitBreakerException):
        list(circuit_generator_failure())

    mock_remote.assert_called_once_with()
