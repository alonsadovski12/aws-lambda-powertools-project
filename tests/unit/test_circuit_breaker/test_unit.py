from unittest.mock import Mock, patch

from pytest import raises

from aws_lambda_powertools.utilities.circuit_breaker.base.base_circuit_breaker import CircuitBreakerException
from aws_lambda_powertools.utilities.circuit_breaker.in_memory_circuit_breaker import InMemoryCircuitBreaker, circuit


class FooError(Exception):
    def __init__(self, val=None):
        self.val = val


class BarError(Exception):
    pass


def test_circuitbreaker__str__():
    cb = InMemoryCircuitBreaker(name='Foobar')
    assert str(cb) == 'Foobar'


def test_circuitbreaker_error__str__():
    cb = InMemoryCircuitBreaker(name='Foobar')
    cb._last_failure = Exception()
    error = CircuitBreakerException(cb)

    assert str(error).startswith('Circuit Foobar OPEN until ')
    assert str(error).endswith('(0 failures, 30 sec remaining) (last_failure: Exception())')


def test_circuitbreaker_should_save_last_exception_on_failure_call():
    cb = InMemoryCircuitBreaker(name='Foobar')

    func = Mock(side_effect=IOError)

    with raises(IOError):
        cb.call(func)

    assert isinstance(cb.last_failure, IOError)


def test_circuitbreaker_should_clear_last_exception_on_success_call():
    cb = InMemoryCircuitBreaker(name='Foobar')
    cb._last_failure = IOError()
    assert isinstance(cb.last_failure, IOError)

    cb.call(lambda: True)

    assert cb.last_failure is None


def test_circuitbreaker_should_call_fallback_function_if_open():
    fallback = Mock(return_value=True)

    func = Mock(return_value=False, __name__="Mock")  # attribute __name__ required for 2.7 compat with functools.wraps

    InMemoryCircuitBreaker.opened = lambda self: True

    cb = InMemoryCircuitBreaker(name='WithFallback', fallback_function=fallback)
    decorated_func = cb.decorate(func)

    decorated_func()
    fallback.assert_called_once_with()


@patch('aws_lambda_powertools.utilities.circuit_breaker.in_memory_circuit_breaker.InMemoryCircuitBreaker.opened',
       return_value=True)
def test_circuitbreaker_should_not_call_function_if_open(patch):
    fallback = Mock(return_value=True)

    func = Mock(return_value=False, __name__="Mock")  # attribute __name__ required for 2.7 compat with functools.wraps

    cb = InMemoryCircuitBreaker(name='WithFallback', fallback_function=fallback)
    decorated_func = cb.decorate(func)

    assert decorated_func() == fallback.return_value
    assert not func.called


def mocked_function(*args, **kwargs):
    pass


@patch('aws_lambda_powertools.utilities.circuit_breaker.in_memory_circuit_breaker.InMemoryCircuitBreaker.opened',
       return_value=True)
def test_circuitbreaker_call_fallback_function_with_parameters(patch):
    fallback = Mock(return_value=True)

    cb = circuit(name='with_fallback', fallback_function=fallback)

    # mock opened prop to see if fallback is called with correct parameters.
    # cb.opened = lambda self: True
    func_decorated = cb.decorate(mocked_function)

    func_decorated('test2', test='test')

    # check args and kwargs are getting correctly to fallback function

    fallback.assert_called_once_with('test2', test='test')


@patch('aws_lambda_powertools.utilities.circuit_breaker.base.base_circuit_breaker.BaseCircuitBreaker.decorate')
def test_circuit_decorator_without_args(circuitbreaker_mock):
    def function():
        return True

    circuit(function)
    circuitbreaker_mock.assert_called_once_with(function)


def test_circuit_decorator_with_args():
    def function_fallback():
        return True

    breaker = circuit(10, 20, [KeyError], 'foobar', function_fallback)

    assert breaker.is_expected_failure(KeyError)
    assert not breaker.is_expected_failure(Exception)
    assert not breaker.is_expected_failure(FooError)
    assert breaker._failure_threshold == 10
    assert breaker._recovery_timeout_in_milli == 200000
    assert breaker._name == "foobar"
    assert breaker._fallback_function == function_fallback


def test_breaker_default_constructor_traps_Exception():
    breaker = circuit()
    assert breaker.is_expected_failure(Exception)
    assert breaker.is_expected_failure(FooError)


def test_breaker_expected_exception_is_custom_exception():
    breaker = circuit(expected_exception=[FooError])
    assert breaker.is_expected_failure(FooError)
    assert not breaker.is_expected_failure(Exception)


def test_breaker_constructor_expected_exception_is_exception_list():
    breaker = circuit(expected_exception=[FooError, BarError])
    assert breaker.is_expected_failure(FooError)
    assert breaker.is_expected_failure(BarError)
    assert not breaker.is_expected_failure(Exception)
