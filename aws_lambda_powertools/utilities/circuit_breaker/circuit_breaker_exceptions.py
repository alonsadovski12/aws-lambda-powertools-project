class CircuitBreakerException(Exception):
    def __init__(self, circuit_breaker, *args, **kwargs):
        super(CircuitBreakerException, self).__init__(*args, **kwargs)
        self._circuit_breaker = circuit_breaker

    def __str__(self, *args, **kwargs):
        return f'Circuit {self._circuit_breaker.name} OPEN until {self._circuit_breaker.open_until} ' \
               f'({self._circuit_breaker.failure_count} failures, ' \
               f'{round(self._circuit_breaker.open_remaining)} sec remaining) ' \
               f'(last_failure: {repr(self._circuit_breaker.last_failure)})'
