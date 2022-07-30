# todo: make abstract and implement in-memory and dynamo support
from typing import Iterable, AnyStr

from aws_lambda_powertools.utilities.circuit_breaker.base.base_circuit_breaker import BaseCircuitBreaker


class CircuitBreakerMonitor:
    circuit_breakers = {}

    @classmethod
    def register(cls, circuit_breaker):
        cls.circuit_breakers[circuit_breaker.name] = circuit_breaker

    @classmethod
    def all_closed(cls):
        # type: () -> bool
        return len(list(cls.get_open())) == 0

    @classmethod
    def get_circuits(cls):
        # type: () -> Iterable[BaseCircuitBreaker]
        return cls.circuit_breakers.values()

    @classmethod
    def get(cls, name):
        # type: (AnyStr) -> BaseCircuitBreaker
        return cls.circuit_breakers.get(name)

    @classmethod
    def get_open(cls):
        # type: () -> Iterable[BaseCircuitBreaker]
        for circuit in cls.get_circuits():
            if circuit.opened:
                yield circuit

    @classmethod
    def get_closed(cls):
        # type: () -> Iterable[BaseCircuitBreaker]
        for circuit in cls.get_circuits():
            if circuit.closed:
                yield circuit
