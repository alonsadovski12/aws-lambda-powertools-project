from typing import Dict


class CircuitBreakerMonitor:
    circuit_breakers: Dict = {}

    @classmethod
    def register(cls, circuit_breaker):
        cls.circuit_breakers[circuit_breaker.name] = circuit_breaker

    @classmethod
    def all_closed(cls):
        return len(list(cls.get_open())) == 0

    @classmethod
    def get_circuits(cls):
        return cls.circuit_breakers.values()

    @classmethod
    def get(cls, name):
        return cls.circuit_breakers.get(name)

    @classmethod
    def get_open(cls):
        for circuit in cls.get_circuits():
            if circuit.opened:
                yield circuit

    @classmethod
    def get_closed(cls):
        for circuit in cls.get_circuits():
            if circuit.closed:
                yield circuit
