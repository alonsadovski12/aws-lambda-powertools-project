from typing import Dict
from dataclasses import asdict, dataclass

from aws_lambda_powertools.utilities.circuit_breaker.schemas.field_validation import FieldValidationClass

@dataclass(init=False)
class CircuitBreakerDynamoDBSchema(FieldValidationClass):
    name: str
    cb_state: str
    opened: int
    failure_count: int
    last_failure: str
    failure_threshold: int
    recovery_timeout: int
    expected_exception: str #List[Exception]


    def dict(self) -> Dict:
        return asdict(self)