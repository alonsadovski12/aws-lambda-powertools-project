import json
from datetime import datetime, timedelta
from typing import Callable, List, Optional
from decimal import Decimal
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from aws_lambda_powertools.logging import Logger
from aws_lambda_powertools.utilities.circuit_breaker.base.base_circuit_breaker import BaseCircuitBreaker, State
from aws_lambda_powertools.utilities.circuit_breaker.circuit_breaker_exceptions import CircuitBreakerException
from aws_lambda_powertools.utilities.circuit_breaker.circuit_breaker_monitor import CircuitBreakerMonitor
from aws_lambda_powertools.utilities.circuit_breaker.schemas.circuit_breaker_schema import CircuitBreakerDynamoDBSchema



logger = Logger(__name__)

class DynamoDBCircuitBreaker(BaseCircuitBreaker):
    def __init__(self,
                 name: str,
                 table_name: str,
                 config: Optional[Config] = None,
                 boto3_session: Optional[boto3.session.Session] = None,
                 failure_threshold: int = None,
                 recovery_timeout: int = None,
                 expected_exception: List[Exception] = None,
                 fallback_function: Callable = None,
                 monitor: CircuitBreakerMonitor = CircuitBreakerMonitor(),
                 logger: Logger = logger):
        super().__init__(name, failure_threshold, recovery_timeout, expected_exception, fallback_function, monitor)
        self.logger = logger

        config = config or Config()
        session = boto3_session or boto3.session.Session()

        self.table = session.resource("dynamodb", config=config).Table(table_name)
        self._init_cb_item_in_dynamodb()

    @property
    def state(self):
        self.logger.info("Retrieving circuit breaker state")
        circuit_breaker_item: CircuitBreakerDynamoDBSchema = self._get_state_from_remote()
        if circuit_breaker_item.cb_state == State.OPEN.value and self._check_remaining(circuit_breaker_item) <= 0:
            self.logger.info("The state has switched to half-open")
            circuit_breaker_item.cb_state = State.HALF_OPEN.value
            self._update_circuit_breaker_item_in_table(circuit_breaker_item)
            return circuit_breaker_item.cb_state
        self.logger.info(f"The state is: {circuit_breaker_item.cb_state}")
        return circuit_breaker_item.cb_state


    @property
    def open_remaining(self):
        """
        Number of seconds remaining, the circuit breaker stays in OPEN state
        :return: int
        """

        circuit_breaker_item: CircuitBreakerDynamoDBSchema = self._get_state_from_remote()
        return self._check_remaining(circuit_breaker_item)

    @property
    def failure_count(self):
        failure_count = self._get_state_from_remote().failure_count
        self.logger.debug(f"Failure count:{ failure_count}")
        return failure_count

    @property
    def closed(self):
        self.logger.info(f"checking if {self.name} in close state")
        return self.state == State.CLOSED.value

    @property
    def opened(self):
        self.logger.info(f"checking if {self.name} in open state")
        return self.state == State.OPEN.value

    @property
    def name(self):
        self.logger.debug(f"CircuitBreaker name:{self._name}")
        return self._name

    @property
    def last_failure(self):
        circuit_breaker_item: CircuitBreakerDynamoDBSchema = self._get_state_from_remote()
        self.logger.debug(f"CircuitBreaker last failure:{circuit_breaker_item.last_failure}")
        return self._last_failure

    @property
    def fallback_function(self):
        return self._fallback_function
    
    def _check_remaining(self, circuit_breaker_obj: CircuitBreakerDynamoDBSchema) -> int:
        remain = (circuit_breaker_obj.opened + circuit_breaker_obj.recovery_timeout) - self.current_milli_time()
        self.logger.info(f"Remaining milliseconds until switch to half-open {remain}")
        return remain



    def _get_empty_dynamo_entry(self)-> CircuitBreakerDynamoDBSchema: 
        return CircuitBreakerDynamoDBSchema(**{'name': self.name,
                                     'cb_state':State.CLOSED.value,
                                     'opened': self._opened,
                                     'expected_exception':str(self._expected_exception),
                                     'failure_count':self._failure_count,
                                     'last_failure':'',
                                     'failure_threshold':self._failure_threshold,
                                     'recovery_timeout':self._recovery_timeout_in_milli})


    def _call_succeeded(self):
        """
        Close circuit after successful execution and reset failure count
        """
        self.logger.info("The requested call succeeded, state is: closed")
        circuit_breaker_item: CircuitBreakerDynamoDBSchema = self._get_state_from_remote()
        circuit_breaker_item.cb_state = State.CLOSED.value
        circuit_breaker_item.last_failure = ''
        circuit_breaker_item.failure_count = 0
        self._update_circuit_breaker_item_in_table(circuit_breaker_item)

    def _call_failed(self):
        """
        Count failure and open circuit, if threshold has been reached
        """
        self.logger.info("The requested call failed")
        circuit_breaker_item: CircuitBreakerDynamoDBSchema = self._get_state_from_remote()
        circuit_breaker_item.failure_count += 1
        if circuit_breaker_item.failure_count >= circuit_breaker_item.failure_threshold:
            self.logger.warning(f'Failure count is above the threshold {circuit_breaker_item.failure_threshold}. moving state to open')
            circuit_breaker_item.cb_state = State.OPEN.value
            circuit_breaker_item.opened = self.current_milli_time()
        
        self._update_circuit_breaker_item_in_table(circuit_breaker_item)
    
    def _init_cb_item_in_dynamodb(self):
        try:
            self.logger.info("retrieving circuit breaker object from dynamoDB")
            entry_item = self.table.get_item(Key={'name':self.name}).get('Item')
            if entry_item is None:
                self.logger.info('circuit breaker does not exist in dynamodb table, putting an empty item in dynamodb')
                self._put_circuit_breaker_item_in_table(self._get_empty_dynamo_entry())
            else:
                return CircuitBreakerDynamoDBSchema(**entry_item)
        except ClientError:
            self.logger.exception('circuit breaker does not exist in dynamodb table')
            self._put_circuit_breaker_item_in_table(self._get_empty_dynamo_entry())

    def _get_state_from_remote(self) -> CircuitBreakerDynamoDBSchema:
        """
            Get the CircuitBreaker entry from the remote table
        """
        try:
            self.logger.info("retrieving circuit breaker object from dynamoDB")
            entry_item = self.table.get_item(Key={'name':self.name}).get('Item')
            self.logger.info(str(entry_item))
            return CircuitBreakerDynamoDBSchema(**entry_item)
        except ClientError:
            self.logger.exception("item does not exist in table")
            return self._get_empty_dynamo_entry()
        except ValueError:
            self.logger.exception("item schema is malformed, restating the circuit")
            return self._get_empty_dynamo_entry()

    def _put_circuit_breaker_item_in_table(self, circuit_breaker_obj: CircuitBreakerDynamoDBSchema):
        try:
            self.logger.info("Putting circuit breaker item in dynamoDB")
            obj = circuit_breaker_obj.dict()
            ddb_data = json.loads(json.dumps(obj), parse_float=Decimal) # to parse float to decimal
            self.table.put_item(Item=ddb_data)
        except self.table.meta.client.exceptions.ConditionalCheckFailedException:
            error_message = "Failed to put record for already existing"
            self.logger.exception(error_message)
            raise CircuitBreakerException(error_message)
    
    def _update_circuit_breaker_item_in_table(self, circuit_breaker_obj: CircuitBreakerDynamoDBSchema):
        self.logger.info("updating circuit breaker item in dynamodb")
        update_expression = 'SET cb_state=:cb_state, opened=:cb_opened, failure_count=:cb_failure_count, last_failure=:cb_last_failure'
        expression_attribute_values = { ':cb_state': circuit_breaker_obj.cb_state,
                                        ':cb_opened': circuit_breaker_obj.opened,
                                        ':cb_failure_count': circuit_breaker_obj.failure_count,
                                        ':cb_last_failure': str(self._last_failure)}
        try:
            self.table.update_item(
                Key={'name': circuit_breaker_obj.name,},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribute_values,
                ReturnValues='UPDATED_OLD',
            )

        except ClientError as ex: #TODO: change to specific exception
            error_message = "Failed to update record in dynamoDB"
            self.logger.exception(error_message)
            raise CircuitBreakerException(error_message)



def circuit(failure_threshold=None,
            recovery_timeout=None,
            expected_exception=None,
            name=None,
            fallback_function=None,
            cls=DynamoDBCircuitBreaker):
    # if the decorator is used without parameters, the
    # wrapped function is provided as first argument
    if callable(failure_threshold):
        return cls().decorate(failure_threshold)
    else:
        return cls(
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            expected_exception=expected_exception,
            name=name,
            fallback_function=fallback_function)
