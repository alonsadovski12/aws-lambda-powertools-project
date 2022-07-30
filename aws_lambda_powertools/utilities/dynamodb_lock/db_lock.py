# pylint: disable=too-many-instance-attributes, too-many-arguments
"""
ֿֿBased on:
https://python-dynamodb-lock.readthedocs.io/en/latest/index.html
https://github.com/mohankishore/python_dynamodb_lock

This is a general purpose distributed locking library built on top of DynamoDB. It is heavily
"inspired" by the java-based AmazonDynamoDBLockClient library, and supports both coarse-grained
and fine-grained locking.
"""

import threading
import time
from urllib.parse import quote

from aws_lambda_powertools.logging_v2.logger import Logger

LOGGER = Logger(__name__)

class BaseDynamoDBLock:
    """
    Represents a distributed lock - as stored in DynamoDB.

    Typically used within the code to represent a lock held by some other lock-client.
    """

    def __init__(self, partition_key, sort_key, owner_name, lease_duration, record_version_number, expiry_time, additional_attributes, logger: None):
        """
        :param str partition_key: The primary lock identifier
        :param str sort_key: If present, forms a "composite identifier" along with the partition_key
        :param str owner_name: The owner name - typically from the lock_client
        :param float lease_duration: The lease duration in seconds - typically from the lock_client
        :param str record_version_number: A "liveness" indicating GUID - changes with every heartbeat
        :param int expiry_time: Epoch timestamp in seconds after which DynamoDB will auto-delete the record
        :param dict additional_attributes: Arbitrary application metadata to be stored with the lock
        :param Logger logger: you can pass logger in order to write logs
        """
        self.partition_key = partition_key
        self.sort_key = sort_key
        self.owner_name = owner_name
        self.lease_duration = lease_duration
        self.record_version_number = record_version_number
        self.expiry_time = expiry_time
        self.additional_attributes = additional_attributes or {}
        # additional properties
        self.unique_identifier = quote(partition_key) + '|' + quote(sort_key)
        self.logger = logger if logger else LOGGER

    def __str__(self):
        """
        Returns a readable string representation of this instance.
        """
        return f'{self.__class__.__name__}::{self.__dict__}'


class DynamoDBLock(BaseDynamoDBLock):
    """
    Represents a lock that is owned by a local DynamoDBLockClient instance.
    """

    PENDING = 'PENDING'
    LOCKED = 'LOCKED'
    RELEASED = 'RELEASED'
    IN_DANGER = 'IN_DANGER'
    INVALID = 'INVALID'

    def __init__(
        self,
        partition_key,
        sort_key,
        owner_name,
        lease_duration,
        record_version_number,
        expiry_time,
        additional_attributes,
        app_callback,
        lock_client,
        logger,
    ):
        """
        :param str partition_key: The primary lock identifier
        :param str sort_key: If present, forms a "composite identifier" along with the partition_key
        :param str owner_name: The owner name - typically from the lock_client
        :param float lease_duration: The lease duration - typically from the lock_client
        :param str record_version_number: Changes with every heartbeat - the "liveness" indicator
        :param int expiry_time: Epoch timestamp in seconds after which DynamoDB will auto-delete the record
        :param dict additional_attributes: Arbitrary application metadata to be stored with the lock

        :param Callable app_callback: Callback function that can be used to notify the app of lock entering
                the danger period, or an unexpected release
        :param DynamoDBLockClient lock_client: The client that "owns" this lock
        :param Logger logger: you can pass logger in order to write logs
        """
        BaseDynamoDBLock.__init__(self, partition_key, sort_key, owner_name, lease_duration, record_version_number, expiry_time,
                                  additional_attributes, logger)
        
        self.app_callback = app_callback
        self.lock_client = lock_client
        # additional properties
        self.last_updated_time = time.monotonic()
        self.thread_lock = threading.RLock()
        self.status = self.PENDING

    def __enter__(self):
        """
        No-op - returns itself
        """
        self.logger.debug('entering lock', unique_identifier=self.unique_identifier)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Releases the lock - with best_effort=True
        """
        self.logger.debug('exiting lock', unique_identifier=self.unique_identifier)
        self.release(best_effort=True)
        return True

    def release(self, best_effort=True):
        """
        Calls the lock_client.release_lock(self, True) method

        :param bool best_effort: If True, any exception when calling DynamoDB will be ignored
                and the clean up steps will continue, hence the lock item in DynamoDb might not
                be updated / deleted but will eventually expire. Defaults to True.
        """
        self.logger.debug('releasing lock', unique_identifier=self.unique_identifier)
        self.lock_client.release_lock(self, best_effort)
