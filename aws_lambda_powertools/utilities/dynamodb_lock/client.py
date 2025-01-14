# pylint: disable=too-many-instance-attributes,unused-variable, too-many-arguments, consider-using-f-string
# pylint: disable=raise-missing-from, no-else-return, no-else-raise, no-else-break, raise-missing-from
# pylint: disable=too-many-locals, too-many-branches, too-many-statements
"""
ֿֿBased on:
https://python-dynamodb-lock.readthedocs.io/en/latest/index.html

This is a general purpose distributed locking library built on top of DynamoDB. It is heavily
"inspired" by the java-based AmazonDynamoDBLockClient library, and supports both coarse-grained
and fine-grained locking.
"""

import datetime
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from typing import Dict, Optional

from botocore.exceptions import ClientError

from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.dynamodb_lock.consts import CONDITIONAL_CHECK_EXCEPTION, DEFAULT_TTL_ATTRIBUTE_NAME
from aws_lambda_powertools.utilities.dynamodb_lock.db_lock import BaseDynamoDBLock, DynamoDBLock
from aws_lambda_powertools.utilities.dynamodb_lock.exceptions import DynamoDBLockError

LOGGER = Logger(__name__)


class DynamoDBLockClient:
    """
    Provides distributed locks using DynamoDB's support for conditional reads/writes.

    Parameters:
        dynamodb_resource: boto3.ServiceResource
            boto3 dynamodb resource client
        owner_name: str
            owner name to set to the lock item, should be unique to allow attribution
        table_name: str
            the dynamoDB table name
        partition_key_name: str
            partition key name
        sort_key_name: str
            sort key name
        ttl_attribute_name: str
            the column name of the ttl in the DynamoDB table. defaults to 'expiry_time'
        heartbeat_period: datetime.timedelta
            How often to update DynamoDB to note that the instance is still running.
            It is recommended to make this at least 4 times smaller (in order to work correctly in the safe period)
            than the leaseDuration. defaults to 5 seconds.
        safe_period: datetime.timedelta
            How long is it okay to go without a heartbeat before considering a lock to be in "danger".
            Defaults to 20 seconds.
        lease_duration: datetime.timedelta
            The length of time that the lease for the lock will be granted for.
            i.e. if there is no heartbeat for this period of time, then the lock will be considered as expired.
            Defaults to 30 seconds.
        expiry_period: datetime.timedelta
            The fallback expiry timestamp to allow DynamoDB to clean up old locks after a server crash.
            This value should be significantly larger than the _lease_duration to ensure that clock-skew etc.
            are not an issue. Defaults to 1 hour.
        app_callback_executor: ThreadPoolExecutor
            The executor to be used for invoking the app_callbacks in case of un-expected errors.
            Defaults to a ThreadPoolExecutor with a maximum of 5 threads.
        logger: Logger
            The logger which will write logs from this object. Defaults: aws_lambda_powertools.logging.logger

    Example
    -----------
    def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:  #pylint: disable=unused-argument
    lock_client = DynamoDBLockClient(
        boto3.resource('dynamodb'),
        owner_name=f'{context.function_name}-{context.aws_request_id}',
        table_name=os.environ.get('LOCK_TABLE_NAME'),
        partition_key_name='name',
        sort_key_name='-',
        lease_duration=datetime.timedelta(seconds=20),
        heartbeat_period=datetime.timedelta(seconds=4),
    )

    lock = None
    try:
        lock = lock_client.acquire_lock(partition_key='alon', sort_key='-')
        do_something()
    except DynamoDBLockError as ex:
        print('dynamodb lock error', str(ex))
        raise InterruptedError() from ex
    finally:
        if lock:
            lock.release()
        lock_client.close()
    """

    # default values for class properties
    _DEFAULT_HEARTBEAT_PERIOD = datetime.timedelta(seconds=5)
    _DEFAULT_SAFE_PERIOD = datetime.timedelta(seconds=20)
    _DEFAULT_LEASE_DURATION = datetime.timedelta(seconds=30)
    _DEFAULT_EXPIRY_PERIOD = datetime.timedelta(hours=1)
    _DEFAULT_APP_CALLBACK_THREADPOOL_SIZE = 5
    # for optional create-table method
    _DEFAULT_READ_CAPACITY = 5
    _DEFAULT_WRITE_CAPACITY = 5

    # to help make the sort-key optional
    _DEFAULT_SORT_KEY_VALUE = "-"

    # DynamoDB "hard-coded" column names
    _COL_OWNER_NAME = "owner_name"
    _COL_LEASE_DURATION = "lease_duration"
    _COL_RECORD_VERSION_NUMBER = "record_version_number"

    def __init__(
        self,
        dynamodb_resource,
        owner_name,
        table_name: str,
        partition_key_name: str,
        sort_key_name: str,
        ttl_attribute_name=DEFAULT_TTL_ATTRIBUTE_NAME,
        heartbeat_period=_DEFAULT_HEARTBEAT_PERIOD,
        safe_period=_DEFAULT_SAFE_PERIOD,
        lease_duration=_DEFAULT_LEASE_DURATION,
        expiry_period=_DEFAULT_EXPIRY_PERIOD,
        app_callback_executor=None,
        logger: Optional[Logger] = None,
    ):
        """
        :param boto3.ServiceResource dynamodb_resource: mandatory argument
        :param str owner_name: owner name to set to the lock item, should be unique to allow attribution
        :param str table_name: the dynamoDB table name
        :param str partition_key_name: partition key name
        :param str sort_key_name: sort key name
        :param str ttl_attribute_name: defaults to 'expiry_time'
        :param datetime.timedelta heartbeat_period: How often to update DynamoDB to note that the
                instance is still running. It is recommended to make this at least 4 times smaller
                than the leaseDuration. Defaults to 5 seconds.
        :param datetime.timedelta safe_period: How long is it okay to go without a heartbeat before
                considering a lock to be in "danger". Defaults to 20 seconds.
        :param datetime.timedelta lease_duration: The length of time that the lease for the lock
                will be granted for. i.e. if there is no heartbeat for this period of time, then
                the lock will be considered as expired. Defaults to 30 seconds.
        :param datetime.timedelta expiry_period: The fallback expiry timestamp to allow DynamoDB
                to cleanup old locks after a server crash. This value should be significantly larger
                than the _lease_duration to ensure that clock-skew etc. are not an issue. Defaults
                to 1 hour.
        :param ThreadPoolExecutor app_callback_executor: The executor to be used for invoking the
                app_callbacks in case of un-expected errors. Defaults to a ThreadPoolExecutor with a
                maximum of 5 threads.
        :param Logger logger: you can pass logger in order to write logs
        """
        self.logger = logger if logger is not None else LOGGER
        self._uuid = uuid.uuid4().hex
        self._dynamodb_resource = dynamodb_resource
        self._table_name = table_name
        self._partition_key_name = partition_key_name
        self._sort_key_name = sort_key_name
        self._ttl_attribute_name = ttl_attribute_name
        self._owner_name = owner_name
        self._heartbeat_period = heartbeat_period
        self._safe_period = safe_period
        self._lease_duration = lease_duration
        self._expiry_period = expiry_period
        self._app_callback_executor = app_callback_executor or ThreadPoolExecutor(
            max_workers=self._DEFAULT_APP_CALLBACK_THREADPOOL_SIZE,
            thread_name_prefix="DynamoDBLockClient-AC-" + self._uuid + "-",
        )
        self._locks: Dict = {}
        self._shutting_down = False

        self._dynamodb_table = dynamodb_resource.Table(table_name)
        self._start_heartbeat_sender_thread()
        self._start_heartbeat_checker_thread()
        self.logger.info("dynamodb client created", client=str(self))

    def _start_heartbeat_sender_thread(self):
        """
        Creates and starts a daemon thread - that sends out periodic heartbeats for the active locks
        """
        self._heartbeat_sender_thread = threading.Thread(
            name="DynamoDBLockClient-HS-" + self._uuid, target=self._send_heartbeat_loop
        )
        self._heartbeat_sender_thread.daemon = True
        self._heartbeat_sender_thread.start()
        self.logger.info("started the heartbeat-sender thread", thread=str(self._heartbeat_sender_thread))

    def _send_heartbeat_and_sleep(
        self, lock: DynamoDBLock, lock_index: int, start_time: float, avg_loop_time: float
    ) -> None:
        self._send_heartbeat(lock)
        curr_loop_end_time = time.monotonic()
        next_loop_start_time = start_time + lock_index * avg_loop_time
        if curr_loop_end_time < next_loop_start_time:
            time.sleep(next_loop_start_time - curr_loop_end_time)

    def _wait_between_send_heartbeat_intervals(self, start_time: float, avg_loop_time: float) -> None:
        end_time = time.monotonic()
        next_start_time = start_time + self._heartbeat_period.total_seconds()
        if end_time < next_start_time and not self._shutting_down:
            time.sleep(next_start_time - end_time)
        elif end_time > next_start_time + avg_loop_time:
            self.logger.warning("sending heartbeats for all the locks took longer than the _heartbeat_period")

    def _wait_between_check_heartbeat_intervals(self, start_time: float) -> None:
        end_time = time.monotonic()
        next_start_time = start_time + self._heartbeat_period.total_seconds()
        if end_time < next_start_time:
            time.sleep(next_start_time - end_time)
        else:
            self.logger.warning("checking heartbeats for all the locks took longer than the _heartbeat_period")

    def _send_heartbeat_loop(self):
        """
        Keeps renewing the leases for the locks owned by this client - till the client is closed.

        The method has a while loop that wakes up on a periodic basis (as defined by the _heartbeat_period)
        and invokes the _send_heartbeat() method on each lock. It spreads the heartbeat-calls evenly over
        the heartbeat window - to minimize the DynamoDB write throughput requirements.
        """
        while not self._shutting_down:
            self.logger.info("starting a send_heartbeat loop")
            start_time = time.monotonic()
            locks = self._locks.copy()

            avg_loop_time = self._heartbeat_period.total_seconds() / len(locks) if locks else -1.0
            loop_index = 1
            for _uid, lock in locks.items():
                self._send_heartbeat_and_sleep(lock, loop_index, start_time, avg_loop_time)
                loop_index += 1

            self.logger.info("finished the send_heartbeat loop")
            self._wait_between_send_heartbeat_intervals(start_time, avg_loop_time)

    def _update_lock_freshness(self, lock: DynamoDBLock) -> None:
        """
        switches the record_version_number on the existing lock - which indicates owner is still alive
        and all other clients should reset their timers.

        this method is called on a background thread, it uses the app_callback to let the
        (lock requestor) app know when there are significant events in the lock lifecycle.

        :param DynamoDBLock lock: the lock instance that needs its lease to be renewed
        """
        new_record_version_number = str(uuid.uuid4())
        new_expiry_time = int(time.time() + self._expiry_period.total_seconds())

        # first, try to update the database
        self._update_lock_in_dynamodb(lock, new_record_version_number, new_expiry_time)

        # if successful, update the in-memory lock representations
        lock.record_version_number = new_record_version_number
        lock.expiry_time = new_expiry_time
        lock.last_updated_time = time.monotonic()
        lock.status = DynamoDBLock.LOCKED
        self.logger.info("successfully sent the heartbeat", lock_unique_identifier=lock.unique_identifier)

    def _send_heartbeat(self, lock: DynamoDBLock):
        """
        Renews the lease for the given lock.

        :param DynamoDBLock lock: the lock instance that needs its lease to be renewed
        """
        self.logger.info("sending a DynamoDBLock heartbeat", lock_unique_identifier=lock.unique_identifier)
        with lock.thread_lock:
            # the ddb-lock might have been released while waiting for the thread-lock
            if lock.unique_identifier not in self._locks:
                return

            if lock.status != DynamoDBLock.LOCKED:
                self.logger.info("skipping the heartbeat as the lock is not locked any more", status=lock.status)
                return

            self._update_lock_freshness(lock)

    def _start_heartbeat_checker_thread(self):
        """
        Creates and starts a daemon thread - that checks that the locks are heartbeat-ing as expected
        The Daemon Thread does not block the main thread from exiting and continues to run in the background
        """
        self._heartbeat_checker_thread = threading.Thread(
            name="DynamoDBLockClient-HC-" + self._uuid, daemon=True, target=self._check_heartbeat_loop
        )
        self._heartbeat_checker_thread.start()
        self.logger.info(
            "started the heartbeat-checker thread", heartbeat_checker_thread=str(self._heartbeat_checker_thread)
        )

    def _check_heartbeat_loop(self):
        """
        Keeps checking the locks to ensure that they are being updated as expected.
        The method has a while loop that wakes up on a periodic basis and check heartbeat on each lock.
        """
        while not self._shutting_down:
            self.logger.info("starting a check_heartbeat loop")
            start_time = time.monotonic()
            locks = self._locks.copy()

            for _uid, lock in locks.items():
                self._check_heartbeat(lock)

            self.logger.info("finished the check_heartbeat loop")
            self._wait_between_check_heartbeat_intervals(start_time)

    def _check_heartbeat(self, lock: DynamoDBLock):
        """
        Checks that the given lock's lease expiry is within the safe-period.

        As this method is called on a background thread, it uses the app_callback to let the
        app know when there are errors events in the lock lifecycle.

        1) LOCK_IN_DANGER
            When the heartbeat for a given lock has failed multiple times, and it is
            now in danger of going past its lease-duration without a successful heartbeat - at which
            point, any other client waiting to acquire the lock will consider it abandoned and take
            over. In this case, the app_callback should try to expedite the processing,  either
            commit or rollback its changes quickly, and release the lock.

        :param DynamoDBLock lock: the lock instance that needs its lease to be renewed
        """
        self.logger.info("checking a DynamoDBLock heartbeat", lock_unique_identifier=lock.unique_identifier)

        with lock.thread_lock:
            if lock.unique_identifier not in self._locks:
                self.logger.info("the lock already released", status=lock.unique_identifier)
                return

            if lock.status != DynamoDBLock.LOCKED:
                self.logger.info("skipping the check as the lock is not locked any more", status=lock.status)
                return

            safe_period_end_time = lock.last_updated_time + self._safe_period.total_seconds()
            if time.monotonic() < safe_period_end_time:
                self.logger.info("lock is safe", lock_unique_identifier=lock.unique_identifier)
            else:
                self.logger.warning("lock is in danger", lock_unique_identifier=lock.unique_identifier)
                lock.status = DynamoDBLock.IN_DANGER
                self._call_app_callback(lock, DynamoDBLockError.LOCK_IN_DANGER)
            self.logger.info("successfully checked the heartbeat", lock_unique_identifier=lock.unique_identifier)

    def _call_app_callback(self, lock: DynamoDBLock, code: str):
        """
        Utility function to route the app_callback through the thread-pool-executor

        :param DynamoDBLock lock: the lock for which the event is being fired
        :param str code: the notification event-type
        """
        self._app_callback_executor.submit(lock.app_callback, code, lock)

    def _register_new_lock_local_memory(self, lock: DynamoDBLock) -> None:
        lock.status = DynamoDBLock.LOCKED
        self._locks[lock.unique_identifier] = lock
        self.logger.info("successfully registered lock in memory", new_lock=str(lock))

    def _register_new_lock(self, lock: DynamoDBLock) -> DynamoDBLock:
        self.logger.info("no existing lock", lock_unique_identifier=lock.unique_identifier)
        self._add_new_lock_to_dynamodb(lock)
        self._register_new_lock_local_memory(lock)
        return lock

    def _register_acquired_locked_after_release(
        self, lock: DynamoDBLock, last_record_version_number: str
    ) -> DynamoDBLock:
        self.logger.warning(
            "existing lock`s lease has expired",
            existing_lock_identifier=lock.unique_identifier,
            last_record_version_number=last_record_version_number,
        )
        self._overwrite_existing_lock_in_dynamodb(lock, last_record_version_number)
        self._register_new_lock_local_memory(lock)
        return lock

    def _wait_between_acquire_lock_retry(
        self,
        lock_uid: str,
        start_time: int,
        retry_index: int,
        retry_timeout_time: int,
        retry_period: datetime.timedelta,
    ) -> None:
        curr_loop_end_time = time.monotonic()
        next_loop_start_time = start_time + retry_index * retry_period.total_seconds()
        if next_loop_start_time > retry_timeout_time:
            raise DynamoDBLockError(DynamoDBLockError.ACQUIRE_TIMEOUT, f"acquire_lock() timed out: {lock_uid}")
        elif next_loop_start_time > curr_loop_end_time:
            self.logger.info("sleeping before a retry", lock_uid=lock_uid)
            time.sleep(next_loop_start_time - curr_loop_end_time)

    def acquire_lock(
        self,
        partition_key,
        sort_key=_DEFAULT_SORT_KEY_VALUE,
        retry_period=None,
        retry_timeout=None,
        additional_attributes=None,
        app_callback=None,
    ):
        """
        Acquires a distributed DynamoDBLock for the given key(s).

        If the lock is currently held by a different client, then this client will keep retrying on
        a periodic basis. In that case, a few different things can happen:

        1) The other client releases the lock
        2) The other client dies, we will wait until the lock passed his expiry_time and we will try to acquire the lock
        3) This client goes over the max-retry-timeout-period
            the client failed to acquire the lock in the given time
        4) Race-condition amongst multiple lock-clients waiting to acquire released lock

        :param str partition_key: The primary lock identifier
        :param str sort_key: Forms a "composite identifier" along with the partition_key. Defaults to '-'
        :param datetime.timedelta retry_period: If the lock is not immediately available, how long
                should we wait between retries? Defaults to heartbeat_period.
        :param datetime.timedelta retry_timeout: If the lock is not available for an extended period,
                how long should we keep trying before giving up and timing out?
                Defaults to lease_duration + heartbeat_period.
        :param dict additional_attributes: Arbitrary application metadata to be stored with the lock
        :param Callable app_callback: Callback function that can be used to notify the app of lock got an error

        :return: A distributed lock instance
        """
        self.logger.info("trying to acquire lock", partition_key=partition_key, sort_key=sort_key)

        # plug in default values as needed
        if not retry_period:
            retry_period = self._heartbeat_period
        if not retry_timeout:
            retry_timeout = self._lease_duration + self._heartbeat_period

        new_lock = DynamoDBLock(
            partition_key=partition_key,
            sort_key=sort_key,
            owner_name=self._owner_name,
            lease_duration=self._lease_duration.total_seconds(),
            record_version_number=str(uuid.uuid4()),
            expiry_time=int(time.time() + self._expiry_period.total_seconds()),
            additional_attributes=additional_attributes,
            app_callback=app_callback,
            lock_client=self,
            logger=self.logger,
        )

        start_time = time.monotonic()
        retry_timeout_time = start_time + retry_timeout.total_seconds()
        retry_count = 0
        last_record_version_number = None
        last_version_fetch_time = -1.0
        while True:
            if self._shutting_down:
                raise DynamoDBLockError(DynamoDBLockError.CLIENT_SHUTDOWN, "Client already shut down")

            try:
                new_lock.last_updated_time = time.monotonic()
                new_lock.expiry_time = int(time.time() + self._expiry_period.total_seconds())

                self.logger.info(
                    "checking the database for existing owner", new_lock_unique_identifier=new_lock.unique_identifier
                )
                existing_lock = self._get_lock_from_dynamodb(partition_key, sort_key)

                if existing_lock is None:
                    return self._register_new_lock(new_lock)
                else:
                    last_version_elapsed_time = time.monotonic() - last_version_fetch_time
                    if existing_lock.record_version_number == last_record_version_number:
                        if last_version_elapsed_time > existing_lock.lease_duration:
                            return self._register_acquired_locked_after_release(new_lock, last_record_version_number)

                    else:
                        self.logger.info(
                            "the lock not released yet",
                            new_lock_unique_identifier=new_lock.unique_identifier,
                            last_record_version_number=last_record_version_number,
                            existing_lock_record_version_number=existing_lock.record_version_number,
                        )
                        last_record_version_number = existing_lock.record_version_number
                        last_version_fetch_time = time.monotonic()
            except ClientError as ex:
                if ex.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    self.logger.info(
                        "someone else beat us to it - just log-it, sleep and retry",
                        new_lock_unique_identifier=new_lock.unique_identifier,
                    )
                else:
                    raise DynamoDBLockError(DynamoDBLockError.UNKNOWN, str(ex))

            retry_count += 1
            self._wait_between_acquire_lock_retry(
                lock_uid=new_lock.unique_identifier,
                start_time=start_time,
                retry_index=retry_count,
                retry_timeout_time=retry_timeout_time,
                retry_period=retry_period,
            )

    def get_lock_status(self, lock_id: str) -> Optional[str]:
        if lock_id in self._locks:
            lock = self._locks.get(lock_id)
            if lock:
                return lock.status
        return None

    def release_lock(self, lock, best_effort=True):
        """
        Releases the given lock - by deleting it from the database.

        :param DynamoDBLock lock: The lock instance that needs to be released
        :param bool best_effort: If True, any exception will be ignored and logged
        """
        self.logger.info("releasing the lock", lock=str(lock))

        with lock.thread_lock:
            if lock.status not in [DynamoDBLock.LOCKED, DynamoDBLock.IN_DANGER]:
                self.logger.info("skipping the release as the lock is not locked any more", lock_status=lock.status)
                return

            if lock.unique_identifier in self._locks:
                lock.status = DynamoDBLock.RELEASED
                self._locks.pop(lock.unique_identifier)  # will stop send heartbeats
                self._delete_lock_from_dynamodb(lock, best_effort)
                self.logger.info("successfully released the lock", lock_unique_identifier=lock.unique_identifier)

    def _delete_lock_from_dynamodb(self, lock: DynamoDBLock, best_effort: bool) -> None:
        try:
            self._dynamodb_table.delete_item(
                Key={self._partition_key_name: lock.partition_key, self._sort_key_name: lock.sort_key},
                ConditionExpression="attribute_exists(#pk) AND attribute_exists(#sk) AND #rvn = :rvn",
                ExpressionAttributeNames={
                    "#pk": self._partition_key_name,
                    "#sk": self._sort_key_name,
                    "#rvn": self._COL_RECORD_VERSION_NUMBER,
                },
                ExpressionAttributeValues={
                    ":rvn": lock.record_version_number,
                },
            )
        except ClientError as ex:
            if best_effort:
                self.logger.warning(
                    "error occurred when deleting the lock from table",
                    lock_unique_identifier=lock.unique_identifier,
                    exc_info=str(ex),
                )
            elif ex.response["Error"]["Code"] == CONDITIONAL_CHECK_EXCEPTION:
                raise DynamoDBLockError(DynamoDBLockError.LOCK_STOLEN, "Lock was stolen by someone else") from ex
            else:
                raise DynamoDBLockError(DynamoDBLockError.UNKNOWN, str(ex)) from ex

    def _update_lock_in_dynamodb(self, lock: DynamoDBLock, new_record_version: str, expiry_time: int) -> None:
        try:
            self._dynamodb_table.update_item(
                Key={self._partition_key_name: lock.partition_key, self._sort_key_name: lock.sort_key},
                UpdateExpression="SET #rvn = :new_rvn, #et = :new_et",
                ConditionExpression="attribute_exists(#pk) AND attribute_exists(#sk) AND #rvn = :old_rvn",
                ExpressionAttributeNames={
                    "#pk": self._partition_key_name,
                    "#sk": self._sort_key_name,
                    "#rvn": self._COL_RECORD_VERSION_NUMBER,
                    "#et": self._ttl_attribute_name,
                },
                ExpressionAttributeValues={
                    ":old_rvn": lock.record_version_number,
                    ":new_rvn": new_record_version,
                    ":new_et": expiry_time,
                },
            )
        except ClientError as ex:
            if ex.response["Error"]["Code"] == CONDITIONAL_CHECK_EXCEPTION:
                # someone else stole our lock!
                self.logger.exception(
                    "LockStolenError while sending heartbeat", lock_unique_identifier=lock.unique_identifier
                )
                lock.status = DynamoDBLock.INVALID
                self._locks.pop(lock.unique_identifier)
                self._call_app_callback(
                    lock, DynamoDBLockError.LOCK_STOLEN
                )  # the app should abort its processing; no need to release
            else:
                self.logger.exception(
                    "ClientError while sending heartbeat",
                    lock_unique_identifier=lock.unique_identifier,
                    exc_info=str(ex),
                )
                raise DynamoDBLockError(code="UNKNOWN", message="ClientError while sending heartbeat") from ex

    def _add_new_lock_record_to_dynamodb(self, lock: DynamoDBLock):
        """
        :param DynamoDBLock lock: The lock instance that needs to be added to the database.
        """
        try:
            self.logger.debug("adding a new lock", lock=str(lock))
            self._dynamodb_table.put_item(
                Item=self._get_record_from_lock(lock),
                ConditionExpression="NOT(attribute_exists(#pk) AND attribute_exists(#sk))",
                ExpressionAttributeNames={
                    "#pk": self._partition_key_name,
                    "#sk": self._sort_key_name,
                },
            )
        except ClientError as ex:
            if ex.response["Error"]["Code"] == CONDITIONAL_CHECK_EXCEPTION:
                # someone else stole our lock!
                self.logger.exception(
                    "LockStolenError while sending heartbeat", lock_unique_identifier=lock.unique_identifier
                )
                lock.status = DynamoDBLock.INVALID
                self._locks.pop(lock.unique_identifier)
                self._call_app_callback(
                    lock, DynamoDBLockError.LOCK_STOLEN
                )  # the app should abort its processing; no need to release
            else:
                self.logger.exception(
                    "ClientError while sending heartbeat",
                    lock_unique_identifier=lock.unique_identifier,
                    exc_info=str(ex),
                )
                raise DynamoDBLockError(code="UNKNOWN", message="ClientError while sending heartbeat") from ex

    def _get_record_from_lock(self, lock: BaseDynamoDBLock) -> Dict:
        """
        Converts a BaseDynamoDBLock  instance to a DynamoDB 'Item' dict

        :param BaseDynamoDBLock lock: The lock instance to be serialized.
        """
        self.logger.debug("get item from lock", lock=str(lock))
        item = lock.additional_attributes.copy()
        item.update(
            {
                self._partition_key_name: lock.partition_key,
                self._sort_key_name: lock.sort_key,
                self._COL_OWNER_NAME: lock.owner_name,
                self._COL_LEASE_DURATION: Decimal.from_float(lock.lease_duration),
                self._COL_RECORD_VERSION_NUMBER: lock.record_version_number,
                self._ttl_attribute_name: lock.expiry_time,
            }
        )
        return item

    def _get_lock_from_dynamodb(self, partition_key, sort_key):
        """
        Loads the lock from the database - or returns None if not available.

        :rtype: BaseDynamoDBLock
        """
        self.logger.debug("getting the lock from dynamodb for", partition_key=partition_key, sort_key=sort_key)
        result = self._dynamodb_table.get_item(
            Key={self._partition_key_name: partition_key, self._sort_key_name: sort_key}, ConsistentRead=True
        )
        if "Item" in result:
            return self._get_lock_from_item(result["Item"])
        else:
            return None

    def _add_new_lock_to_dynamodb(self, lock):
        """
        Adds a new lock into the database - while checking that it does not exist already.

        :param DynamoDBLock lock: The lock instance that needs to be added to the database.
        """
        self.logger.debug("adding a new lock", lock=str(lock))
        self._dynamodb_table.put_item(
            Item=self._get_item_from_lock(lock),
            ConditionExpression="NOT(attribute_exists(#pk) AND attribute_exists(#sk))",
            ExpressionAttributeNames={
                "#pk": self._partition_key_name,
                "#sk": self._sort_key_name,
            },
        )

    def _overwrite_existing_lock_in_dynamodb(self, lock, record_version_number):
        """
        Overwrites an existing lock in the database - while checking that the version has not changed.

        :param DynamoDBLock lock: The new lock instance that needs to overwrite the old one in the database.
        :param str record_version_number: The version-number for the old lock instance in the database.
        """
        self.logger.debug(
            "overwriting existing-rvn with new lock", record_version_number=record_version_number, lock=str(lock)
        )
        self._dynamodb_table.put_item(
            Item=self._get_item_from_lock(lock),
            ConditionExpression="attribute_exists(#pk) AND attribute_exists(#sk) AND #rvn = :old_rvn",
            ExpressionAttributeNames={
                "#pk": self._partition_key_name,
                "#sk": self._sort_key_name,
                "#rvn": self._COL_RECORD_VERSION_NUMBER,
            },
            ExpressionAttributeValues={
                ":old_rvn": record_version_number,
            },
        )

    def _get_lock_from_item(self, item):
        """
        Converts a DynamoDB 'Item' dict to a BaseDynamoDBLock instance

        :param dict item: The DynamoDB 'Item' dict object to be de-serialized.
        :rtype: BaseDynamoDBLock
        """
        self.logger.debug("get lock from item", item=str(item))
        lock = BaseDynamoDBLock(
            partition_key=item.pop(self._partition_key_name),
            sort_key=item.pop(self._sort_key_name),
            owner_name=item.pop(self._COL_OWNER_NAME),
            lease_duration=float(item.pop(self._COL_LEASE_DURATION)),
            record_version_number=item.pop(self._COL_RECORD_VERSION_NUMBER),
            expiry_time=int(item.pop(self._ttl_attribute_name)),
            additional_attributes=item,
            logger=self.logger,
        )
        return lock

    def _get_item_from_lock(self, lock):
        """
        Converts a BaseDynamoDBLock (or subclass) instance to a DynamoDB 'Item' dict

        :param BaseDynamoDBLock lock: The lock instance to be serialized.
        :rtype: dict
        """
        self.logger.debug("get item from lock", lock=str(lock))
        item = lock.additional_attributes.copy()
        item.update(
            {
                self._partition_key_name: lock.partition_key,
                self._sort_key_name: lock.sort_key,
                self._COL_OWNER_NAME: lock.owner_name,
                self._COL_LEASE_DURATION: Decimal.from_float(lock.lease_duration),
                self._COL_RECORD_VERSION_NUMBER: lock.record_version_number,
                self._ttl_attribute_name: lock.expiry_time,
            }
        )
        return item

    def _release_all_locks(self):
        """
        Iterates over all the locks and releases each one.
        """
        self.logger.info("releasing all locks", locks=len(self._locks))
        for _uid, lock in self._locks.copy().items():
            self.release_lock(lock, best_effort=True)
            self._call_app_callback(lock, DynamoDBLockError.LOCK_STOLEN)

    def close(self, release_locks=False):
        """
        Shuts down the background thread - and releases all locks if so asked.

        By default, this method will NOT release all the locks - as releasing the locks while
        the application is still making changes assuming that it has the lock can be dangerous.
        As soon as a lock is released by this client, some other client may pick it up, and the
        associated app may start processing the underlying business entity in parallel.

        It is recommended that the application manage its shutdown-lifecycle such that all the
        worker threads operating under these locks are first terminated (committed or rolled-back),
        the corresponding locks released (one at a time - by each worker thread), and then the
        lock_client.close() method is called. Alternatively, consider letting the process die
        without releasing all the locks - they will be auto-released when their lease runs out
        after a while.

        :param bool release_locks: if True, releases all the locks. Defaults to False.
        """
        if self._shutting_down:
            return
        self.logger.info("shutting down")
        self._shutting_down = True
        self._heartbeat_sender_thread.join()
        self._heartbeat_checker_thread.join()
        if release_locks:
            self._release_all_locks()
        self._shutting_down = False  # getting ready for next lock

    def __str__(self):
        """
        Returns a readable string representation of this instance.
        """
        return "%s::%s" % (self.__class__.__name__, self.__dict__)

    @classmethod
    def create_dynamodb_table(
        cls,
        dynamodb_client,
        table_name,
        partition_key_name,
        sort_key_name,
        ttl_attribute_name=DEFAULT_TTL_ATTRIBUTE_NAME,
        read_capacity=_DEFAULT_READ_CAPACITY,
        write_capacity=_DEFAULT_WRITE_CAPACITY,
    ):
        """
        Helper method to create the DynamoDB table

        :param boto3.DynamoDB.Client dynamodb_client: mandatory argument
        :param str table_name: defaults to 'DynamoDBLockTable'
        :param str partition_key_name: defaults to 'lock_key'
        :param str sort_key_name: defaults to 'sort_key'
        :param str ttl_attribute_name: defaults to 'expiry_time'
        :param int read_capacity: the max TPS for strongly-consistent reads; defaults to 5
        :param int write_capacity: the max TPS for write operations; defaults to 5
        """
        dynamodb_client.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": partition_key_name, "KeyType": "HASH"},
                {"AttributeName": sort_key_name, "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": partition_key_name, "AttributeType": "S"},
                {"AttributeName": sort_key_name, "AttributeType": "S"},
            ],
            ProvisionedThroughput={"ReadCapacityUnits": read_capacity, "WriteCapacityUnits": write_capacity},
        )
        cls._wait_for_table_to_be_active(dynamodb_client, table_name)

        dynamodb_client.update_time_to_live(
            TableName=table_name, TimeToLiveSpecification={"Enabled": True, "AttributeName": ttl_attribute_name}
        )
        cls._wait_for_table_to_be_active(dynamodb_client, table_name)

    @classmethod
    def _wait_for_table_to_be_active(cls, dynamodb_client, table_name):
        while True:
            response = dynamodb_client.describe_table(TableName=table_name)
            status = response.get("Table", {}).get("TableStatus", "UNKNOWN")
            if status == "ACTIVE":
                break
            else:
                time.sleep(2)
