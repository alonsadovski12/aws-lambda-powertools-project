class DynamoDBLockError(Exception):
    """
    Wrapper for all kinds of errors that might occur during the acquire and release calls.
    """

    # code-constants
    CLIENT_SHUTDOWN = "CLIENT_SHUTDOWN"
    ACQUIRE_TIMEOUT = "ACQUIRE_TIMEOUT"
    LOCK_NOT_OWNED = "LOCK_NOT_OWNED"
    LOCK_STOLEN = "LOCK_STOLEN"
    LOCK_IN_DANGER = "LOCK_IN_DANGER"
    UNKNOWN = "UNKNOWN"

    def __init__(self, code="UNKNOWN", message="Unknown error"):
        Exception.__init__(self)
        self.code = code
        self.message = message

    def __str__(self):
        """
        Returns a readable string representation of this instance.
        """
        return f"{self.__class__.__name__}: {self.code} - {self.message}"
