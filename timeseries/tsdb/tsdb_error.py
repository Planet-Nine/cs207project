import enum

# NOTE: Probably not necessary, though it can make test-writing easier.
# FIXME: Probably should've inherited from appropriate exceptions here.


class TSDBError(Exception):
    pass


class TSDBOperationError(Exception):
    pass


class TSDBConnectionError(Exception):
    pass

# FIXME: This should be replaced with a TSDB "response" Op, which could
#        encapsulate both a status code as well as (optionally) a select()
#        response.


class TSDBStatus(enum.IntEnum):
    OK = 0
    UNKNOWN_ERROR = 1
    INVALID_OPERATION = 2
    INVALID_KEY = 3
    INVALID_COMPONENT = 4
    PYPE_ERROR = 5

    @staticmethod
    def encoded_length():
        return 3

    def encode(self):
        return str.encode('{:3d}'.format(self.value))

    @classmethod
    def from_bytes(cls, data):
        return cls(int(data.decode()))
