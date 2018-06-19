from typing import Union


class TrieDbError(Exception):

    def __init__(self, msg: Union[str, bytes] = b'TrieDb Error'):
        if isinstance(msg, str):
            self.msg = msg.encode()
        else:
            self.msg = msg


class TrieDbBadRequest(TrieDbError):
    """ Bad request error """


class TrieDbProtocolError(TrieDbError):
    """ Protocol error """


class TrieDbConnectionError(TrieDbError):
    """ Connection Error """


class TrieDbClientError(TrieDbError):
    """ Client Error """
