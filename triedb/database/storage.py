import string
import asyncio
from itertools import chain
from typing import Optional

from datrie import Trie

from triedb.exceptions import TrieDbBadRequest
from .backup import BackupWorker

ALLOWED_KEY_SYMBOLS = string.ascii_letters + string.digits


class TrieStorage:

    def __init__(self,
                 backup_frequency: Optional[int]=None,
                 backup_path: str='data.trie',
                 allowed_key_symbols=ALLOWED_KEY_SYMBOLS):

        self.backup_worker: BackupWorker = BackupWorker(
            self,
            frequency=backup_frequency,
            path=backup_path
        )

        self._store: Trie = None
        self._backup_task: asyncio.Future = None
        self._working = False
        self.last_backup = None

        if self.backup_worker:
            self._store = self.backup_worker.restore()

        if self._store is None:
            self._store = Trie(allowed_key_symbols)

        self._commands = {
            b'SET': self.set,
            b'EXISTS': self.exists,
            b'PEXISTS': self.pexists,
            b'GET': self.get,
            b'PGET': self.pget,
            b'PGETL': self.pgetl,
            b'WPGET': self.wpget,
            b'FLUSH': self.flush,
            b'ECHO': self.echo
        }

    def start(self):
        self._backup_task = asyncio.ensure_future(self.backup_worker.backup())
        self._working = True

    async def close(self):
        self._backup_task.cancel()
        await self.backup_worker.close()
        self._working = False

    @staticmethod
    def check_length(length, command, *args):
        if len(args) != length:
            raise TrieDbBadRequest(b'Wrong number of arguments for %s' % command)

    def execute(self, command, *args):
        if not self._working:
            raise TrieDbBadRequest(b'TrieDb is not working yet')
        try:
            return self._commands[command](*args)
        except KeyError:
            raise TrieDbBadRequest(b'Command does not exists')

    def set(self, *args):
        """ Set key to value """
        self.check_length(2, b'SET', *args)
        try:
            self._store[args[0]] = args[1]
        except TypeError:
            raise TrieDbBadRequest(b'Wrong type for key argument for SET')

    def exists(self, *args):
        """ Check if keys exists """
        if not args:
            raise TrieDbBadRequest(b'Wrong number of arguments for EXISTS')
        return sum(key in self._store for key in args)

    def pexists(self, *args):
        """ Check if keys with prefix exists """
        if not args:
            raise TrieDbBadRequest(b'Wrong number of arguments for PEXISTS')
        return sum(self._store.has_keys_with_prefix(pref) for pref in args)

    def get(self, *args):
        """ Get value of key """
        self.check_length(1, b'GET', *args)
        try:
            return self._store.get(args[0])
        except TypeError:
            raise TrieDbBadRequest(b'Wrong type for key argument for GET')

    def pget(self, *args):
        """ Get all key/value where keys are prefixes of word """
        self.check_length(1, b'PGET', *args)
        try:
            return list(chain(*self._store.prefix_items(args[0])))
        except TypeError:
            raise TrieDbBadRequest(b'Wrong type for word argument for PGET')

    def pgetl(self, *args):
        """ Get one key/value where key is longest prefix of word """
        self.check_length(1, b'PGETL', *args)
        try:
            return self._store.longest_prefix_item(args[0])
        except TypeError:
            raise TrieDbBadRequest(b'Wrong type for word argument for PGETL')
        except KeyError:
            return None

    def wpget(self, *args):
        self.check_length(1, b'WPGET', *args)
        """ Get all key/value keys where word is prefix """
        try:
            return list(chain(*self._store.items(args[0] or None)))
        except TypeError:
            raise TrieDbBadRequest(b'Wrong type for word argument for WPGET')

    def flush(self, *_):
        """ Flush database """
        self._store.clear()

    def echo(self, *args):
        """ Echo for connection checking """
        self.check_length(1, b'ECHO', *args)
        return args[0]
