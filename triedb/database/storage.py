from typing import Optional
import string

from datrie import Trie

from triedb.exceptions import TrieDbBadRequest


class BackupWorker:

    def __init__(self, storage, *,
                 frequency: Optional[int],
                 path: str):
        self.storage = storage
        self.frequency = frequency
        self.path = path

    def restore(self):
        """ Restore storage from file on startup """
        if self.path:
            pass
        return Trie(string.ascii_lowercase)

    async def backup(self):
        """ Background backup task """
        pass

    def close(self):
        """ Stopping """
        pass


class TrieStorage:

    def __init__(self,
                 backup_frequency: Optional[int]=None,
                 backup_path: str='data.trie'):

        self.backup_worker: BackupWorker = BackupWorker(
            self,
            frequency=backup_frequency,
            path=backup_path
        )

        if self.backup_worker:
            self._store = self.backup_worker.restore()
        else:
            self._store = Trie(string.ascii_lowercase)

        self._commands = {
            b'SET': self.set,

            b'EXISTS': self.exists,
            b'PEXISTS': self.pexists,

            b'GET': self.get,
            b'PGET': self.pget,
            b'PGETL': self.pgetl,
            b'WPGET': self.wpget,

            b'FLUSH': self.flush,
        }

    def execute(self, command, *args):
        try:
            return self._commands[command](*args)
        except KeyError:
            raise TrieDbBadRequest(b'Command does not exists')

    def set(self, *args):
        """ Set key to value """

    def exists(self, *args):
        """ Check if key exists """

    def pexists(self, *args):
        """ Check if keys with prefix exists """

    def get(self, *args):
        """ Get value of key """

    def pget(self, *args):
        """ Get all key/value where keys are prefixes of word """

    def pgetl(self, *args):
        """ Get one key/value where key is longest prefix of word """

    def wpget(self, *args):
        """ Get all key/value keys where word is prefix """

    def flush(self, *args):
        """ Flush database """
