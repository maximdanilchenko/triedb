from typing import Optional


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
        return None

    async def backup(self):
        """ Background backup task """
        pass

    async def close(self):
        """ Stopping """
        pass
