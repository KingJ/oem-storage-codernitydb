from oem_framework.models.core import ModelRegistry
from oem_framework.plugin import Plugin
from oem_framework.storage import DatabaseStorage
from oem_storage_codernitydb.collection import CollectionCodernityDbStorage


class DatabaseCodernityDbStorage(DatabaseStorage, Plugin):
    __key__ = 'codernitydb/database'

    def __init__(self, parent, source, target, version=None, database=None):
        self.parent = parent
        self.source = source
        self.target = target
        self.version = version

        self.database = database

    def initialize(self, client):
        super(DatabaseCodernityDbStorage, self).initialize(client)

        if self.database is None:
            self.database = self.parent.database

    @classmethod
    def open(cls, parent, source, target, version=None, database=None):
        storage = cls(parent, source, target, version, database)
        storage.initialize(parent._client)
        return storage

    def open_collection(self, source, target):
        return ModelRegistry['Collection'].load(
            CollectionCodernityDbStorage.open(self, source, target),
            source, target
        )
