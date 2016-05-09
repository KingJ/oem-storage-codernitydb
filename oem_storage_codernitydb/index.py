from oem_framework.models.core import ModelRegistry
from oem_framework.plugin import Plugin
from oem_framework.storage import IndexStorage
from oem_storage_codernitydb.metadata import MetadataCodernityDbStorage


class IndexCodernityDbStorage(IndexStorage, Plugin):
    __key__ = 'codernitydb/index'

    def __init__(self, parent):
        super(IndexCodernityDbStorage, self).__init__()

        self.parent = parent

        self.name = None

    @classmethod
    def open(cls, parent):
        storage = cls(parent)
        storage.initialize(parent._client)
        return storage

    def initialize(self, client):
        super(IndexCodernityDbStorage, self).initialize(client)

        self.name = '%s_%s_metadata' % (self.parent.source, self.parent.target)

    def load(self, collection):
        index = ModelRegistry['Index'](collection, self)

        for item in self.main.database.all(self.name, with_doc=True):
            # Retrieve item document
            doc = item.get('doc')

            if doc is None:
                continue

            # Retrieve item key
            key = doc.get('_', {}).get('k')

            if key is None:
                continue

            # Update index items
            index.items[key] = doc

        return index

    def parse_metadata(self, collection, key, value):
        return ModelRegistry['Metadata'].from_dict(
            collection, value,
            key=str(key),
            storage=MetadataCodernityDbStorage.open(self.parent, key)
        )
