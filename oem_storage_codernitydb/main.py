from oem_framework.models.core import ModelRegistry
from oem_storage_codernitydb.database import DatabaseCodernityDbStorage
from oem_storage_codernitydb.indices import MetadataIndex, ItemIndex
from oem_framework.storage import ProviderStorage
from oem_framework.plugin import Plugin

from CodernityDB.database import RecordNotFound
from CodernityDB.database_super_thread_safe import SuperThreadSafeDatabase
import os


class CodernityDbStorage(ProviderStorage, Plugin):
    __key__ = 'codernitydb'

    def __init__(self, path=None):
        super(CodernityDbStorage, self).__init__()

        self.path = path

        self.database = SuperThreadSafeDatabase(path)

        if os.path.exists(path):
            self.database.open()

    #
    # Provider methods
    #

    def create(self, source, target):
        if os.path.exists(self.path):
            return True

        # Create database
        self.database.create()

        # Add indices
        self.database.add_index(MetadataIndex(self.path, 'anidb_imdb_metadata'))
        self.database.add_index(ItemIndex(self.path,     'anidb_imdb_item'))
        self.database.add_index(MetadataIndex(self.path, 'anidb_tvdb_metadata'))
        self.database.add_index(ItemIndex(self.path,     'anidb_tvdb_item'))

        self.database.add_index(MetadataIndex(self.path, 'imdb_anidb_metadata'))
        self.database.add_index(ItemIndex(self.path,     'imdb_anidb_item'))

        self.database.add_index(MetadataIndex(self.path, 'tvdb_anidb_metadata'))
        self.database.add_index(ItemIndex(self.path,     'tvdb_anidb_item'))

        return True

    def open_database(self, source, target, version=None, database=None):
        return ModelRegistry['Database'].load(
            DatabaseCodernityDbStorage.open(self, source, target, version, database),
            source, target
        )

    #
    # Index methods
    #

    def has_index(self, source, target, version):
        return len(list(
            self.database.all('%s_%s_metadata' % (source, target), limit=1)
        )) > 0

    def update_index(self, source, target, version, response):
        data = self.format.decode(ModelRegistry['Index'], self.format.load_file(response.raw))

        for key, item in data.get('items', {}).items():
            # Set attributes
            item['_'] = {
                't': 'metadata',
                'k': key,

                'c': {
                    's': source,
                    't': target
                }
            }

            # Store metadata in database
            self.database.insert(item)

        # Update index
        self.database.reindex_index('%s_%s_metadata' % (source, target))
        return True

    #
    # Item methods
    #

    def has_item(self, source, target, version, key):
        try:
            self.database.get('%s_%s_item' % (source, target), key)
            return True
        except RecordNotFound:
            pass

        return False

    def update_item(self, source, target, version, key, response, metadata):
        item = self.format.decode(
            ModelRegistry['Item'],
            self.format.load_file(response.raw),
            media=metadata.media
        )

        # Set attributes
        item['_'] = {
            't': 'item',
            'k': key,

            'c': {
                's': source,
                't': target
            }
        }

        # Store metadata in database
        self.database.insert(item)
        return True
