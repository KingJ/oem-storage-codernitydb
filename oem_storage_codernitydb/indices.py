from CodernityDB.tree_index import TreeBasedIndex
from hashlib import md5


class MetadataIndex(TreeBasedIndex):
    _version = 1

    def __init__(self, db_path, name, *args, **kwargs):
        kwargs['name'] = name
        kwargs['key_format'] = '32s'

        super(MetadataIndex, self).__init__(db_path, *args, **kwargs)

        fragments = name.split('_')

        if len(fragments) != 3:
            raise ValueError('Invalid index name: %r' % name)

        self.source = fragments[0]
        self.target = fragments[1]

    def make_key(self, key):
        return md5(str(key)).hexdigest()

    def make_key_value(self, data):
        attributes = data.get('_', {})

        if attributes.get('t') != 'metadata':
            return

        collection = attributes.get('c', {})

        if collection.get('s') != self.source or collection.get('t') != self.target:
            return

        return md5(str(attributes.get('k'))).hexdigest(), None


class ItemIndex(TreeBasedIndex):
    _version = 1

    def __init__(self, db_path, name, *args, **kwargs):
        kwargs['name'] = name
        kwargs['key_format'] = '32s'

        super(ItemIndex, self).__init__(db_path, *args, **kwargs)

        fragments = name.split('_')

        if len(fragments) != 3:
            raise ValueError('Invalid index name: %r' % name)

        self.source = fragments[0]
        self.target = fragments[1]

    def make_key(self, key):
        return md5(str(key)).hexdigest()

    def make_key_value(self, data):
        attributes = data.get('_', {})

        if attributes.get('t') != 'item':
            return

        collection = attributes.get('c', {})

        if collection.get('s') != self.source or collection.get('t') != self.target:
            return

        return md5(str(attributes.get('k'))).hexdigest(), None
