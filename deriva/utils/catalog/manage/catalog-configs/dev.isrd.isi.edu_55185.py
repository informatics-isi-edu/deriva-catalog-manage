import argparse
from attrdict import AttrDict
from deriva.core import ErmrestCatalog, get_credential, DerivaPathError
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args
from deriva.core.ermrest_config import tag as chaise_tags
import deriva.core.ermrest_model as em

groups = {
    'test-writer': 'https://auth.globus.org/646933ac-16f6-11e9-b9af-0edc9bdd56a6',
    'test-reader': 'https://auth.globus.org/4966c7fe-16f6-11e9-8bb8-0ee7d80087ee',
    'test-curator': 'https://auth.globus.org/86cd6ee0-16f6-11e9-b9af-0edc9bdd56a6',
    'isrd-systems': 'https://auth.globus.org/3938e0d0-ed35-11e5-8641-22000ab4b42b'
}

annotations = {
    'tag:isrd.isi.edu,2019:catalog-config': {
        'name': 'test',
        'groups': {
            'admin': 'https://auth.globus.org/3938e0d0-ed35-11e5-8641-22000ab4b42b',
            'reader': 'https://auth.globus.org/4966c7fe-16f6-11e9-8bb8-0ee7d80087ee',
            'writer': 'https://auth.globus.org/646933ac-16f6-11e9-b9af-0edc9bdd56a6',
            'curator': 'https://auth.globus.org/86cd6ee0-16f6-11e9-b9af-0edc9bdd56a6'
        }
    },
}

acls = {
    'owner': [groups['isrd-systems']],
    'select': [groups['test-writer'], groups['test-reader']],
    'create': [],
    'insert': [groups['test-curator'], groups['test-writer']],
    'delete': [groups['test-curator']],
    'enumerate': ['*'],
    'write': [],
    'update': [groups['test-curator']]
}


def main(catalog, mode, replace=False):
    updater = CatalogUpdater(catalog)
    updater.update_catalog(mode, annotations, acls, replace=replace)


if __name__ == "__main__":
    server = 'dev.isrd.isi.edu'
    catalog_id = 55185
    mode, replace, server, catalog_id = parse_args(server, catalog_id, is_catalog=True)
    credential = get_credential(server)
    catalog = ErmrestCatalog('https', server, catalog_id, credentials=credential)
    main(catalog, mode, replace)

