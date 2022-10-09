import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args
from deriva.core import tag as chaise_tags
import deriva.core.ermrest_model as em

groups = {}

annotations = {
    'tag:isrd.isi.edu,2019:catalog-config': {
        'name': 'core',
        'groups': {
            'admin': 'https://auth.globus.org/80af39fa-9503-11e8-88d8-0a7d99bc78fe',
            'reader': 'https://auth.globus.org/5bd8b30e-9503-11e8-ba34-0e5b3fbbcf14',
            'writer': 'https://auth.globus.org/72bdb36c-9503-11e8-8c03-0e847f194132',
            'curator': 'https://auth.globus.org/23a4c100-24e9-11e9-8d33-0edc9bdd56a6'
        }
    },
}

acls = {}


def main(catalog, mode, replace=False):
    updater = CatalogUpdater(catalog)
    updater.update_catalog(mode, annotations, acls, replace=replace)


if __name__ == "__main__":
    host = 'core.isrd.isi.edu'
    catalog_id = 1
    mode, replace, host, catalog_id = parse_args(host, catalog_id, is_catalog=True)
    catalog = ErmrestCatalog('https', host, catalog_id=catalog_id, credentials=get_credential(host))
    main(catalog, mode, replace)
