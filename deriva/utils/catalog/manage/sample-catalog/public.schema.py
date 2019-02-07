import argparse
from attrdict import AttrDict
from deriva.core import ErmrestCatalog, get_credential, DerivaPathError
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {
    'isrd-systems': 'https://auth.globus.org/3938e0d0-ed35-11e5-8641-22000ab4b42b',
    'test-writer': 'https://auth.globus.org/646933ac-16f6-11e9-b9af-0edc9bdd56a6',
    'test-curator': 'https://auth.globus.org/86cd6ee0-16f6-11e9-b9af-0edc9bdd56a6',
    'test-reader': 'https://auth.globus.org/4966c7fe-16f6-11e9-8bb8-0ee7d80087ee'
}

schema_name = 'public'

table_names = ['ERMrest_Client', 'ERMrest_Group', 'Catalog_Group', ]

annotations = {chaise_tags.display: {'name_style': {'underline_space': True}}}

acls = {}

comment = 'standard public schema'

schema_def = em.Schema.define('public', comment=comment, acls=acls, annotations=annotations, )


def main(catalog, mode, replace=False):
    updater = CatalogUpdater(catalog)
    updater.update_catalog.update_schema(mode, schema_name, schema_def, replace=replace)


if __name__ == "__main__":
    server = 'dev.isrd.isi.edu'
    catalog_id = 55674
    mode, replace, server, catalog_id = parse_args(server, catalog_id, is_catalog=True)
    credential = get_credential(server)
    catalog = ErmrestCatalog('https', server, catalog_id, credentials=credential)
    main(catalog, mode, replace)

