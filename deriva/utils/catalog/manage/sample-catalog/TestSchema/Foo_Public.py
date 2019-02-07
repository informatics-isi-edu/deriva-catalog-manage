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

table_name = 'Foo_Public'

schema_name = 'TestSchema'

column_annotations = {'Owner': {}}

column_comment = {'Owner': 'Group that can update the record.'}

column_acls = {}

column_acl_bindings = {}

column_defs = [
    em.Column.define('Id', em.builtin_types['int4'], nullok=False,
                     ),
    em.Column.define('Field_0', em.builtin_types['int4'],
                     ),
    em.Column.define('Field_1', em.builtin_types['int4'],
                     ),
    em.Column.define('Field_2', em.builtin_types['boolean'],
                     ),
    em.Column.define('Field_3', em.builtin_types['float8'],
                     ),
    em.Column.define('Field_4', em.builtin_types['date'],
                     ),
    em.Column.define('Field_5', em.builtin_types['text'],
                     ),
    em.Column.define('Owner', em.builtin_types['text'], comment=column_comment['Owner'],
                     ),
]

table_annotations = {}

table_comment = None

table_acls = {}

table_acl_bindings = {}

key_defs = [
    em.Key.define(['RID'], constraint_names=[('TestSchema', 'Foo_Public_RIDkey1')],
                  ),
    em.Key.define(['Id'], constraint_names=[('TestSchema', 'Foo_Public_Id_key)')],
                  ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['Owner'],
        'public',
        'Catalog_Group', ['ID'],
        constraint_names=[('TestSchema', 'Foo_Public_Catalog_Group_fkey')],
        acls={
            'insert': [groups['test-curator']],
            'update': [groups['test-curator']]
        },
        acl_bindings={
            'set_owner': {
                'types': ['update', 'insert'],
                'projection': ['ID'],
                'projection_type': 'acl',
                'scope_acl': ['*']
            }
        },
    ),
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[('TestSchema', 'Foo_Public_RCB_fkey')],
        acls={
            'insert': ['*'],
            'update': ['*']
        },
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[('TestSchema', 'Foo_Public_RMB_fkey')],
        acls={
            'insert': ['*'],
            'update': ['*']
        },
    ),
]

table_def = em.Table.define(
    table_name,
    column_defs=column_defs,
    key_defs=key_defs,
    fkey_defs=fkey_defs,
    annotations=table_annotations,
    acls=table_acls,
    acl_bindings=table_acl_bindings,
    comment=table_comment,
    provide_system=True
)


def main(catalog, mode, replace=False):
    updater = CatalogUpdater(catalog)
    updater.update_table(mode, schema_name, table_def, replace=replace)


if __name__ == "__main__":
    server = 'dev.isrd.isi.edu'
    catalog_id = 55674
    mode, replace, server, catalog_id = parse_args(server, catalog_id, is_table=True)
    credential = get_credential(server)
    catalog = ErmrestCatalog('https', server, catalog_id, credentials=credential)
    main(catalog, mode, replace)

