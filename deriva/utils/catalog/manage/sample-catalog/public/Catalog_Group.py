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

table_name = 'Catalog_Group'

schema_name = 'public'

column_annotations = {
    'RCT': {
        chaise_tags.display: {
            'name': 'Creation Time'
        }
    },
    'RMT': {
        chaise_tags.display: {
            'name': 'Modified Time'
        }
    },
    'RCB': {
        chaise_tags.display: {
            'name': 'Created By'
        }
    },
    'RMB': {
        chaise_tags.display: {
            'name': 'Modified By'
        }
    },
    'URL': {
        chaise_tags.display: {
            'name': 'Group Management Page'
        },
        chaise_tags.column_display: {
            '*': {
                'markdown_pattern': '[**{{Display_Name}}**]({{{URL}}})'
            }
        }
    }
}

column_comment = {}

column_acls = {}

column_acl_bindings = {}

column_defs = [
    em.Column.define('Display_Name', em.builtin_types['text'],
                     ),
    em.Column.define('URL', em.builtin_types['text'], annotations=column_annotations['URL'],
                     ),
    em.Column.define('Description', em.builtin_types['text'],
                     ),
    em.Column.define('ID', em.builtin_types['text'], nullok=False,
                     ),
]

visible_columns = {
    '*': [
        {
            'source': 'RID'
        }, {
            'source': 'RCT'
        }, {
            'source': 'RMT'
        }, {
            'source': [{
                'outbound': ['public', 'Catalog_Group_RCB_fkey']
            }, 'ID']
        }, {
            'source': [{
                'outbound': ['public', 'Catalog_Group_RMB_fkey']
            }, 'ID']
        }, {
            'source': 'Display_Name'
        }, {
            'source': 'URL'
        }, {
            'source': 'Description'
        }, {
            'source': [{
                'outbound': ['public', 'Catalog_Group_ID1']
            }, 'ID']
        }
    ]
}

table_display = {'row_name': {'row_markdown_pattern': '{{{Display_Name}}}'}}

table_annotations = {
    chaise_tags.table_display: table_display,
    chaise_tags.visible_columns: visible_columns,
}

table_comment = None

table_acls = {
    'insert': [groups['test-writer'], groups['test-curator']],
    'select': [groups['test-reader']]
}

table_acl_bindings = {}

key_defs = [
    em.Key.define(['RID'], constraint_names=[('public', 'Catalog_Group_RIDkey1')],
                  ),
    em.Key.define(
        ['ID'],
        constraint_names=[('public', 'Group_ID_key')],
        comment='Compound key to ensure that columns sync up into Catalog_Groups on update.',
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['ID', 'Description', 'URL', 'Display_Name'],
        'public',
        'ERMrest_Group', ['ID', 'Description', 'URL', 'Display_Name'],
        constraint_names=[('public', 'Catalog_Group_ID1')],
        acls={
            'insert': [groups['test-curator']],
            'update': [groups['test-curator']]
        },
        acl_bindings={
            'set_owner': {
                'types': ['insert'],
                'projection': ['ID'],
                'projection_type': 'acl',
                'scope_acl': ['*']
            }
        },
        on_update='CASCADE',
    ),
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[('public', 'Catalog_Group_RCB_fkey')],
        acls={
            'insert': ['*'],
            'update': ['*']
        },
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[('public', 'Catalog_Group_RMB_fkey')],
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

