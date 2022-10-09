import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential
import deriva.core.ermrest_model as em
from deriva.core import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {
    'forecast-writer': 'https://auth.globus.org/a060fd0b-3f13-11eb-85ca-0aecec13621f',
    'forecast-reader': 'https://auth.globus.org/a060fd0b-3f13-11eb-85ca-0aecec13621f',
    'forecast-curator': 'https://auth.globus.org/a060fd0b-3f13-11eb-85ca-0aecec13621f'
}

table_name = 'Catalog_Group'

schema_name = 'public'

column_annotations = {
    'RCT': {
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.generated: None,
        chaise_tags.immutable: None
    },
    'RMT': {
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.generated: None,
        chaise_tags.immutable: None
    },
    'RCB': {
        chaise_tags.display: {
            'name': 'Created By'
        },
        chaise_tags.generated: None,
        chaise_tags.immutable: None
    },
    'RMB': {
        chaise_tags.display: {
            'name': 'Modified By'
        },
        chaise_tags.generated: None,
        chaise_tags.immutable: None
    },
    'Display_Name': {
        chaise_tags.display: {
            'name': 'Display Name'
        }
    },
    'Owner': {}
}

column_comment = {'Owner': 'Group that can update the record.'}

column_acls = {}

column_acl_bindings = {}

column_defs = [
    em.Column.define(
        'Display_Name', em.builtin_types['text'], annotations=column_annotations['Display_Name'],
    ),
    em.Column.define('URL', em.builtin_types['text'],
                     ),
    em.Column.define('Description', em.builtin_types['text'],
                     ),
    em.Column.define('ID', em.builtin_types['text'], nullok=False,
                     ),
    em.Column.define('Owner', em.builtin_types['text'], comment=column_comment['Owner'],
                     ),
]

table_display = {
    '*': {
        'row_order': [{
            'column': 'Display_Name',
            'descending': False
        }]
    },
    'row_name': {
        'row_markdown_pattern': '{{{Display_Name}}}'
    }
}

visible_columns = {
    '*': [
        {
            'source': 'Display_Name'
        }, {
            'source': 'URL'
        }, {
            'source': 'Description'
        }, {
            'source': 'RCT'
        }, {
            'source': 'RCB'
        }
    ]
}

table_annotations = {
    chaise_tags.table_display: table_display,
    chaise_tags.visible_columns: visible_columns,
}

table_comment = None

table_acls = {
    'insert': [groups['forecast-writer'], groups['forecast-curator']],
    'select': [groups['forecast-reader']]
}

table_acl_bindings = {}

key_defs = [
    em.Key.define(['RID'], constraint_names=[['public', 'Catalog_Group_RIDkey1']],
                  ),
    em.Key.define(
        ['ID'],
        constraint_names=[['public', 'Group_ID_key']],
        comment='Compound key to ensure that columns sync up into Catalog_Groups on update.',
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['ID', 'URL', 'Description', 'Display_Name'],
        'public',
        'ERMrest_Group', ['ID', 'URL', 'Description', 'Display_Name'],
        constraint_names=[['public', 'Catalog_Group_Description1']],
        acls={
            'insert': [groups['forecast-curator']],
            'update': [groups['forecast-curator']]
        },
        acl_bindings={
            'set_owner': {
                'types': ['insert'],
                'scope_acl': ['*'],
                'projection': ['ID'],
                'projection_type': 'acl'
            }
        },
        on_update='CASCADE',
    ),
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['public', 'Catalog_Group_RCB_fkey']],
        acls={
            'insert': ['*'],
            'update': ['*']
        },
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['public', 'Catalog_Group_RMB_fkey']],
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


def main(catalog, mode, replace=False, really=False):
    updater = CatalogUpdater(catalog)
    table_def['column_annotations'] = column_annotations
    table_def['column_comment'] = column_comment
    updater.update_table(mode, schema_name, table_def, replace=replace, really=really)


if __name__ == "__main__":
    host = 'forecast.derivacloud.org'
    catalog_id = 1
    mode, replace, host, catalog_id = parse_args(host, catalog_id, is_table=True)
    catalog = ErmrestCatalog('https', host, catalog_id=catalog_id, credentials=get_credential(host))
    main(catalog, mode, replace)
