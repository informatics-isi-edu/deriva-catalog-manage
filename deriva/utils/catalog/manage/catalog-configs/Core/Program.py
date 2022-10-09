import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential
import deriva.core.ermrest_model as em
from deriva.core import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'Program'

schema_name = 'Core'

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
    'Code_Reference': {
        chaise_tags.column_display: {
            '*': {
                'markdown_pattern': '[{{{Code_Reference}}}]({{{Code_Reference}}})'
            }
        }
    },
    'Owner': {}
}

column_comment = {'Owner': 'Group that can update the record.'}

column_acls = {}

column_acl_bindings = {}

column_defs = [
    em.Column.define('Name', em.builtin_types['text'],
                     ),
    em.Column.define('Algorithm', em.builtin_types['text'],
                     ),
    em.Column.define('Description', em.builtin_types['text'],
                     ),
    em.Column.define(
        'Code_Reference',
        em.builtin_types['text'],
        annotations=column_annotations['Code_Reference'],
    ),
    em.Column.define('Version', em.builtin_types['text'],
                     ),
    em.Column.define('Environment', em.builtin_types['text'],
                     ),
    em.Column.define('Owner', em.builtin_types['text'], comment=column_comment['Owner'],
                     ),
]

visible_columns = {
    '*': [
        {
            'source': 'RID'
        }, {
            'source': 'Name'
        }, {
            'source': 'Algorithm'
        }, {
            'source': 'Description'
        }, {
            'source': 'Code_Reference'
        }, {
            'source': 'Version'
        }, {
            'source': 'Environment'
        }, {
            'source': 'RCT'
        }, {
            'source': 'RMT'
        }, {
            'source': [{
                'outbound': ['Core', 'Program_RCB_fkey']
            }, 'ID']
        }, {
            'source': [{
                'outbound': ['Core', 'Program_RMB_fkey']
            }, 'ID']
        }, {
            'source': [{
                'outbound': ['Core', 'Program_Catalog_Group_fkey']
            }, 'ID']
        }
    ]
}

table_annotations = {chaise_tags.visible_columns: visible_columns, }

table_comment = 'program information'

table_acls = {}

table_acl_bindings = {}

key_defs = [
    em.Key.define(
        ['Name'],
        constraint_names=[['Core', 'Program_RID_Name_key']],
        comment='program name must be distinct.',
    ),
    em.Key.define(['RID'], constraint_names=[['Core', 'Program_RIDkey1']],
                  ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['Core', 'Program_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['Core', 'Program_RMB_fkey']],
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
    host = 'core.isrd.isi.edu'
    catalog_id = 1
    mode, replace, host, catalog_id = parse_args(host, catalog_id, is_table=True)
    catalog = ErmrestCatalog('https', host, catalog_id=catalog_id, credentials=get_credential(host))
    main(catalog, mode, replace)
