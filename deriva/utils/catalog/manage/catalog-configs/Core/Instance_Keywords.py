import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential
import deriva.core.ermrest_model as em
from deriva.core import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'Instance_Keywords'

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
    'Instance_RID': {
        chaise_tags.display: {
            'name': 'Instance'
        }
    }
}

column_comment = {}

column_acls = {}

column_acl_bindings = {}

column_defs = [
    em.Column.define(
        'Instance_RID', em.builtin_types['text'], annotations=column_annotations['Instance_RID'],
    ),
    em.Column.define('Keyword_RID', em.builtin_types['text'],
                     ),
]

visible_columns = {
    '*': [
        {
            'source': 'RID'
        }, {
            'source': [
                {
                    'outbound': ['Core', 'Instance_Keywords_Mapping_Instance_RID_fkey']
                }, 'RID'
            ]
        }, {
            'source': [{
                'outbound': ['Core', 'Instance_Keywords_Mapping_Keyword_RID_fkey']
            }, 'id']
        }, {
            'source': 'RCT'
        }, {
            'source': 'RMT'
        }, {
            'source': [{
                'outbound': ['Core', 'Instance_Keywords_RCB_fkey']
            }, 'ID']
        }, {
            'source': [{
                'outbound': ['Core', 'Instance_Keywords_RMB_fkey']
            }, 'ID']
        }, {
            'source': [{
                'outbound': ['Core', 'Instance_Keywords_Catalog_Group_fkey']
            }, 'ID']
        }
    ]
}

table_annotations = {chaise_tags.visible_columns: visible_columns, }

table_comment = None

table_acls = {}

table_acl_bindings = {}

key_defs = [
    em.Key.define(['RID'], constraint_names=[['Core', 'Instance_Keywords_RIDkey1']],
                  ),
    em.Key.define(
        ['Keyword_RID', 'Instance_RID'],
        constraint_names=[['Core', 'Instance_Keywords_Mapping_RID_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['Instance_RID'],
        'Core',
        'Instance', ['RID'],
        constraint_names=[['Core', 'Instance_Keywords_Mapping_Instance_RID_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['Keyword_RID'],
        'Vocab',
        'Keywords', ['id'],
        constraint_names=[['Core', 'Instance_Keywords_Mapping_Keyword_RID_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
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
