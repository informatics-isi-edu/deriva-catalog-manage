import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential
import deriva.core.ermrest_model as em
from deriva.core import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'Instance_Level'

schema_name = 'Vocab'

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
    'id': {
        chaise_tags.generated: None
    },
    'uri': {
        chaise_tags.generated: None
    },
    'name': {
        chaise_tags.display: {
            'name': 'Name'
        }
    },
    'description': {
        chaise_tags.display: {
            'name': 'Description'
        }
    },
    'synonyms': {
        chaise_tags.display: {
            'name': 'Synonyms'
        }
    },
    'Owner': {}
}

column_comment = {
    'id': 'The preferred Compact URI (CURIE) for this term.',
    'uri': 'The preferred URI for this term.',
    'name': 'The preferred human-readable name for this term.',
    'description': 'A longer human-readable description of this term.',
    'synonyms': 'Alternate human-readable names for this term.',
    'Owner': 'Group that can update the record.'
}

column_acls = {}

column_acl_bindings = {}

column_defs = [
    em.Column.define(
        'id',
        em.builtin_types['ermrest_curie'],
        nullok=False,
        default='CORE:{RID}',
        annotations=column_annotations['id'],
        comment=column_comment['id'],
    ),
    em.Column.define(
        'uri',
        em.builtin_types['ermrest_uri'],
        nullok=False,
        default='/id/{RID}',
        annotations=column_annotations['uri'],
        comment=column_comment['uri'],
    ),
    em.Column.define(
        'name',
        em.builtin_types['text'],
        nullok=False,
        annotations=column_annotations['name'],
        comment=column_comment['name'],
    ),
    em.Column.define(
        'description',
        em.builtin_types['markdown'],
        nullok=False,
        annotations=column_annotations['description'],
        comment=column_comment['description'],
    ),
    em.Column.define(
        'synonyms',
        em.builtin_types['text[]'],
        annotations=column_annotations['synonyms'],
        comment=column_comment['synonyms'],
    ),
    em.Column.define('Owner', em.builtin_types['text'], comment=column_comment['Owner'],
                     ),
]

table_display = {'row_name': {'row_markdown_pattern': '{{{name}}}'}}

visible_columns = {
    '*': [
        {
            'source': 'RID'
        }, {
            'source': 'name'
        }, {
            'source': 'description'
        }, {
            'source': 'synonyms'
        }, {
            'source': 'RCT'
        }, {
            'source': 'RMT'
        }, {
            'source': [{
                'outbound': ['Vocab', 'Instance_Level_RCB_fkey']
            }, 'ID']
        }, {
            'source': [{
                'outbound': ['Vocab', 'Instance_Level_RMB_fkey']
            }, 'ID']
        }, {
            'source': [{
                'outbound': ['Vocab', 'Instance_Level_Catalog_Group_fkey']
            }, 'ID']
        }
    ]
}

table_annotations = {
    chaise_tags.table_display: table_display,
    chaise_tags.visible_columns: visible_columns,
}

table_comment = None

table_acls = {}

table_acl_bindings = {}

key_defs = [
    em.Key.define(['id'], constraint_names=[['Vocab', 'Instance_Level_idkey1']],
                  ),
    em.Key.define(['RID'], constraint_names=[['Vocab', 'Instance_Level_RIDkey1']],
                  ),
    em.Key.define(['uri'], constraint_names=[['Vocab', 'Instance_Level_urikey1']],
                  ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['Vocab', 'Instance_Level_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['Vocab', 'Instance_Level_RMB_fkey']],
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
