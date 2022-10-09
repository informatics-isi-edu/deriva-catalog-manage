import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential
import deriva.core.ermrest_model as em
from deriva.core import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'File'

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
    'Category_RID': {
        chaise_tags.display: {
            'name': 'Category'
        }
    },
    'URI': {
        chaise_tags.asset: {
            'md5': 'MD5',
            'url_pattern': '/hatrac/resources/public/file_category/{{{Category_RID}}}/file/{{{MD5}}}',
            'filename_column': 'Name',
            'byte_count_column': 'Size'
        },
        chaise_tags.display: {
            'name': 'File'
        }
    },
    'Owner': {}
}

column_comment = {'Owner': 'Group that can update the record.'}

column_acls = {}

column_acl_bindings = {}

column_defs = [
    em.Column.define(
        'Category_RID', em.builtin_types['text'], annotations=column_annotations['Category_RID'],
    ),
    em.Column.define('Name', em.builtin_types['text'],
                     ),
    em.Column.define('Description', em.builtin_types['text'],
                     ),
    em.Column.define('URI', em.builtin_types['text'], annotations=column_annotations['URI'],
                     ),
    em.Column.define('Size', em.builtin_types['int4'],
                     ),
    em.Column.define('MD5', em.builtin_types['text'],
                     ),
    em.Column.define('Owner', em.builtin_types['text'], comment=column_comment['Owner'],
                     ),
]

table_display = {'row_name': {'row_markdown_pattern': '{{{Name}}}'}}

visible_columns = {
    '*': [
        {
            'source': 'RID'
        }, {
            'source': [{
                'outbound': ['Core', 'File_Category_RID_fkey']
            }, 'RID']
        }, {
            'source': 'Name'
        }, {
            'source': 'Description'
        }, {
            'source': 'URI'
        }, {
            'source': 'Size'
        }, {
            'source': 'RCT'
        }, {
            'source': 'RMT'
        }, {
            'source': [{
                'outbound': ['Core', 'File_RCB_fkey']
            }, 'ID']
        }, {
            'source': [{
                'outbound': ['Core', 'File_RMB_fkey']
            }, 'ID']
        }, {
            'source': [{
                'outbound': ['Core', 'File_Catalog_Group_fkey']
            }, 'ID']
        }
    ]
}

table_annotations = {
    chaise_tags.table_display: table_display,
    chaise_tags.visible_columns: visible_columns,
}

table_comment = 'file information'

table_acls = {}

table_acl_bindings = {}

key_defs = [
    em.Key.define(['RID'], constraint_names=[['Core', 'File_RIDkey1']],
                  ),
    em.Key.define(
        ['Name', 'Category_RID'],
        constraint_names=[['Core', 'File_Category_RID_Name_key']],
        comment='file category and file name must be distinct.',
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['Category_RID'],
        'Vocab',
        'File_Category', ['RID'],
        constraint_names=[['Core', 'File_Category_RID_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['RCB'], 'public', 'ERMrest_Client', ['ID'], constraint_names=[['Core', 'File_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'], 'public', 'ERMrest_Client', ['ID'], constraint_names=[['Core', 'File_RMB_fkey']],
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
