import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential
import deriva.core.ermrest_model as em
from deriva.core import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {
    'core-writer': 'https://auth.globus.org/72bdb36c-9503-11e8-8c03-0e847f194132',
    'DERIVA Forecast Demo Creator': 'https://auth.globus.org/a060fd0b-3f13-11eb-85ca-0aecec13621f',
    'core-reader': 'https://auth.globus.org/5bd8b30e-9503-11e8-ba34-0e5b3fbbcf14',
    'core-curator': 'https://auth.globus.org/23a4c100-24e9-11e9-8d33-0edc9bdd56a6',
    'core-admin': 'https://auth.globus.org/80af39fa-9503-11e8-88d8-0a7d99bc78fe'
}

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

table_acls = {
    'owner': [groups['core-admin']],
    'write': [],
    'delete': [groups['core-curator']],
    'insert': [groups['core-curator'], groups['core-writer']],
    'select': ['*'],
    'update': [groups['core-curator']],
    'enumerate': ['*']
}

table_acl_bindings = {
    'self_service': {
        'types': ['update', 'delete'],
        'scope_acl': ['*'],
        'projection': ['RCB'],
        'projection_type': 'acl'
    },
    'self_service_group': {
        'types': ['update', 'delete'],
        'scope_acl': ['*'],
        'projection': ['Owner'],
        'projection_type': 'acl'
    },
    'self_service_creator': {
        'types': ['update', 'delete'],
        'scope_acl': ['*'],
        'projection': ['RCB'],
        'projection_type': 'acl'
    }
}

key_defs = [
    em.Key.define(['RID'], constraint_names=[['Core', 'File_RIDkey1']],
                  ),
    em.Key.define(
        ['Category_RID', 'Name'],
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
        acls={
            'insert': ['*'],
            'update': ['*']
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['Owner'],
        'public',
        'Catalog_Group', ['ID'],
        constraint_names=[['Core', 'File_Catalog_Group_fkey']],
        acls={
            'insert': [groups['core-curator']],
            'update': [groups['core-curator']]
        },
        acl_bindings={
            'set_owner': {
                'types': ['update', 'insert'],
                'scope_acl': ['*'],
                'projection': ['ID'],
                'projection_type': 'acl'
            }
        },
    ),
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['Core', 'File_RCB_fkey']],
        acls={
            'insert': ['*'],
            'update': ['*']
        },
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['Core', 'File_RMB_fkey']],
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
