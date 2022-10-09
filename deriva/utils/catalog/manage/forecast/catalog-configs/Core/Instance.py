import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential
import deriva.core.ermrest_model as em
from deriva.core import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {
    'forcast-writer': 'https://auth.globus.org/72bdb36c-9503-11e8-8c03-0e847f194132',
    'DERIVA Forecast Demo Creator': 'https://auth.globus.org/a060fd0b-3f13-11eb-85ca-0aecec13621f',
    'forecast-reader': 'https://auth.globus.org/5bd8b30e-9503-11e8-ba34-0e5b3fbbcf14',
    'forecast-curator': 'https://auth.globus.org/23a4c100-24e9-11e9-8d33-0edc9bdd56a6',
    'forecast-admin': 'https://auth.globus.org/80af39fa-9503-11e8-88d8-0a7d99bc78fe'
}

table_name = 'Instance'

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
    'Domain_ID': {
        chaise_tags.display: {
            'name': 'Domain'
        }
    },
    'Level_ID': {
        chaise_tags.display: {
            'name': 'Level'
        }
    },
    'Workflow_URI': {
        chaise_tags.asset: {
            'md5': 'Workflow_MD5',
            'url_pattern': '/hatrac/resources/public/workflow/file/{{{Workflow_MD5}}}',
            'filename_column': 'Workflow_Name',
            'byte_count_column': 'Workflow_Size'
        },
        chaise_tags.display: {
            'name': 'Workflow'
        },
        chaise_tags.column_display: {
            '*': {
                'markdown_pattern': '![Image]({{Workflow_URI}})'
            }
        }
    },
    'Science_ID': {
        chaise_tags.display: {
            'name': 'Science'
        }
    },
    'Owner': {},
    'Persistent_ID': {
        chaise_tags.generated: None,
        chaise_tags.column_display: {
            '*': {
                'markdown_pattern': '[{{{Persistent_ID}}}]({{{Persistent_ID}}})'
            }
        }
    },
    'Require_DOI?': {
        chaise_tags.column_display: {
            '*': {
                'pre_format': {
                    'format': '%t',
                    'bool_true_value': 'Yes',
                    'bool_false_value': 'No'
                }
            }
        }
    }
}

column_comment = {
    'Owner': 'Group that can update the record.',
    'Require_DOI?': 'True/Yes if a DOI is required (recommended if the record will be cited in a publication). '
}

column_acls = {}

column_acl_bindings = {}

column_defs = [
    em.Column.define('Name', em.builtin_types['text'],
                     ),
    em.Column.define('Description', em.builtin_types['text'],
                     ),
    em.Column.define(
        'Domain_ID', em.builtin_types['text'], annotations=column_annotations['Domain_ID'],
    ),
    em.Column.define(
        'Level_ID', em.builtin_types['text'], annotations=column_annotations['Level_ID'],
    ),
    em.Column.define('Workflow_Name', em.builtin_types['text'],
                     ),
    em.Column.define(
        'Workflow_URI', em.builtin_types['text'], annotations=column_annotations['Workflow_URI'],
    ),
    em.Column.define('Workflow_Size', em.builtin_types['int4'],
                     ),
    em.Column.define('Workflow_MD5', em.builtin_types['text'],
                     ),
    em.Column.define(
        'Science_ID', em.builtin_types['text'], annotations=column_annotations['Science_ID'],
    ),
    em.Column.define('Owner', em.builtin_types['text'], comment=column_comment['Owner'],
                     ),
    em.Column.define(
        'Persistent_ID',
        em.builtin_types['text'],
        annotations=column_annotations['Persistent_ID'],
    ),
    em.Column.define(
        'Require_DOI?',
        em.builtin_types['boolean'],
        annotations=column_annotations['Require_DOI?'],
        comment=column_comment['Require_DOI?'],
    ),
]

table_display = {
    '*': {
        'row_order': [{
            'column': 'RCT',
            'descending': True
        }]
    },
    'row_name': {
        'row_markdown_pattern': '{{{Name}}}'
    }
}

visible_columns = {
    'entry': [
        {
            'source': [{
                'outbound': ['Core', 'Instance_Science_ID_fkey']
            }, 'ID']
        }, {
            'source': 'Name'
        }, {
            'source': 'Description'
        }, {
            'source': [{
                'outbound': ['Core', 'Instance_Domain_RID_fkey']
            }, 'id']
        }, {
            'source': [{
                'outbound': ['Core', 'Instance_Level_RID_fkey']
            }, 'id']
        }, {
            'source': [
                {
                    'inbound': ['Core', 'Instance_Keywords_Mapping_Instance_RID_fkey']
                }, {
                    'outbound': ['Core', 'Instance_Keywords_Mapping_Keyword_RID_fkey']
                }, 'RID'
            ]
        }, {
            'source': [{
                'outbound': ['Core', 'Instance_RCB_fkey']
            }, 'ID']
        }, {
            'source': 'Workflow_URI'
        }, {
            'source': 'RCT'
        }, {
            'source': 'RMT'
        }, {
            'source': [{
                'outbound': ['Core', 'Instance_RCB_fkey']
            }, 'ID']
        }, {
            'source': [{
                'outbound': ['Core', 'Instance_Catalog_Group_fkey']
            }, 'ID']
        }
    ],
    'compact': [
        {
            'source': [{
                'outbound': ['Core', 'Instance_Science_ID_fkey']
            }, 'ID']
        }, {
            'source': 'Name'
        }, {
            'source': 'Description'
        }, {
            'source': [{
                'outbound': ['Core', 'Instance_Domain_RID_fkey']
            }, 'id']
        }, {
            'source': [{
                'outbound': ['Core', 'Instance_Level_RID_fkey']
            }, 'id']
        }, {
            'entity': 'true',
            'source': [
                {
                    'inbound': ['Core', 'Instance_Keywords_Mapping_Instance_RID_fkey']
                }, {
                    'outbound': ['Core', 'Instance_Keywords_Mapping_Keyword_RID_fkey']
                }, 'RID'
            ],
            'aggregate': 'array',
            'array_display': 'csv',
            'markdown_name': 'Keywords'
        }, {
            'source': 'RCT'
        }, {
            'source': [{
                'outbound': ['Core', 'Instance_RCB_fkey']
            }, 'ID']
        }, {
            'source': [{
                'outbound': ['Core', 'Instance_Catalog_Group_fkey']
            }, 'ID']
        }
    ],
    'detailed': [
        {
            'source': [{
                'outbound': ['Core', 'Instance_Science_ID_fkey']
            }, 'ID']
        }, {
            'source': 'Name'
        }, {
            'source': 'Description'
        }, {
            'source': [{
                'outbound': ['Core', 'Instance_Domain_RID_fkey']
            }, 'id']
        }, {
            'source': [{
                'outbound': ['Core', 'Instance_Level_RID_fkey']
            }, 'id']
        }, ['Core', 'Instance_Keywords_Mapping_Instance_RID_fkey'], {
            'source': 'Workflow_URI'
        }, {
            'source': 'RCT'
        }, {
            'source': 'RMT'
        }, {
            'source': [{
                'outbound': ['Core', 'Instance_RCB_fkey']
            }, 'ID']
        }, {
            'source': [{
                'outbound': ['Core', 'Instance_Catalog_Group_fkey']
            }, 'ID']
        }
    ]
}

visible_foreign_keys = {'*': [['Core', 'Step_Instance_RID_fkey']]}

table_annotations = {
    chaise_tags.table_display: table_display,
    chaise_tags.visible_columns: visible_columns,
    chaise_tags.visible_foreign_keys: visible_foreign_keys,
}

table_comment = None

table_acls = {
    'owner': [groups['forecast-admin']],
    'write': [],
    'delete': [groups['forecast-curator']],
    'insert': [groups['forecast-curator'], groups['forecast-writer']],
    'select': ['*'],
    'update': [groups['forecast-curator']],
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
    em.Key.define(['RID'], constraint_names=[['Core', 'Instance_RIDkey1']],
                  ),
    em.Key.define(
        ['Name'],
        constraint_names=[['Core', 'Instance_Name_key']],
        comment='Instance name must be distinct.',
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['Owner'],
        'public',
        'Catalog_Group', ['ID'],
        constraint_names=[['Core', 'Instance_Catalog_Group_fkey']],
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
        constraint_names=[['Core', 'Instance_RCB_fkey']],
        acls={
            'insert': ['*'],
            'update': ['*']
        },
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['Core', 'Instance_RMB_fkey']],
        acls={
            'insert': ['*'],
            'update': ['*']
        },
    ),
    em.ForeignKey.define(
        ['Domain_ID'],
        'Vocab',
        'Domain', ['id'],
        constraint_names=[['Core', 'Instance_Domain_RID_fkey']],
        acls={
            'insert': ['*'],
            'update': ['*']
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['Science_ID'],
        'Vocab',
        'Science', ['ID'],
        constraint_names=[['Core', 'Instance_Science_ID_fkey']],
        acls={
            'insert': ['*'],
            'update': ['*']
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['Level_ID'],
        'Vocab',
        'Instance_Level', ['id'],
        constraint_names=[['Core', 'Instance_Level_RID_fkey']],
        acls={
            'insert': ['*'],
            'update': ['*']
        },
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
    host = 'forecast.derivacloud.org'
    catalog_id = 1
    mode, replace, host, catalog_id = parse_args(host, catalog_id, is_table=True)
    catalog = ErmrestCatalog('https', host, catalog_id=catalog_id, credentials=get_credential(host))
    main(catalog, mode, replace)
