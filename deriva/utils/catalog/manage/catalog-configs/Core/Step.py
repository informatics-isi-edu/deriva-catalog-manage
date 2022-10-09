import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential
import deriva.core.ermrest_model as em
from deriva.core import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'Step'

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
    'Step_Type_ID': {
        chaise_tags.display: {
            'name': 'Step Type'
        }
    },
    'Instance_RID': {
        chaise_tags.display: {
            'name': 'Instance'
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
    em.Column.define('Description', em.builtin_types['text'],
                     ),
    em.Column.define('Instruction', em.builtin_types['text'],
                     ),
    em.Column.define(
        'Step_Type_ID', em.builtin_types['text'], annotations=column_annotations['Step_Type_ID'],
    ),
    em.Column.define(
        'Instance_RID', em.builtin_types['text'], annotations=column_annotations['Instance_RID'],
    ),
    em.Column.define('Sequence', em.builtin_types['text'],
                     ),
    em.Column.define('Owner', em.builtin_types['text'], comment=column_comment['Owner'],
                     ),
]

table_display = {
    '*': {
        'row_order': [{
            'column': 'Sequence',
            'descending': False
        }]
    },
    'row_name': {
        'row_markdown_pattern': '{{{Name}}}'
    }
}

visible_columns = {
    '*': [
        {
            'source': 'RID'
        }, {
            'source': 'Name'
        }, {
            'source': 'Description'
        }, {
            'source': 'Instruction'
        }, {
            'source': [{
                'outbound': ['Core', 'Step_Steptype_ID_fkey']
            }, 'id']
        }, {
            'source': [{
                'outbound': ['Core', 'Step_Instance_RID_fkey']
            }, 'RID']
        }, {
            'source': 'Sequence'
        }, {
            'source': 'RCT'
        }, {
            'source': 'RMT'
        }, {
            'source': [{
                'outbound': ['Core', 'Step_RCB_fkey']
            }, 'ID']
        }, {
            'source': [{
                'outbound': ['Core', 'Step_RMB_fkey']
            }, 'ID']
        }, {
            'source': [{
                'outbound': ['Core', 'Step_Catalog_Group_fkey']
            }, 'ID']
        }
    ]
}

visible_foreign_keys = {
    '*': [
        ['Core', 'Step_InputFile_Mapping_Step_RID_fkey'],
        ['Core', 'Step_Program_Mapping_Step_RID_fkey'],
        ['Core', 'Step_OutputFile_Mapping_Step_RID_fkey']
    ]
}

export = {
    'templates': [
        {
            'type': 'BAG',
            'outputs': [
                {
                    'source': {
                        'api': 'attribute',
                        'path': 'I:=(Instance_RID)=(Core:Instance:RID)/Instance_Name:=I:Name,Step_RID:=M:RID,Step_Sequence:=M:Sequence,Step_Description:=M:Description'
                    },
                    'destination': {
                        'name': 'Step',
                        'type': 'csv'
                    }
                }, {
                    'source': {
                        'api': 'attribute',
                        'path': '(RID)=(Core:Step_Input_File:Step_RID)/(File_RID)=(Core:File:RID)/url:=URI'
                    },
                    'destination': {
                        'name': 'input_files',
                        'type': 'download'
                    }
                }, {
                    'source': {
                        'api': 'attribute',
                        'path': '(RID)=(Core:Step_Output_File:Step_RID)/(File_RID)=(Core:File:RID)/url:=URI'
                    },
                    'destination': {
                        'name': 'output_files',
                        'type': 'download'
                    }
                }
            ],
            'displayname': 'BDBag'
        }
    ]
}

table_annotations = {
    chaise_tags.export: export,
    chaise_tags.table_display: table_display,
    chaise_tags.visible_columns: visible_columns,
    chaise_tags.visible_foreign_keys: visible_foreign_keys,
}

table_comment = 'Step information of the instance'

table_acls = {}

table_acl_bindings = {}

key_defs = [
    em.Key.define(['RID'], constraint_names=[['Core', 'Step_RIDkey1']],
                  ),
    em.Key.define(
        ['Name', 'Instance_RID'],
        constraint_names=[['Core', 'Instance_RID_Step_Name_key']],
        comment='Instance plus step name must be distinct.',
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['Instance_RID'],
        'Core',
        'Instance', ['RID'],
        constraint_names=[['Core', 'Step_Instance_RID_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'display': {
                    'compact': {
                        'column_order': ['Instance_RID', 'Sequence']
                    }
                }
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['Step_Type_ID'],
        'Vocab',
        'Step_Type', ['id'],
        constraint_names=[['Core', 'Step_Steptype_ID_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['RCB'], 'public', 'ERMrest_Client', ['ID'], constraint_names=[['Core', 'Step_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'], 'public', 'ERMrest_Client', ['ID'], constraint_names=[['Core', 'Step_RMB_fkey']],
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
