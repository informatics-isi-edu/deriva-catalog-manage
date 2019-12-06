import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_modeling_post_process'

schema_name = 'PDB'

column_annotations = {
    'software_id': {},
    'dataset_group_id': {},
    'num_models_end': {},
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'protocol_id': {},
    'RMB': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Modified By'
        },
        chaise_tags.immutable: None
    },
    'RCB': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Created By'
        },
        chaise_tags.immutable: None
    },
    'type': {},
    'structure_id': {},
    'Owner': {},
    'details': {},
    'num_models_begin': {},
    'feature': {},
    'script_file_id': {},
    'step_id': {},
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    },
    'struct_assembly_id': {},
    'feature_name': {},
    'id': {},
    'analysis_id': {}
}

column_comment = {
    'software_id': 'A reference to table software.pdbx_ordinal.',
    'dataset_group_id': 'A reference to table ihm_dataset_group.id.',
    'num_models_end': 'type:int4\nThe number of models the the end of the post processing step.',
    'type': 'type:text\nThe type of post modeling analysis being carried out.',
    'protocol_id': 'A reference to table ihm_modeling_protocol.id.',
    'structure_id': 'A reference to table entry.id.',
    'Owner': 'Group that can update the record.',
    'details': 'type:text\nAdditional details regarding post processing.',
    'num_models_begin': 'type:int4\nThe number of models at the beginning of the post processing step.',
    'feature': 'type:text\nThe parameter/feature used in the post modeling analysis.',
    'script_file_id': 'A reference to table ihm_external_files.id.',
    'step_id': 'type:int4\nIn a multi-step process, this identifier denotes the particular\n step in the post modeling analysis.',
    'struct_assembly_id': 'A reference to table ihm_struct_assembly.id.',
    'feature_name': 'type:text\nThe name of the parameter/feature used in the post modeling analysis.\nexamples:Rosetta energy,GOAP (orientation-dependent all-atom statistical potential)',
    'id': 'type:int4\nA unique identifier for the post modeling analysis/step combination.',
    'analysis_id': 'type:int4\nAn identifier for the post modeling analysis. This data item accounts for\n multiple post-modeling analyses that can be carried out.'
}

column_acls = {}

column_acl_bindings = {}

column_defs = [
    em.Column.define(
        'structure_id',
        em.builtin_types['text'],
        nullok=False,
        comment=column_comment['structure_id'],
    ),
    em.Column.define(
        'analysis_id',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['analysis_id'],
    ),
    em.Column.define(
        'dataset_group_id', em.builtin_types['int4'], comment=column_comment['dataset_group_id'],
    ),
    em.Column.define('details', em.builtin_types['text'], comment=column_comment['details'],
                     ),
    em.Column.define('feature', em.builtin_types['text'], comment=column_comment['feature'],
                     ),
    em.Column.define(
        'feature_name', em.builtin_types['text'], comment=column_comment['feature_name'],
    ),
    em.Column.define('id', em.builtin_types['int4'], nullok=False, comment=column_comment['id'],
                     ),
    em.Column.define(
        'num_models_begin', em.builtin_types['int4'], comment=column_comment['num_models_begin'],
    ),
    em.Column.define(
        'num_models_end', em.builtin_types['int4'], comment=column_comment['num_models_end'],
    ),
    em.Column.define(
        'protocol_id',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['protocol_id'],
    ),
    em.Column.define(
        'script_file_id', em.builtin_types['int4'], comment=column_comment['script_file_id'],
    ),
    em.Column.define(
        'software_id', em.builtin_types['int4'], comment=column_comment['software_id'],
    ),
    em.Column.define(
        'step_id', em.builtin_types['int4'], nullok=False, comment=column_comment['step_id'],
    ),
    em.Column.define(
        'struct_assembly_id',
        em.builtin_types['int4'],
        comment=column_comment['struct_assembly_id'],
    ),
    em.Column.define('type', em.builtin_types['text'], comment=column_comment['type'],
                     ),
    em.Column.define('Owner', em.builtin_types['text'], comment=column_comment['Owner'],
                     ),
]

table_display = {'row_name': {'row_markdown_pattern': '{{{id}}}'}}

visible_foreign_keys = {
    'detailed': [['PDB', 'ihm_ensemble_info_post_process_id_fkey']],
    'filter': 'detailed'
}

visible_columns = {
    'entry': [
        {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_modeling_post_process_structure_id_fkey']
            }, 'RID']
        }, 'analysis_id', {
            'markdown_name': 'dataset group id',
            'comment': 'A reference to table ihm_dataset_group.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_modeling_post_process_dataset_group_id_fkey']
                }, 'RID'
            ]
        }, 'details', ['PDB', 'ihm_modeling_post_process_feature_term_fkey'], 'feature_name', 'id',
        'num_models_begin', 'num_models_end', {
            'markdown_name': 'protocol id',
            'comment': 'A reference to table ihm_modeling_protocol.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_modeling_post_process_protocol_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'script file id',
            'comment': 'A reference to table ihm_external_files.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_modeling_post_process_script_file_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'software id',
            'comment': 'A reference to table software.pdbx_ordinal.',
            'source': [{
                'outbound': ['PDB', 'ihm_modeling_post_process_software_id_fkey']
            }, 'RID']
        }, 'step_id', {
            'markdown_name': 'struct assembly id',
            'comment': 'A reference to table ihm_struct_assembly.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_modeling_post_process_struct_assembly_id_fkey']
                }, 'RID'
            ]
        }, ['PDB', 'ihm_modeling_post_process_type_term_fkey']
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_modeling_post_process_structure_id_fkey']
            }, 'RID']
        }, 'analysis_id', {
            'markdown_name': 'dataset group id',
            'comment': 'A reference to table ihm_dataset_group.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_modeling_post_process_dataset_group_id_fkey']
                }, 'RID'
            ]
        }, 'details', ['PDB', 'ihm_modeling_post_process_feature_term_fkey'], 'feature_name', 'id',
        'num_models_begin', 'num_models_end', {
            'markdown_name': 'protocol id',
            'comment': 'A reference to table ihm_modeling_protocol.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_modeling_post_process_protocol_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'script file id',
            'comment': 'A reference to table ihm_external_files.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_modeling_post_process_script_file_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'software id',
            'comment': 'A reference to table software.pdbx_ordinal.',
            'source': [{
                'outbound': ['PDB', 'ihm_modeling_post_process_software_id_fkey']
            }, 'RID']
        }, 'step_id', {
            'markdown_name': 'struct assembly id',
            'comment': 'A reference to table ihm_struct_assembly.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_modeling_post_process_struct_assembly_id_fkey']
                }, 'RID'
            ]
        }, ['PDB', 'ihm_modeling_post_process_type_term_fkey'],
        ['PDB', 'ihm_modeling_post_process_RCB_fkey'],
        ['PDB', 'ihm_modeling_post_process_RMB_fkey'], 'RCT', 'RMT',
        ['PDB', 'ihm_modeling_post_process_Owner_fkey']
    ]
}

table_annotations = {
    chaise_tags.visible_columns: visible_columns,
    chaise_tags.table_display: table_display,
    chaise_tags.visible_foreign_keys: visible_foreign_keys,
}

table_comment = None

table_acls = {}

table_acl_bindings = {}

key_defs = [
    em.Key.define(['RID'], constraint_names=[['PDB', 'ihm_modeling_post_process_RIDkey1']],
                  ),
    em.Key.define(
        ['id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_modeling_post_process_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['feature'],
        'Vocab',
        'ihm_modeling_post_process_feature_term', ['ID'],
        constraint_names=[['PDB', 'ihm_modeling_post_process_feature_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['type'],
        'Vocab',
        'ihm_modeling_post_process_type_term', ['ID'],
        constraint_names=[['PDB', 'ihm_modeling_post_process_type_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_modeling_post_process_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_modeling_post_process_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_modeling_post_process_structure_id_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'dataset_group_id'],
        'PDB',
        'ihm_dataset_group', ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_modeling_post_process_dataset_group_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'protocol_id'],
        'PDB',
        'ihm_modeling_protocol', ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_modeling_post_process_protocol_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'script_file_id'],
        'PDB',
        'ihm_external_files', ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_modeling_post_process_script_file_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'software_id'],
        'PDB',
        'software', ['structure_id', 'pdbx_ordinal'],
        constraint_names=[['PDB', 'ihm_modeling_post_process_software_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['struct_assembly_id', 'structure_id'],
        'PDB',
        'ihm_struct_assembly', ['id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_modeling_post_process_struct_assembly_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
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
    host = 'pdb.isrd.isi.edu'
    catalog_id = 9
    mode, replace, host, catalog_id = parse_args(host, catalog_id, is_table=True)
    catalog = DerivaCatalog(host, catalog_id=catalog_id, validate=False)
    main(catalog, mode, replace)
