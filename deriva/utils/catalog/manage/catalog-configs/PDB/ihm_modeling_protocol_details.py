import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_modeling_protocol_details'

schema_name = 'PDB'

column_annotations = {
    'dataset_group_id': {},
    'multi_scale_flag': {},
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
    'description': {},
    'step_method': {},
    'Owner': {},
    'ensemble_flag': {},
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    },
    'struct_assembly_id': {},
    'num_models_end': {},
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'multi_state_flag': {},
    'protocol_id': {},
    'step_name': {},
    'structure_id': {},
    'struct_assembly_description': {},
    'ordered_flag': {},
    'step_id': {},
    'num_models_begin': {},
    'script_file_id': {},
    'id': {},
    'software_id': {}
}

column_comment = {
    'dataset_group_id': 'A reference to table ihm_dataset_group.id.',
    'num_models_end': 'type:int4\nThe number of models at the end of the step.',
    'step_name': 'type:text\nThe name or type of the modeling step.\nexamples:Sampling/Scoring,Refinement',
    'multi_state_flag': 'type:text\nA flag to indicate if the modeling is multi state.',
    'Owner': 'Group that can update the record.',
    'protocol_id': 'A reference to table ihm_modeling_protocol.id.',
    'script_file_id': 'A reference to table ihm_external_files.id.',
    'structure_id': 'A reference to table entry.id.',
    'description': 'type:text\nTextual description of the protocol step.',
    'step_method': 'type:text\nDescription of the method involved in the modeling step.\nexamples:Replica exchange monte carlo,Simulated annealing monte carlo,Monte carlo sampling',
    'struct_assembly_description': 'type:text\nA textual description of the structural assembly being modeled.\nexamples:Nup84 sub-complex,PhoQ',
    'ordered_flag': 'type:text\nA flag to indicate if the modeling involves an ensemble ordered by time or other order.',
    'step_id': 'type:int4\nAn index for a particular step within the modeling protocol.',
    'num_models_begin': 'type:int4\nThe number of models in the beginning of the step.',
    'ensemble_flag': 'type:text\nA flag to indicate if the modeling involves an ensemble.',
    'multi_scale_flag': 'type:text\nA flag to indicate if the modeling is multi scale.',
    'struct_assembly_id': 'A reference to table ihm_struct_assembly.id.',
    'id': 'type:int4\nA unique identifier for the modeling protocol/step combination.',
    'software_id': 'A reference to table software.pdbx_ordinal.'
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
        'dataset_group_id', em.builtin_types['int4'], comment=column_comment['dataset_group_id'],
    ),
    em.Column.define(
        'description', em.builtin_types['text'], comment=column_comment['description'],
    ),
    em.Column.define(
        'ensemble_flag', em.builtin_types['text'], comment=column_comment['ensemble_flag'],
    ),
    em.Column.define('id', em.builtin_types['int4'], nullok=False, comment=column_comment['id'],
                     ),
    em.Column.define(
        'multi_scale_flag', em.builtin_types['text'], comment=column_comment['multi_scale_flag'],
    ),
    em.Column.define(
        'multi_state_flag', em.builtin_types['text'], comment=column_comment['multi_state_flag'],
    ),
    em.Column.define(
        'num_models_begin', em.builtin_types['int4'], comment=column_comment['num_models_begin'],
    ),
    em.Column.define(
        'num_models_end', em.builtin_types['int4'], comment=column_comment['num_models_end'],
    ),
    em.Column.define(
        'ordered_flag', em.builtin_types['text'], comment=column_comment['ordered_flag'],
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
        'step_method', em.builtin_types['text'], comment=column_comment['step_method'],
    ),
    em.Column.define('step_name', em.builtin_types['text'], comment=column_comment['step_name'],
                     ),
    em.Column.define(
        'struct_assembly_description',
        em.builtin_types['text'],
        comment=column_comment['struct_assembly_description'],
    ),
    em.Column.define(
        'struct_assembly_id',
        em.builtin_types['int4'],
        comment=column_comment['struct_assembly_id'],
    ),
    em.Column.define('Owner', em.builtin_types['text'], comment=column_comment['Owner'],
                     ),
]

visible_columns = {
    'entry': [
        {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_modeling_protocol_details_structure_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'dataset group id',
            'comment': 'A reference to table ihm_dataset_group.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_modeling_protocol_details_dataset_group_id_fkey']
                }, 'RID'
            ]
        }, 'description', ['PDB', 'ihm_modeling_protocol_details_ensemble_flag_term_fkey'], 'id',
        ['PDB', 'hm_modeling_protocol_details_multi_scale_flag_term_fkey'],
        ['PDB', 'hm_modeling_protocol_details_multi_state_flag_term_fkey'], 'num_models_begin',
        'num_models_end', ['PDB', 'ihm_modeling_protocol_details_ordered_flag_term_fkey'], {
            'markdown_name': 'protocol id',
            'comment': 'A reference to table ihm_modeling_protocol.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_modeling_protocol_details_protocol_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'script file id',
            'comment': 'A reference to table ihm_external_files.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_modeling_protocol_details_script_file_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'software id',
            'comment': 'A reference to table software.pdbx_ordinal.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_modeling_protocol_details_software_id_fkey']
                }, 'RID'
            ]
        }, 'step_id', 'step_method', 'step_name', 'struct_assembly_description', {
            'markdown_name': 'struct assembly id',
            'comment': 'A reference to table ihm_struct_assembly.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_modeling_protocol_details_struct_assembly_id_fkey']
                }, 'RID'
            ]
        }
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_modeling_protocol_details_structure_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'dataset group id',
            'comment': 'A reference to table ihm_dataset_group.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_modeling_protocol_details_dataset_group_id_fkey']
                }, 'RID'
            ]
        }, 'description', ['PDB', 'ihm_modeling_protocol_details_ensemble_flag_term_fkey'], 'id',
        ['PDB', 'hm_modeling_protocol_details_multi_scale_flag_term_fkey'],
        ['PDB', 'hm_modeling_protocol_details_multi_state_flag_term_fkey'], 'num_models_begin',
        'num_models_end', ['PDB', 'ihm_modeling_protocol_details_ordered_flag_term_fkey'], {
            'markdown_name': 'protocol id',
            'comment': 'A reference to table ihm_modeling_protocol.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_modeling_protocol_details_protocol_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'script file id',
            'comment': 'A reference to table ihm_external_files.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_modeling_protocol_details_script_file_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'software id',
            'comment': 'A reference to table software.pdbx_ordinal.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_modeling_protocol_details_software_id_fkey']
                }, 'RID'
            ]
        }, 'step_id', 'step_method', 'step_name', 'struct_assembly_description', {
            'markdown_name': 'struct assembly id',
            'comment': 'A reference to table ihm_struct_assembly.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_modeling_protocol_details_struct_assembly_id_fkey']
                }, 'RID'
            ]
        }, ['PDB', 'ihm_modeling_protocol_details_RCB_fkey'],
        ['PDB', 'ihm_modeling_protocol_details_RMB_fkey'], 'RCT', 'RMT',
        ['PDB', 'ihm_modeling_protocol_details_Owner_fkey']
    ]
}

table_annotations = {chaise_tags.visible_columns: visible_columns, }

table_comment = None

table_acls = {}

table_acl_bindings = {}

key_defs = [
    em.Key.define(['RID'], constraint_names=[['PDB', 'ihm_modeling_protocol_details_RIDkey1']],
                  ),
    em.Key.define(
        ['id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_modeling_protocol_details_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['ensemble_flag'],
        'Vocab',
        'ihm_modeling_protocol_details_ensemble_flag_term', ['ID'],
        constraint_names=[['PDB', 'ihm_modeling_protocol_details_ensemble_flag_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['multi_scale_flag'],
        'Vocab',
        'hm_modeling_protocol_details_multi_scale_flag_term', ['ID'],
        constraint_names=[['PDB', 'hm_modeling_protocol_details_multi_scale_flag_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['multi_state_flag'],
        'Vocab',
        'hm_modeling_protocol_details_multi_state_flag_term', ['ID'],
        constraint_names=[['PDB', 'hm_modeling_protocol_details_multi_state_flag_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['ordered_flag'],
        'Vocab',
        'ihm_modeling_protocol_details_ordered_flag_term', ['ID'],
        constraint_names=[['PDB', 'ihm_modeling_protocol_details_ordered_flag_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_modeling_protocol_details_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_modeling_protocol_details_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_modeling_protocol_details_structure_id_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['dataset_group_id', 'structure_id'],
        'PDB',
        'ihm_dataset_group', ['id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_modeling_protocol_details_dataset_group_id_fkey']],
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
        constraint_names=[['PDB', 'ihm_modeling_protocol_details_protocol_id_fkey']],
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
        constraint_names=[['PDB', 'ihm_modeling_protocol_details_script_file_id_fkey']],
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
        constraint_names=[['PDB', 'ihm_modeling_protocol_details_software_id_fkey']],
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
        constraint_names=[['PDB', 'ihm_modeling_protocol_details_struct_assembly_id_fkey']],
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
