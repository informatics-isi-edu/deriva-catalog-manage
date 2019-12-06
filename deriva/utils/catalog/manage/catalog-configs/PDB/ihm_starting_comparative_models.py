import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_starting_comparative_models'

schema_name = 'PDB'

column_annotations = {
    'Owner': {},
    'template_sequence_identity': {},
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'template_dataset_list_id': {},
    'template_seq_id_end': {},
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
    'alignment_file_id': {},
    'structure_id': {},
    'template_sequence_identity_denominator': {},
    'starting_model_auth_asym_id': {},
    'template_seq_id_begin': {},
    'starting_model_seq_id_begin': {},
    'details': {},
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    },
    'starting_model_seq_id_end': {},
    'template_auth_asym_id': {},
    'id': {},
    'starting_model_id': {}
}

column_comment = {
    'starting_model_auth_asym_id': 'type:text\nThe chainId/auth_asym_id corresponding to the starting model.',
    'template_sequence_identity': 'type:float4\nThe percentage sequence identity between the template sequence and the comparative model sequence.',
    'details': 'type:text\nAdditional details regarding the starting comparative models.',
    'template_sequence_identity_denominator': 'type:int4\nThe denominator used while calculating the sequence identity provided in \n _ihm_starting_comparative_models.template_sequence_identity.',
    'template_seq_id_end': 'type:int4\nThe ending residue index of the template.',
    'structure_id': 'A reference to table entry.id.',
    'alignment_file_id': 'A reference to table ihm_external_files.id.',
    'starting_model_seq_id_end': 'type:int4\nThe ending residue index of the starting model.',
    'Owner': 'Group that can update the record.',
    'template_auth_asym_id': 'type:text\nThe chainId/auth_asym_id corresponding to the template.',
    'template_seq_id_begin': 'type:int4\nThe starting residue index of the template.',
    'starting_model_seq_id_begin': 'type:int4\nThe starting residue index of the starting model.',
    'template_dataset_list_id': 'A reference to table ihm_dataset_list.id.',
    'id': 'type:int4\nA unique identifier for the starting comparative model.',
    'starting_model_id': 'A reference to table ihm_starting_model_details.starting_model_id.'
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
        'alignment_file_id',
        em.builtin_types['int4'],
        comment=column_comment['alignment_file_id'],
    ),
    em.Column.define('details', em.builtin_types['text'], comment=column_comment['details'],
                     ),
    em.Column.define('id', em.builtin_types['int4'], nullok=False, comment=column_comment['id'],
                     ),
    em.Column.define(
        'starting_model_auth_asym_id',
        em.builtin_types['text'],
        nullok=False,
        comment=column_comment['starting_model_auth_asym_id'],
    ),
    em.Column.define(
        'starting_model_id',
        em.builtin_types['text'],
        nullok=False,
        comment=column_comment['starting_model_id'],
    ),
    em.Column.define(
        'starting_model_seq_id_begin',
        em.builtin_types['int4'],
        comment=column_comment['starting_model_seq_id_begin'],
    ),
    em.Column.define(
        'starting_model_seq_id_end',
        em.builtin_types['int4'],
        comment=column_comment['starting_model_seq_id_end'],
    ),
    em.Column.define(
        'template_auth_asym_id',
        em.builtin_types['text'],
        nullok=False,
        comment=column_comment['template_auth_asym_id'],
    ),
    em.Column.define(
        'template_dataset_list_id',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['template_dataset_list_id'],
    ),
    em.Column.define(
        'template_seq_id_begin',
        em.builtin_types['int4'],
        comment=column_comment['template_seq_id_begin'],
    ),
    em.Column.define(
        'template_seq_id_end',
        em.builtin_types['int4'],
        comment=column_comment['template_seq_id_end'],
    ),
    em.Column.define(
        'template_sequence_identity',
        em.builtin_types['float4'],
        comment=column_comment['template_sequence_identity'],
    ),
    em.Column.define(
        'template_sequence_identity_denominator',
        em.builtin_types['int4'],
        comment=column_comment['template_sequence_identity_denominator'],
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
                    'outbound': ['PDB', 'ihm_starting_comparative_models_structure_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'alignment file id',
            'comment': 'A reference to table ihm_external_files.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_starting_comparative_models_alignment_file_id_fkey']
                }, 'RID'
            ]
        }, 'details', 'id', 'starting_model_auth_asym_id', {
            'markdown_name': 'starting model id',
            'comment': 'A reference to table ihm_starting_model_details.starting_model_id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_starting_comparative_models_starting_model_id_fkey']
                }, 'RID'
            ]
        }, 'starting_model_seq_id_begin', 'starting_model_seq_id_end', 'template_auth_asym_id', {
            'markdown_name': 'template dataset list id',
            'comment': 'A reference to table ihm_dataset_list.id.',
            'source': [
                {
                    'outbound': [
                        'PDB', 'ihm_starting_comparative_models_template_dataset_list_id_fkey'
                    ]
                }, 'RID'
            ]
        }, 'template_seq_id_begin', 'template_seq_id_end', 'template_sequence_identity',
        ['PDB', 'models_template_sequence_identity_denominator_term_fkey']
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_starting_comparative_models_structure_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'alignment file id',
            'comment': 'A reference to table ihm_external_files.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_starting_comparative_models_alignment_file_id_fkey']
                }, 'RID'
            ]
        }, 'details', 'id', 'starting_model_auth_asym_id', {
            'markdown_name': 'starting model id',
            'comment': 'A reference to table ihm_starting_model_details.starting_model_id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_starting_comparative_models_starting_model_id_fkey']
                }, 'RID'
            ]
        }, 'starting_model_seq_id_begin', 'starting_model_seq_id_end', 'template_auth_asym_id', {
            'markdown_name': 'template dataset list id',
            'comment': 'A reference to table ihm_dataset_list.id.',
            'source': [
                {
                    'outbound': [
                        'PDB', 'ihm_starting_comparative_models_template_dataset_list_id_fkey'
                    ]
                }, 'RID'
            ]
        }, 'template_seq_id_begin', 'template_seq_id_end', 'template_sequence_identity',
        ['PDB', 'models_template_sequence_identity_denominator_term_fkey'],
        ['PDB', 'ihm_starting_comparative_models_RCB_fkey'],
        ['PDB', 'ihm_starting_comparative_models_RMB_fkey'], 'RCT', 'RMT',
        ['PDB', 'ihm_starting_comparative_models_Owner_fkey']
    ]
}

table_annotations = {chaise_tags.visible_columns: visible_columns, }

table_comment = None

table_acls = {}

table_acl_bindings = {}

key_defs = [
    em.Key.define(['RID'], constraint_names=[['PDB', 'ihm_starting_comparative_models_RIDkey1']],
                  ),
    em.Key.define(
        ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_starting_comparative_models_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_starting_comparative_models_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_starting_comparative_models_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_starting_comparative_models_structure_id_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['alignment_file_id', 'structure_id'],
        'PDB',
        'ihm_external_files', ['id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_starting_comparative_models_alignment_file_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'starting_model_id'],
        'PDB',
        'ihm_starting_model_details', ['structure_id', 'starting_model_id'],
        constraint_names=[['PDB', 'ihm_starting_comparative_models_starting_model_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['template_dataset_list_id', 'structure_id'],
        'PDB',
        'ihm_dataset_list', ['id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_starting_comparative_models_template_dataset_list_id_fkey']],
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
