import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_predicted_contact_restraint'

schema_name = 'PDB'

column_annotations = {
    'seq_id_2': {},
    'probability': {},
    'comp_id_2': {},
    'seq_id_1': {},
    'entity_id_2': {},
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
    'Owner': {},
    'comp_id_1': {},
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    },
    'entity_description_1': {},
    'distance_lower_limit': {},
    'distance_upper_limit': {},
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'asym_id_1': {},
    'group_id': {},
    'restraint_type': {},
    'structure_id': {},
    'rep_atom_1': {},
    'asym_id_2': {},
    'model_granularity': {},
    'software_id': {},
    'entity_id_1': {},
    'dataset_list_id': {},
    'entity_description_2': {},
    'id': {},
    'rep_atom_2': {}
}

column_comment = {
    'seq_id_2': 'A reference to table entity_poly_seq.num.',
    'probability': 'type:float4\nThe real number that indicates the probability that the predicted distance restraint \n is correct. This number should fall between 0.0 and 1.0.',
    'comp_id_2': 'A reference to table chem_comp.id.',
    'seq_id_1': 'A reference to table entity_poly_seq.num.',
    'entity_id_2': 'A reference to table entity.id.',
    'distance_upper_limit': 'type:float4\nThe upper limit to the distance threshold applied to this predicted contact restraint\n in the integrative modeling task.',
    'Owner': 'Group that can update the record.',
    'entity_description_1': 'type:text\nA text description of molecular entity 1. \n',
    'distance_lower_limit': 'type:float4\nThe lower limit to the distance threshold applied to this predicted contact restraint\n in the integrative modeling task.',
    'asym_id_1': 'A reference to table struct_asym.id.',
    'comp_id_1': 'A reference to table chem_comp.id.',
    'rep_atom_2': 'type:text\nIf _ihm_predicted_contact_restraint.model_granularity is by-residue, then indicate the atom \n used to represent the second monomer partner in three-dimension. Default is the C-alpha atom.',
    'group_id': 'type:int4\nAn identifier to group the predicted contacts.',
    'restraint_type': 'type:text\nThe type of distance restraint applied.',
    'structure_id': 'A reference to table entry.id.',
    'rep_atom_1': 'type:text\nIf _ihm_predicted_contact_restraint.model_granularity is by-residue, then indicate the atom \n used to represent the first monomer partner in three-dimension. Default is the C-alpha atom.',
    'asym_id_2': 'A reference to table struct_asym.id.',
    'model_granularity': 'type:text\nThe granularity of the predicted contact as applied to the multi-scale model.',
    'software_id': 'A reference to table software.pdbx_ordinal.',
    'entity_id_1': 'A reference to table entity.id.',
    'dataset_list_id': 'A reference to table ihm_dataset_list.id.',
    'entity_description_2': 'type:text\nA text description of molecular entity 2. \n',
    'id': 'type:int4\nA unique identifier for the predicted contact restraint.'
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
        'asym_id_1', em.builtin_types['text'], nullok=False, comment=column_comment['asym_id_1'],
    ),
    em.Column.define(
        'asym_id_2', em.builtin_types['text'], nullok=False, comment=column_comment['asym_id_2'],
    ),
    em.Column.define(
        'comp_id_1', em.builtin_types['text'], nullok=False, comment=column_comment['comp_id_1'],
    ),
    em.Column.define(
        'comp_id_2', em.builtin_types['text'], nullok=False, comment=column_comment['comp_id_2'],
    ),
    em.Column.define(
        'dataset_list_id',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['dataset_list_id'],
    ),
    em.Column.define(
        'distance_lower_limit',
        em.builtin_types['float4'],
        comment=column_comment['distance_lower_limit'],
    ),
    em.Column.define(
        'distance_upper_limit',
        em.builtin_types['float4'],
        comment=column_comment['distance_upper_limit'],
    ),
    em.Column.define(
        'entity_description_1',
        em.builtin_types['text'],
        comment=column_comment['entity_description_1'],
    ),
    em.Column.define(
        'entity_description_2',
        em.builtin_types['text'],
        comment=column_comment['entity_description_2'],
    ),
    em.Column.define(
        'entity_id_1',
        em.builtin_types['text'],
        nullok=False,
        comment=column_comment['entity_id_1'],
    ),
    em.Column.define(
        'entity_id_2',
        em.builtin_types['text'],
        nullok=False,
        comment=column_comment['entity_id_2'],
    ),
    em.Column.define('group_id', em.builtin_types['int4'], comment=column_comment['group_id'],
                     ),
    em.Column.define('id', em.builtin_types['int4'], nullok=False, comment=column_comment['id'],
                     ),
    em.Column.define(
        'model_granularity',
        em.builtin_types['text'],
        nullok=False,
        comment=column_comment['model_granularity'],
    ),
    em.Column.define(
        'probability', em.builtin_types['float4'], comment=column_comment['probability'],
    ),
    em.Column.define(
        'rep_atom_1', em.builtin_types['text'], comment=column_comment['rep_atom_1'],
    ),
    em.Column.define(
        'rep_atom_2', em.builtin_types['text'], comment=column_comment['rep_atom_2'],
    ),
    em.Column.define(
        'restraint_type',
        em.builtin_types['text'],
        nullok=False,
        comment=column_comment['restraint_type'],
    ),
    em.Column.define(
        'seq_id_1', em.builtin_types['int4'], nullok=False, comment=column_comment['seq_id_1'],
    ),
    em.Column.define(
        'seq_id_2', em.builtin_types['int4'], nullok=False, comment=column_comment['seq_id_2'],
    ),
    em.Column.define(
        'software_id', em.builtin_types['int4'], comment=column_comment['software_id'],
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
                    'outbound': ['PDB', 'ihm_predicted_contact_restraint_structure_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'asym id 1',
            'comment': 'A reference to table struct_asym.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_predicted_contact_restraint_asym_id_1_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'asym id 2',
            'comment': 'A reference to table struct_asym.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_predicted_contact_restraint_asym_id_2_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'comp id 1',
            'comment': 'A reference to table chem_comp.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_predicted_contact_restraint_comp_id_1_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'comp id 2',
            'comment': 'A reference to table chem_comp.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_predicted_contact_restraint_comp_id_2_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'dataset list id',
            'comment': 'A reference to table ihm_dataset_list.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_predicted_contact_restraint_dataset_list_id_fkey']
                }, 'RID'
            ]
        }, 'distance_lower_limit', 'distance_upper_limit', 'entity_description_1',
        'entity_description_2', {
            'markdown_name': 'entity id 1',
            'comment': 'A reference to table entity.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_predicted_contact_restraint_entity_id_1_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'entity id 2',
            'comment': 'A reference to table entity.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_predicted_contact_restraint_entity_id_2_fkey']
                }, 'RID'
            ]
        }, 'group_id', 'id', ['PDB', 'predicted_contact_restraint_model_granularity_term_fkey'],
        'probability', ['PDB', 'ihm_predicted_contact_restraint_rep_atom_1_term_fkey'],
        ['PDB', 'ihm_predicted_contact_restraint_rep_atom_2_term_fkey'],
        ['PDB', 'hm_predicted_contact_restraint_restraint_type_term_fkey'], {
            'markdown_name': 'seq id 1',
            'comment': 'A reference to table entity_poly_seq.num.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_predicted_contact_restraint_seq_id_1_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'seq id 2',
            'comment': 'A reference to table entity_poly_seq.num.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_predicted_contact_restraint_seq_id_2_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'software id',
            'comment': 'A reference to table software.pdbx_ordinal.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_predicted_contact_restraint_software_id_fkey']
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
                    'outbound': ['PDB', 'ihm_predicted_contact_restraint_structure_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'asym id 1',
            'comment': 'A reference to table struct_asym.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_predicted_contact_restraint_asym_id_1_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'asym id 2',
            'comment': 'A reference to table struct_asym.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_predicted_contact_restraint_asym_id_2_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'comp id 1',
            'comment': 'A reference to table chem_comp.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_predicted_contact_restraint_comp_id_1_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'comp id 2',
            'comment': 'A reference to table chem_comp.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_predicted_contact_restraint_comp_id_2_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'dataset list id',
            'comment': 'A reference to table ihm_dataset_list.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_predicted_contact_restraint_dataset_list_id_fkey']
                }, 'RID'
            ]
        }, 'distance_lower_limit', 'distance_upper_limit', 'entity_description_1',
        'entity_description_2', {
            'markdown_name': 'entity id 1',
            'comment': 'A reference to table entity.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_predicted_contact_restraint_entity_id_1_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'entity id 2',
            'comment': 'A reference to table entity.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_predicted_contact_restraint_entity_id_2_fkey']
                }, 'RID'
            ]
        }, 'group_id', 'id', ['PDB', 'predicted_contact_restraint_model_granularity_term_fkey'],
        'probability', ['PDB', 'ihm_predicted_contact_restraint_rep_atom_1_term_fkey'],
        ['PDB', 'ihm_predicted_contact_restraint_rep_atom_2_term_fkey'],
        ['PDB', 'hm_predicted_contact_restraint_restraint_type_term_fkey'], {
            'markdown_name': 'seq id 1',
            'comment': 'A reference to table entity_poly_seq.num.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_predicted_contact_restraint_seq_id_1_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'seq id 2',
            'comment': 'A reference to table entity_poly_seq.num.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_predicted_contact_restraint_seq_id_2_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'software id',
            'comment': 'A reference to table software.pdbx_ordinal.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_predicted_contact_restraint_software_id_fkey']
                }, 'RID'
            ]
        }, ['PDB', 'ihm_predicted_contact_restraint_RCB_fkey'],
        ['PDB', 'ihm_predicted_contact_restraint_RMB_fkey'], 'RCT', 'RMT',
        ['PDB', 'ihm_predicted_contact_restraint_Owner_fkey']
    ]
}

table_annotations = {chaise_tags.visible_columns: visible_columns, }

table_comment = None

table_acls = {}

table_acl_bindings = {}

key_defs = [
    em.Key.define(['RID'], constraint_names=[['PDB', 'ihm_predicted_contact_restraint_RIDkey1']],
                  ),
    em.Key.define(
        ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_predicted_contact_restraint_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['model_granularity'],
        'Vocab',
        'predicted_contact_restraint_model_granularity_term', ['ID'],
        constraint_names=[['PDB', 'predicted_contact_restraint_model_granularity_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['rep_atom_1'],
        'Vocab',
        'ihm_predicted_contact_restraint_rep_atom_1_term', ['ID'],
        constraint_names=[['PDB', 'ihm_predicted_contact_restraint_rep_atom_1_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['rep_atom_2'],
        'Vocab',
        'ihm_predicted_contact_restraint_rep_atom_2_term', ['ID'],
        constraint_names=[['PDB', 'ihm_predicted_contact_restraint_rep_atom_2_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['restraint_type'],
        'Vocab',
        'hm_predicted_contact_restraint_restraint_type_term', ['ID'],
        constraint_names=[['PDB', 'hm_predicted_contact_restraint_restraint_type_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_predicted_contact_restraint_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_predicted_contact_restraint_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_predicted_contact_restraint_structure_id_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'asym_id_1'],
        'PDB',
        'struct_asym', ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_predicted_contact_restraint_asym_id_1_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'asym_id_2'],
        'PDB',
        'struct_asym', ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_predicted_contact_restraint_asym_id_2_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['dataset_list_id', 'structure_id'],
        'PDB',
        'ihm_dataset_list', ['id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_predicted_contact_restraint_dataset_list_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['software_id', 'structure_id'],
        'PDB',
        'software', ['pdbx_ordinal', 'structure_id'],
        constraint_names=[['PDB', 'ihm_predicted_contact_restraint_software_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'entity_id_1', 'comp_id_1', 'seq_id_1'],
        'PDB',
        'entity_poly_seq', ['structure_id', 'entity_id', 'mon_id', 'num'],
        constraint_names=[['PDB', 'ihm_predicted_contact_restraint_mm_poly_res_label_1_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'seq_id_2', 'comp_id_2', 'entity_id_2'],
        'PDB',
        'entity_poly_seq', ['structure_id', 'num', 'mon_id', 'entity_id'],
        constraint_names=[['PDB', 'ihm_predicted_contact_restraint_mm_poly_res_label_2_fkey']],
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
