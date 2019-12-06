import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'entity_poly_seq'

schema_name = 'PDB'

column_annotations = {
    'num': {},
    'Owner': {},
    'hetero': {},
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'mon_id': {},
    'entity_id': {},
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    },
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
    'structure_id': {}
}

column_comment = {
    'num': 'type:int4\nThe value of _entity_poly_seq.num must uniquely and sequentially\n identify a record in the ENTITY_POLY_SEQ list.\n\n Note that this item must be a number and that the sequence\n numbers must progress in increasing numerical order.',
    'Owner': 'Group that can update the record.',
    'hetero': 'type:text\nA flag to indicate whether this monomer in the polymer is\n heterogeneous in sequence.',
    'entity_id': 'A reference to table entity.id.',
    'mon_id': 'A reference to table chem_comp.id.',
    'structure_id': 'A reference to table entry.id.'
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
        'entity_id', em.builtin_types['text'], nullok=False, comment=column_comment['entity_id'],
    ),
    em.Column.define('hetero', em.builtin_types['text'], comment=column_comment['hetero'],
                     ),
    em.Column.define(
        'mon_id', em.builtin_types['text'], nullok=False, comment=column_comment['mon_id'],
    ),
    em.Column.define(
        'num', em.builtin_types['int4'], nullok=False, comment=column_comment['num'],
    ),
    em.Column.define('Owner', em.builtin_types['text'], comment=column_comment['Owner'],
                     ),
]

visible_foreign_keys = {
    'detailed': [
        ['PDB', 'ihm_entity_poly_segment_mm_poly_res_label_begin_fkey'],
        ['PDB', 'ihm_entity_poly_segment_mm_poly_res_label_end_fkey'],
        ['PDB', 'ihm_starting_model_seq_dif_mm_poly_res_label_fkey'],
        ['PDB', 'ihm_poly_probe_position_mm_poly_res_label_fkey'],
        ['PDB', 'ihm_cross_link_list_mm_poly_res_label_1_fkey'],
        ['PDB', 'ihm_cross_link_list_mm_poly_res_label_2_fkey'],
        ['PDB', 'ihm_cross_link_restraint_mm_poly_res_label_1_fkey'],
        ['PDB', 'ihm_cross_link_restraint_mm_poly_res_label_2_fkey'],
        ['PDB', 'ihm_hydroxyl_radical_fp_restraint_mm_poly_res_label_fkey'],
        ['PDB', 'ihm_predicted_contact_restraint_mm_poly_res_label_1_fkey'],
        ['PDB', 'ihm_predicted_contact_restraint_mm_poly_res_label_2_fkey'],
        ['PDB', 'ihm_poly_atom_feature_mm_poly_res_label_fkey'],
        ['PDB', 'ihm_poly_residue_feature_mm_poly_res_label_begin_fkey'],
        ['PDB', 'ihm_poly_residue_feature_mm_poly_res_label_end_fkey'],
        ['PDB', 'ihm_residues_not_modeled_mm_poly_res_label_begin_fkey'],
        ['PDB', 'ihm_residues_not_modeled_mm_poly_res_label_end_fkey']
    ],
    'filter': 'detailed'
}

visible_columns = {
    'entry': [
        {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'entity_poly_seq_structure_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'entity id',
            'comment': 'A reference to table entity.id.',
            'source': [{
                'outbound': ['PDB', 'entity_poly_seq_entity_id_fkey']
            }, 'RID']
        }, ['PDB', 'entity_poly_seq_hetero_term_fkey'], {
            'markdown_name': 'mon id',
            'comment': 'A reference to table chem_comp.id.',
            'source': [{
                'outbound': ['PDB', 'entity_poly_seq_mon_id_fkey']
            }, 'RID']
        }, 'num'
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'entity_poly_seq_structure_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'entity id',
            'comment': 'A reference to table entity.id.',
            'source': [{
                'outbound': ['PDB', 'entity_poly_seq_entity_id_fkey']
            }, 'RID']
        }, ['PDB', 'entity_poly_seq_hetero_term_fkey'], {
            'markdown_name': 'mon id',
            'comment': 'A reference to table chem_comp.id.',
            'source': [{
                'outbound': ['PDB', 'entity_poly_seq_mon_id_fkey']
            }, 'RID']
        }, 'num', ['PDB', 'entity_poly_seq_RCB_fkey'], ['PDB', 'entity_poly_seq_RMB_fkey'], 'RCT',
        'RMT', ['PDB', 'entity_poly_seq_Owner_fkey']
    ]
}

table_annotations = {
    chaise_tags.visible_columns: visible_columns,
    chaise_tags.visible_foreign_keys: visible_foreign_keys,
}

table_comment = None

table_acls = {}

table_acl_bindings = {}

key_defs = [
    em.Key.define(
        ['entity_id', 'num', 'structure_id', 'mon_id'],
        constraint_names=[['PDB', 'entity_poly_seq_primary_key']],
    ),
    em.Key.define(['RID'], constraint_names=[['PDB', 'entity_poly_seq_RIDkey1']],
                  ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['hetero'],
        'Vocab',
        'entity_poly_seq_hetero_term', ['ID'],
        constraint_names=[['PDB', 'entity_poly_seq_hetero_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'entity_poly_seq_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'entity_poly_seq_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'entity_poly_seq_structure_id_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'entity_id'],
        'PDB',
        'entity_poly', ['structure_id', 'entity_id'],
        constraint_names=[['PDB', 'entity_poly_seq_entity_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'mon_id'],
        'PDB',
        'chem_comp', ['structure_id', 'id'],
        constraint_names=[['PDB', 'entity_poly_seq_mon_id_fkey']],
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
