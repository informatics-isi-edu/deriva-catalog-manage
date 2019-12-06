import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_residues_not_modeled'

schema_name = 'PDB'

column_annotations = {
    'seq_id_begin': {},
    'entity_description': {},
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'reason': {},
    'seq_id_end': {},
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
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    },
    'structure_id': {},
    'model_id': {},
    'Owner': {},
    'details': {},
    'asym_id': {},
    'entity_id': {},
    'comp_id_begin': {},
    'comp_id_end': {},
    'id': {}
}

column_comment = {
    'seq_id_begin': 'A reference to table entity_poly_seq.num.',
    'seq_id_end': 'A reference to table entity_poly_seq.num.',
    'reason': 'type:text\nThe reason why the residues are missing in the structural model.',
    'entity_description': 'type:text\nA text description of the molecular entity, whose residues are not modeled. \n This data item is a pointer to _entity.pdbx_description in the ENTITY category.',
    'structure_id': 'A reference to table entry.id.',
    'model_id': 'A reference to table ihm_model_list.model_id.',
    'Owner': 'Group that can update the record.',
    'details': 'type:text\nAdditional details regarding the missing segments.',
    'asym_id': 'A reference to table struct_asym.id.',
    'entity_id': 'A reference to table entity.id.',
    'comp_id_begin': 'A reference to table chem_comp.id.',
    'comp_id_end': 'A reference to table chem_comp.id.',
    'id': 'type:int4\nA unique identifier for the category.'
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
        'asym_id', em.builtin_types['text'], nullok=False, comment=column_comment['asym_id'],
    ),
    em.Column.define(
        'comp_id_begin',
        em.builtin_types['text'],
        nullok=False,
        comment=column_comment['comp_id_begin'],
    ),
    em.Column.define(
        'comp_id_end',
        em.builtin_types['text'],
        nullok=False,
        comment=column_comment['comp_id_end'],
    ),
    em.Column.define('details', em.builtin_types['text'], comment=column_comment['details'],
                     ),
    em.Column.define(
        'entity_description',
        em.builtin_types['text'],
        comment=column_comment['entity_description'],
    ),
    em.Column.define(
        'entity_id', em.builtin_types['text'], nullok=False, comment=column_comment['entity_id'],
    ),
    em.Column.define('id', em.builtin_types['int4'], nullok=False, comment=column_comment['id'],
                     ),
    em.Column.define(
        'model_id', em.builtin_types['int4'], nullok=False, comment=column_comment['model_id'],
    ),
    em.Column.define('reason', em.builtin_types['text'], comment=column_comment['reason'],
                     ),
    em.Column.define(
        'seq_id_begin',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['seq_id_begin'],
    ),
    em.Column.define(
        'seq_id_end',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['seq_id_end'],
    ),
    em.Column.define('Owner', em.builtin_types['text'], comment=column_comment['Owner'],
                     ),
]

visible_columns = {
    'entry': [
        {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_residues_not_modeled_structure_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'asym id',
            'comment': 'A reference to table struct_asym.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_residues_not_modeled_asym_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'comp id begin',
            'comment': 'A reference to table chem_comp.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_residues_not_modeled_comp_id_begin_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'comp id end',
            'comment': 'A reference to table chem_comp.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_residues_not_modeled_comp_id_end_fkey']
            }, 'RID']
        }, 'details', 'entity_description', {
            'markdown_name': 'entity id',
            'comment': 'A reference to table entity.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_residues_not_modeled_entity_id_fkey']
            }, 'RID']
        }, 'id', {
            'markdown_name': 'model id',
            'comment': 'A reference to table ihm_model_list.model_id.',
            'source': [{
                'outbound': ['PDB', 'ihm_residues_not_modeled_model_id_fkey']
            }, 'RID']
        }, ['PDB', 'ihm_residues_not_modeled_reason_term_fkey'], {
            'markdown_name': 'seq id begin',
            'comment': 'A reference to table entity_poly_seq.num.',
            'source': [{
                'outbound': ['PDB', 'ihm_residues_not_modeled_seq_id_begin_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'seq id end',
            'comment': 'A reference to table entity_poly_seq.num.',
            'source': [{
                'outbound': ['PDB', 'ihm_residues_not_modeled_seq_id_end_fkey']
            }, 'RID']
        }
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_residues_not_modeled_structure_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'asym id',
            'comment': 'A reference to table struct_asym.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_residues_not_modeled_asym_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'comp id begin',
            'comment': 'A reference to table chem_comp.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_residues_not_modeled_comp_id_begin_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'comp id end',
            'comment': 'A reference to table chem_comp.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_residues_not_modeled_comp_id_end_fkey']
            }, 'RID']
        }, 'details', 'entity_description', {
            'markdown_name': 'entity id',
            'comment': 'A reference to table entity.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_residues_not_modeled_entity_id_fkey']
            }, 'RID']
        }, 'id', {
            'markdown_name': 'model id',
            'comment': 'A reference to table ihm_model_list.model_id.',
            'source': [{
                'outbound': ['PDB', 'ihm_residues_not_modeled_model_id_fkey']
            }, 'RID']
        }, ['PDB', 'ihm_residues_not_modeled_reason_term_fkey'], {
            'markdown_name': 'seq id begin',
            'comment': 'A reference to table entity_poly_seq.num.',
            'source': [{
                'outbound': ['PDB', 'ihm_residues_not_modeled_seq_id_begin_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'seq id end',
            'comment': 'A reference to table entity_poly_seq.num.',
            'source': [{
                'outbound': ['PDB', 'ihm_residues_not_modeled_seq_id_end_fkey']
            }, 'RID']
        }, ['PDB', 'ihm_residues_not_modeled_RCB_fkey'],
        ['PDB', 'ihm_residues_not_modeled_RMB_fkey'], 'RCT', 'RMT',
        ['PDB', 'ihm_residues_not_modeled_Owner_fkey']
    ]
}

table_annotations = {chaise_tags.visible_columns: visible_columns, }

table_comment = None

table_acls = {}

table_acl_bindings = {}

key_defs = [
    em.Key.define(['RID'], constraint_names=[['PDB', 'ihm_residues_not_modeled_RIDkey1']],
                  ),
    em.Key.define(
        ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_residues_not_modeled_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['reason'],
        'Vocab',
        'ihm_residues_not_modeled_reason_term', ['ID'],
        constraint_names=[['PDB', 'ihm_residues_not_modeled_reason_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_residues_not_modeled_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_residues_not_modeled_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_residues_not_modeled_structure_id_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['asym_id', 'structure_id'],
        'PDB',
        'struct_asym', ['id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_residues_not_modeled_asym_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'model_id'],
        'PDB',
        'ihm_model_list', ['structure_id', 'model_id'],
        constraint_names=[['PDB', 'ihm_residues_not_modeled_model_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['seq_id_begin', 'structure_id', 'comp_id_begin', 'entity_id'],
        'PDB',
        'entity_poly_seq', ['num', 'structure_id', 'mon_id', 'entity_id'],
        constraint_names=[['PDB', 'ihm_residues_not_modeled_mm_poly_res_label_begin_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'seq_id_end', 'comp_id_end', 'entity_id'],
        'PDB',
        'entity_poly_seq', ['structure_id', 'num', 'mon_id', 'entity_id'],
        constraint_names=[['PDB', 'ihm_residues_not_modeled_mm_poly_res_label_end_fkey']],
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
