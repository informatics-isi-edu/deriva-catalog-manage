import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_poly_residue_feature'

schema_name = 'PDB'

column_annotations = {
    'seq_id_begin': {},
    'residue_range_granularity': {},
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'rep_atom': {},
    'Owner': {},
    'feature_id': {},
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
    'interface_residue_flag': {},
    'ordinal_id': {},
    'asym_id': {},
    'entity_id': {},
    'comp_id_begin': {},
    'comp_id_end': {}
}

column_comment = {
    'seq_id_begin': 'A reference to table entity_poly_seq.num.',
    'seq_id_end': 'A reference to table entity_poly_seq.num.',
    'rep_atom': 'type:text\nIf _ihm_poly_residue_feature.granularity is by-residue, then indicate the atom used to represent \n the residue in three-dimension. Default is the C-alpha atom.',
    'Owner': 'Group that can update the record.',
    'feature_id': 'A reference to table ihm_feature_list.feature_id.',
    'residue_range_granularity': 'type:text\nThe coarse-graining information, if the feature is a residue range.',
    'structure_id': 'A reference to table entry.id.',
    'interface_residue_flag': 'type:text\nA flag to indicate if the feature is an interface residue, identified by experiments and\n therefore, used to build spatial restraints during modeling.',
    'ordinal_id': 'type:int4\nA unique identifier for the category.',
    'asym_id': 'A reference to table struct_asym.id.',
    'entity_id': 'A reference to table entity.id.',
    'comp_id_begin': 'A reference to table chem_comp.id.',
    'comp_id_end': 'A reference to table chem_comp.id.'
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
    em.Column.define('asym_id', em.builtin_types['text'], comment=column_comment['asym_id'],
                     ),
    em.Column.define(
        'comp_id_begin', em.builtin_types['text'], comment=column_comment['comp_id_begin'],
    ),
    em.Column.define(
        'comp_id_end', em.builtin_types['text'], comment=column_comment['comp_id_end'],
    ),
    em.Column.define(
        'entity_id', em.builtin_types['text'], nullok=False, comment=column_comment['entity_id'],
    ),
    em.Column.define(
        'feature_id',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['feature_id'],
    ),
    em.Column.define(
        'interface_residue_flag',
        em.builtin_types['text'],
        comment=column_comment['interface_residue_flag'],
    ),
    em.Column.define(
        'ordinal_id',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['ordinal_id'],
    ),
    em.Column.define('rep_atom', em.builtin_types['text'], comment=column_comment['rep_atom'],
                     ),
    em.Column.define(
        'residue_range_granularity',
        em.builtin_types['text'],
        comment=column_comment['residue_range_granularity'],
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
                'outbound': ['PDB', 'ihm_poly_residue_feature_structure_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'asym id',
            'comment': 'A reference to table struct_asym.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_poly_residue_feature_asym_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'comp id begin',
            'comment': 'A reference to table chem_comp.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_poly_residue_feature_comp_id_begin_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'comp id end',
            'comment': 'A reference to table chem_comp.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_poly_residue_feature_comp_id_end_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'entity id',
            'comment': 'A reference to table entity.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_poly_residue_feature_entity_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'feature id',
            'comment': 'A reference to table ihm_feature_list.feature_id.',
            'source': [{
                'outbound': ['PDB', 'ihm_poly_residue_feature_feature_id_fkey']
            }, 'RID']
        }, ['PDB', 'm_poly_residue_feature_interface_residue_flag_term_fkey'], 'ordinal_id',
        ['PDB', 'ihm_poly_residue_feature_rep_atom_term_fkey'],
        ['PDB', 'oly_residue_feature_residue_range_granularity_term_fkey'], {
            'markdown_name': 'seq id begin',
            'comment': 'A reference to table entity_poly_seq.num.',
            'source': [{
                'outbound': ['PDB', 'ihm_poly_residue_feature_seq_id_begin_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'seq id end',
            'comment': 'A reference to table entity_poly_seq.num.',
            'source': [{
                'outbound': ['PDB', 'ihm_poly_residue_feature_seq_id_end_fkey']
            }, 'RID']
        }
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_poly_residue_feature_structure_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'asym id',
            'comment': 'A reference to table struct_asym.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_poly_residue_feature_asym_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'comp id begin',
            'comment': 'A reference to table chem_comp.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_poly_residue_feature_comp_id_begin_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'comp id end',
            'comment': 'A reference to table chem_comp.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_poly_residue_feature_comp_id_end_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'entity id',
            'comment': 'A reference to table entity.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_poly_residue_feature_entity_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'feature id',
            'comment': 'A reference to table ihm_feature_list.feature_id.',
            'source': [{
                'outbound': ['PDB', 'ihm_poly_residue_feature_feature_id_fkey']
            }, 'RID']
        }, ['PDB', 'm_poly_residue_feature_interface_residue_flag_term_fkey'], 'ordinal_id',
        ['PDB', 'ihm_poly_residue_feature_rep_atom_term_fkey'],
        ['PDB', 'oly_residue_feature_residue_range_granularity_term_fkey'], {
            'markdown_name': 'seq id begin',
            'comment': 'A reference to table entity_poly_seq.num.',
            'source': [{
                'outbound': ['PDB', 'ihm_poly_residue_feature_seq_id_begin_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'seq id end',
            'comment': 'A reference to table entity_poly_seq.num.',
            'source': [{
                'outbound': ['PDB', 'ihm_poly_residue_feature_seq_id_end_fkey']
            }, 'RID']
        }, ['PDB', 'ihm_poly_residue_feature_RCB_fkey'],
        ['PDB', 'ihm_poly_residue_feature_RMB_fkey'], 'RCT', 'RMT',
        ['PDB', 'ihm_poly_residue_feature_Owner_fkey']
    ]
}

table_annotations = {chaise_tags.visible_columns: visible_columns, }

table_comment = None

table_acls = {}

table_acl_bindings = {}

key_defs = [
    em.Key.define(['RID'], constraint_names=[['PDB', 'ihm_poly_residue_feature_RIDkey1']],
                  ),
    em.Key.define(
        ['ordinal_id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_poly_residue_feature_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_poly_residue_feature_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_poly_residue_feature_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['interface_residue_flag'],
        'Vocab',
        'm_poly_residue_feature_interface_residue_flag_term', ['ID'],
        constraint_names=[['PDB', 'm_poly_residue_feature_interface_residue_flag_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['rep_atom'],
        'Vocab',
        'ihm_poly_residue_feature_rep_atom_term', ['ID'],
        constraint_names=[['PDB', 'ihm_poly_residue_feature_rep_atom_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['residue_range_granularity'],
        'Vocab',
        'oly_residue_feature_residue_range_granularity_term', ['ID'],
        constraint_names=[['PDB', 'oly_residue_feature_residue_range_granularity_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_poly_residue_feature_structure_id_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['asym_id', 'structure_id'],
        'PDB',
        'struct_asym', ['id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_poly_residue_feature_asym_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'feature_id'],
        'PDB',
        'ihm_feature_list', ['structure_id', 'feature_id'],
        constraint_names=[['PDB', 'ihm_poly_residue_feature_feature_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['entity_id', 'comp_id_begin', 'structure_id', 'seq_id_begin'],
        'PDB',
        'entity_poly_seq', ['entity_id', 'mon_id', 'structure_id', 'num'],
        constraint_names=[['PDB', 'ihm_poly_residue_feature_mm_poly_res_label_begin_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['entity_id', 'comp_id_end', 'seq_id_end', 'structure_id'],
        'PDB',
        'entity_poly_seq', ['entity_id', 'mon_id', 'num', 'structure_id'],
        constraint_names=[['PDB', 'ihm_poly_residue_feature_mm_poly_res_label_end_fkey']],
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
