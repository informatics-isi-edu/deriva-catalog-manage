import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_cross_link_restraint'

schema_name = 'PDB'

column_annotations = {
    'distance_threshold': {},
    'seq_id_2': {},
    'conditional_crosslink_flag': {},
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
    'psi': {},
    'sigma_2': {},
    'comp_id_1': {},
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    },
    'atom_id_1': {},
    'Owner': {},
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
    'sigma_1': {},
    'asym_id_2': {},
    'atom_id_2': {},
    'entity_id_1': {},
    'model_granularity': {},
    'id': {}
}

column_comment = {
    'distance_threshold': 'type:float4\nThe distance threshold applied to this crosslink in the integrative modeling task.',
    'seq_id_2': 'A reference to table entity_poly_seq.num.',
    'Owner': 'Group that can update the record.',
    'conditional_crosslink_flag': 'type:text\nThe cross link conditionality.',
    'comp_id_1': 'A reference to table chem_comp.id.',
    'asym_id_1': 'A reference to table struct_asym.id.',
    'comp_id_2': 'A reference to table chem_comp.id.',
    'group_id': 'A reference to table ihm_cross_link_list.id.',
    'entity_id_2': 'A reference to table entity.id.',
    'restraint_type': 'type:text\nThe type of the cross link restraint applied.',
    'seq_id_1': 'A reference to table entity_poly_seq.num.',
    'structure_id': 'A reference to table entry.id.',
    'psi': 'type:float4\nThe uncertainty in the crosslinking experimental data;\n may be approximated to the false positive rate.',
    'sigma_2': 'type:float4\nThe uncertainty in the position of residue 2 in the crosslink\n arising due to the multi-scale nature of the model represention.',
    'sigma_1': 'type:float4\nThe uncertainty in the position of residue 1 in the crosslink\n arising due to the multi-scale nature of the model represention.',
    'asym_id_2': 'A reference to table struct_asym.id.',
    'atom_id_2': 'type:text\nThe atom identifier for the second monomer partner in the cross link.\n This data item is a pointer to _chem_comp_atom.atom_id in the \n CHEM_COMP_ATOM category.',
    'entity_id_1': 'A reference to table entity.id.',
    'model_granularity': 'type:text\nThe coarse-graining information for the crosslink implementation.',
    'id': 'type:int4\nA unique identifier for the cross link record.',
    'atom_id_1': 'type:text\nThe atom identifier for the first monomer partner in the cross link.\n This data item is a pointer to _chem_comp_atom.atom_id in the \n CHEM_COMP_ATOM category.'
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
    em.Column.define('atom_id_1', em.builtin_types['text'], comment=column_comment['atom_id_1'],
                     ),
    em.Column.define('atom_id_2', em.builtin_types['text'], comment=column_comment['atom_id_2'],
                     ),
    em.Column.define(
        'comp_id_1', em.builtin_types['text'], nullok=False, comment=column_comment['comp_id_1'],
    ),
    em.Column.define(
        'comp_id_2', em.builtin_types['text'], nullok=False, comment=column_comment['comp_id_2'],
    ),
    em.Column.define(
        'conditional_crosslink_flag',
        em.builtin_types['text'],
        comment=column_comment['conditional_crosslink_flag'],
    ),
    em.Column.define(
        'distance_threshold',
        em.builtin_types['float4'],
        nullok=False,
        comment=column_comment['distance_threshold'],
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
    em.Column.define(
        'group_id', em.builtin_types['int4'], nullok=False, comment=column_comment['group_id'],
    ),
    em.Column.define('id', em.builtin_types['int4'], nullok=False, comment=column_comment['id'],
                     ),
    em.Column.define(
        'model_granularity',
        em.builtin_types['text'],
        nullok=False,
        comment=column_comment['model_granularity'],
    ),
    em.Column.define('psi', em.builtin_types['float4'], comment=column_comment['psi'],
                     ),
    em.Column.define(
        'restraint_type', em.builtin_types['text'], comment=column_comment['restraint_type'],
    ),
    em.Column.define(
        'seq_id_1', em.builtin_types['int4'], nullok=False, comment=column_comment['seq_id_1'],
    ),
    em.Column.define(
        'seq_id_2', em.builtin_types['int4'], nullok=False, comment=column_comment['seq_id_2'],
    ),
    em.Column.define('sigma_1', em.builtin_types['float4'], comment=column_comment['sigma_1'],
                     ),
    em.Column.define('sigma_2', em.builtin_types['float4'], comment=column_comment['sigma_2'],
                     ),
    em.Column.define('Owner', em.builtin_types['text'], comment=column_comment['Owner'],
                     ),
]

table_display = {'row_name': {'row_markdown_pattern': '{{{id}}}'}}

visible_foreign_keys = {
    'detailed': [
        ['PDB', 'ihm_cross_link_result_restraint_id_fkey'],
        ['PDB', 'ihm_cross_link_result_parameters_restraint_id_fkey']
    ],
    'filter': 'detailed'
}

visible_columns = {
    '*': [
        {
            'source': 'RID'
        }, {
            'source': [{
                'outbound': ['PDB', 'ihm_cross_link_restraint_structure_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'asym id 1',
            'source': [{
                'outbound': ['PDB', 'ihm_cross_link_restraint_asym_id_1_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'asym id 2',
            'source': [{
                'outbound': ['PDB', 'ihm_cross_link_restraint_asym_id_2_fkey']
            }, 'RID']
        }, {
            'source': 'atom_id_1'
        }, {
            'source': 'atom_id_2'
        }, {
            'markdown_name': 'comp id 1',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_cross_link_restraint_mm_poly_res_label_1_fkey']
                }, 'mon_id'
            ]
        }, {
            'markdown_name': 'comp id 2',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_cross_link_restraint_mm_poly_res_label_2_fkey']
                }, 'mon_id'
            ]
        }, ['PDB', 'oss_link_restraint_conditional_crosslink_flag_term_fkey'], {
            'source': 'distance_threshold'
        }, {
            'markdown_name': 'entity id 1',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_cross_link_restraint_mm_poly_res_label_1_fkey']
                }, 'entity_id'
            ]
        }, {
            'markdown_name': 'entity id 2',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_cross_link_restraint_mm_poly_res_label_2_fkey']
                }, 'entity_id'
            ]
        }, ['PDB', 'ihm_cross_link_restraint_group_id_fkey'], {
            'source': 'id'
        }, {
            'source': [
                {
                    'outbound': ['PDB', 'ihm_cross_link_restraint_model_granularity_term_fkey']
                }, 'RID'
            ]
        }, {
            'source': 'psi'
        }, {
            'source': [
                {
                    'outbound': ['PDB', 'ihm_cross_link_restraint_restraint_type_term_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'seq id 1',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_cross_link_restraint_mm_poly_res_label_1_fkey']
                }, 'num'
            ]
        }, {
            'markdown_name': 'seq id 2',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_cross_link_restraint_mm_poly_res_label_2_fkey']
                }, 'num'
            ]
        }, {
            'source': 'sigma_1'
        }, {
            'source': 'sigma_2'
        }, {
            'source': 'entity_description_1'
        }, {
            'source': 'entity_description_2'
        }, ['PDB', 'ihm_cross_link_list_dataset_list_id_fkey'], {
            'markdown_name': 'molecular entity 1',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_cross_link_restraint_mm_poly_res_label_1_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'molecular entity 2',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_cross_link_restraint_mm_poly_res_label_2_fkey']
                }, 'RID'
            ]
        }, {
            'source': 'RCT'
        }, {
            'source': 'RMT'
        }, {
            'source': [{
                'outbound': ['PDB', 'ihm_cross_link_restraint_RCB_fkey']
            }, 'RID']
        }, {
            'source': [{
                'outbound': ['PDB', 'ihm_cross_link_restraint_RMB_fkey']
            }, 'RID']
        }, {
            'source': [{
                'outbound': ['PDB', 'ihm_cross_link_restraint_Owner_fkey']
            }, 'RID']
        }
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
    em.Key.define(['RID'], constraint_names=[['PDB', 'ihm_cross_link_restraint_RIDkey1']],
                  ),
    em.Key.define(
        ['id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_cross_link_restraint_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['conditional_crosslink_flag'],
        'Vocab',
        'oss_link_restraint_conditional_crosslink_flag_term', ['ID'],
        constraint_names=[['PDB', 'oss_link_restraint_conditional_crosslink_flag_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['model_granularity'],
        'Vocab',
        'ihm_cross_link_restraint_model_granularity_term', ['ID'],
        constraint_names=[['PDB', 'ihm_cross_link_restraint_model_granularity_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['restraint_type'],
        'Vocab',
        'ihm_cross_link_restraint_restraint_type_term', ['ID'],
        constraint_names=[['PDB', 'ihm_cross_link_restraint_restraint_type_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_cross_link_restraint_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_cross_link_restraint_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_cross_link_restraint_structure_id_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'asym_id_1'],
        'PDB',
        'struct_asym', ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_cross_link_restraint_asym_id_1_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['asym_id_2', 'structure_id'],
        'PDB',
        'struct_asym', ['id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_cross_link_restraint_asym_id_2_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'group_id'],
        'PDB',
        'ihm_cross_link_list', ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_cross_link_restraint_group_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['comp_id_1', 'seq_id_1', 'structure_id', 'entity_id_1'],
        'PDB',
        'entity_poly_seq', ['mon_id', 'num', 'structure_id', 'entity_id'],
        constraint_names=[['PDB', 'ihm_cross_link_restraint_mm_poly_res_label_1_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['comp_id_2', 'seq_id_2', 'structure_id', 'entity_id_2'],
        'PDB',
        'entity_poly_seq', ['mon_id', 'num', 'structure_id', 'entity_id'],
        constraint_names=[['PDB', 'ihm_cross_link_restraint_mm_poly_res_label_2_fkey']],
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
