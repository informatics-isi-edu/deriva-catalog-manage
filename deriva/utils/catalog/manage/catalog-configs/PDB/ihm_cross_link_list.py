import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_cross_link_list'

schema_name = 'PDB'

column_annotations = {
    'seq_id_2': {},
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'seq_id_1': {},
    'comp_id_2': {},
    'group_id': {},
    'entity_id_2': {},
    'linker_type': {},
    'structure_id': {},
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
    'entity_description_1': {},
    'dataset_list_id': {},
    'Owner': {},
    'details': {},
    'linker_chem_comp_descriptor_id': {},
    'entity_id_1': {},
    'comp_id_1': {},
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    },
    'entity_description_2': {},
    'id': {}
}

column_comment = {
    'seq_id_2': 'A reference to table entity_poly_seq.num.',
    'comp_id_1': 'A reference to table chem_comp.id.',
    'comp_id_2': 'A reference to table chem_comp.id.',
    'group_id': 'type:int4\nAn identifier for a set of ambiguous crosslink restraints. \n Handles experimental uncertainties in the identities of \n crosslinked residues.',
    'entity_id_2': 'A reference to table entity.id.',
    'seq_id_1': 'A reference to table entity_poly_seq.num.',
    'linker_type': 'type:text\nThe type of crosslinker used.',
    'structure_id': 'A reference to table entry.id.',
    'entity_description_1': 'type:text\nA text description of molecular entity 1. \n',
    'dataset_list_id': 'A reference to table ihm_dataset_list.id.',
    'Owner': 'Group that can update the record.',
    'details': 'type:text\nAdditional details regarding the cross link or the cross linking agent.',
    'linker_chem_comp_descriptor_id': 'A reference to table ihm_chemical_component_descriptor.id.',
    'entity_id_1': 'A reference to table entity.id.',
    'entity_description_2': 'type:text\nA text description of molecular entity 2. \n',
    'id': 'type:int4\nA unique identifier for the cross link restraint.'
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
    em.Column.define('details', em.builtin_types['text'], comment=column_comment['details'],
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
    em.Column.define(
        'group_id', em.builtin_types['int4'], nullok=False, comment=column_comment['group_id'],
    ),
    em.Column.define('id', em.builtin_types['int4'], nullok=False, comment=column_comment['id'],
                     ),
    em.Column.define(
        'linker_chem_comp_descriptor_id',
        em.builtin_types['int4'],
        comment=column_comment['linker_chem_comp_descriptor_id'],
    ),
    em.Column.define(
        'linker_type',
        em.builtin_types['text'],
        nullok=False,
        comment=column_comment['linker_type'],
    ),
    em.Column.define(
        'seq_id_1', em.builtin_types['int4'], nullok=False, comment=column_comment['seq_id_1'],
    ),
    em.Column.define(
        'seq_id_2', em.builtin_types['int4'], nullok=False, comment=column_comment['seq_id_2'],
    ),
    em.Column.define('Owner', em.builtin_types['text'], comment=column_comment['Owner'],
                     ),
]

table_display = {'row_name': {'row_markdown_pattern': '{{{id}}}'}}

visible_foreign_keys = {
    'detailed': [['PDB', 'ihm_cross_link_restraint_group_id_fkey']],
    'filter': 'detailed'
}

visible_columns = {
    '*': [
        {
            'source': 'RID'
        }, {
            'source': [{
                'outbound': ['PDB', 'ihm_cross_link_list_structure_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'comp id 1',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_cross_link_list_mm_poly_res_label_1_fkey']
                }, 'mon_id'
            ]
        }, {
            'markdown_name': 'comp id 2',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_cross_link_list_mm_poly_res_label_2_fkey']
                }, 'mon_id'
            ]
        }, ['PDB', 'ihm_cross_link_list_dataset_list_id_fkey'], {
            'source': 'entity_description_1'
        }, {
            'source': 'entity_description_2'
        }, {
            'markdown_name': 'entity id 1',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_cross_link_list_mm_poly_res_label_1_fkey']
                }, 'entity_id'
            ]
        }, {
            'markdown_name': 'entity id 2',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_cross_link_list_mm_poly_res_label_2_fkey']
                }, 'entity_id'
            ]
        }, {
            'source': 'group_id'
        }, {
            'source': 'id'
        }, ['PDB', 'ihm_cross_link_list_linker_type_term_fkey'], {
            'markdown_name': 'seq id 1',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_cross_link_list_mm_poly_res_label_1_fkey']
                }, 'num'
            ]
        }, {
            'markdown_name': 'seq id 2',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_cross_link_list_mm_poly_res_label_2_fkey']
                }, 'num'
            ]
        }, {
            'markdown_name': 'molecular entity 1',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_cross_link_list_mm_poly_res_label_1_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'molecular entity 2',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_cross_link_list_mm_poly_res_label_2_fkey']
                }, 'RID'
            ]
        }, {
            'source': 'RCT'
        }, {
            'source': 'RMT'
        }, {
            'source': [{
                'outbound': ['PDB', 'ihm_cross_link_list_RCB_fkey']
            }, 'RID']
        }, {
            'source': [{
                'outbound': ['PDB', 'ihm_cross_link_list_RMB_fkey']
            }, 'RID']
        }, {
            'source': [{
                'outbound': ['PDB', 'ihm_cross_link_list_Owner_fkey']
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
    em.Key.define(['RID'], constraint_names=[['PDB', 'ihm_cross_link_list_RIDkey1']],
                  ),
    em.Key.define(
        ['structure_id', 'id'], constraint_names=[['PDB', 'ihm_cross_link_list_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['linker_type'],
        'Vocab',
        'ihm_cross_link_list_linker_type_term', ['ID'],
        constraint_names=[['PDB', 'ihm_cross_link_list_linker_type_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_cross_link_list_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_cross_link_list_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_cross_link_list_structure_id_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['dataset_list_id', 'structure_id'],
        'PDB',
        'ihm_dataset_list', ['id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_cross_link_list_dataset_list_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['linker_chem_comp_descriptor_id', 'structure_id'],
        'PDB',
        'ihm_chemical_component_descriptor', ['id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_cross_link_list_linker_chem_comp_descriptor_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'seq_id_1', 'entity_id_1', 'comp_id_1'],
        'PDB',
        'entity_poly_seq', ['structure_id', 'num', 'entity_id', 'mon_id'],
        constraint_names=[['PDB', 'ihm_cross_link_list_mm_poly_res_label_1_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'comp_id_2', 'seq_id_2', 'entity_id_2'],
        'PDB',
        'entity_poly_seq', ['structure_id', 'mon_id', 'num', 'entity_id'],
        constraint_names=[['PDB', 'ihm_cross_link_list_mm_poly_res_label_2_fkey']],
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
