import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_poly_probe_position'

schema_name = 'PDB'

column_annotations = {
    'seq_id': {},
    'entity_description': {},
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'mut_res_chem_comp_id': {},
    'Owner': {},
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
    'structure_id': {},
    'comp_id': {},
    'modification_flag': {},
    'mutation_flag': {},
    'entity_id': {},
    'mod_res_chem_comp_descriptor_id': {},
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    },
    'id': {}
}

column_comment = {
    'entity_description': 'type:text\nDescription of the entity.',
    'mut_res_chem_comp_id': 'A reference to table chem_comp.id.',
    'Owner': 'Group that can update the record.',
    'structure_id': 'A reference to table entry.id.',
    'description': 'type:text\nAn author provided description for the residue position in the polymer.',
    'comp_id': 'A reference to table chem_comp.id.',
    'entity_id': 'A reference to table entity.id.',
    'modification_flag': 'type:text\nA flag to indicate whether the residue is chemically modified or not.',
    'mutation_flag': 'type:text\nA flag to indicate whether the residue has an engineered mutation or not.',
    'seq_id': 'A reference to table entity_poly_seq.num.',
    'mod_res_chem_comp_descriptor_id': 'A reference to table ihm_chemical_component_descriptor.id.',
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
        'comp_id', em.builtin_types['text'], nullok=False, comment=column_comment['comp_id'],
    ),
    em.Column.define(
        'description', em.builtin_types['text'], comment=column_comment['description'],
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
        'mod_res_chem_comp_descriptor_id',
        em.builtin_types['int4'],
        comment=column_comment['mod_res_chem_comp_descriptor_id'],
    ),
    em.Column.define(
        'modification_flag',
        em.builtin_types['text'],
        nullok=False,
        comment=column_comment['modification_flag'],
    ),
    em.Column.define(
        'mut_res_chem_comp_id',
        em.builtin_types['text'],
        comment=column_comment['mut_res_chem_comp_id'],
    ),
    em.Column.define(
        'mutation_flag',
        em.builtin_types['text'],
        nullok=False,
        comment=column_comment['mutation_flag'],
    ),
    em.Column.define(
        'seq_id', em.builtin_types['int4'], nullok=False, comment=column_comment['seq_id'],
    ),
    em.Column.define('Owner', em.builtin_types['text'], comment=column_comment['Owner'],
                     ),
]

visible_foreign_keys = {
    'detailed': [['PDB', 'ihm_poly_probe_conjugate_position_id_fkey']],
    'filter': 'detailed'
}

visible_columns = {
    'entry': [
        {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_poly_probe_position_structure_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'comp id',
            'comment': 'A reference to table chem_comp.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_poly_probe_position_comp_id_fkey']
            }, 'RID']
        }, 'description', 'entity_description', {
            'markdown_name': 'entity id',
            'comment': 'A reference to table entity.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_poly_probe_position_entity_id_fkey']
            }, 'RID']
        }, 'id', {
            'markdown_name': 'mod res chem comp descriptor id',
            'comment': 'A reference to table ihm_chemical_component_descriptor.id.',
            'source': [
                {
                    'outbound': [
                        'PDB', 'ihm_poly_probe_position_mod_res_chem_comp_descriptor_id_fkey'
                    ]
                }, 'RID'
            ]
        }, ['PDB', 'ihm_poly_probe_position_modification_flag_term_fkey'], {
            'markdown_name': 'mut res chem comp id',
            'comment': 'A reference to table chem_comp.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_poly_probe_position_mut_res_chem_comp_id_fkey']
                }, 'RID'
            ]
        }, ['PDB', 'ihm_poly_probe_position_mutation_flag_term_fkey'], {
            'markdown_name': 'seq id',
            'comment': 'A reference to table entity_poly_seq.num.',
            'source': [{
                'outbound': ['PDB', 'ihm_poly_probe_position_seq_id_fkey']
            }, 'RID']
        }
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_poly_probe_position_structure_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'comp id',
            'comment': 'A reference to table chem_comp.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_poly_probe_position_comp_id_fkey']
            }, 'RID']
        }, 'description', 'entity_description', {
            'markdown_name': 'entity id',
            'comment': 'A reference to table entity.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_poly_probe_position_entity_id_fkey']
            }, 'RID']
        }, 'id', {
            'markdown_name': 'mod res chem comp descriptor id',
            'comment': 'A reference to table ihm_chemical_component_descriptor.id.',
            'source': [
                {
                    'outbound': [
                        'PDB', 'ihm_poly_probe_position_mod_res_chem_comp_descriptor_id_fkey'
                    ]
                }, 'RID'
            ]
        }, ['PDB', 'ihm_poly_probe_position_modification_flag_term_fkey'], {
            'markdown_name': 'mut res chem comp id',
            'comment': 'A reference to table chem_comp.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_poly_probe_position_mut_res_chem_comp_id_fkey']
                }, 'RID'
            ]
        }, ['PDB', 'ihm_poly_probe_position_mutation_flag_term_fkey'], {
            'markdown_name': 'seq id',
            'comment': 'A reference to table entity_poly_seq.num.',
            'source': [{
                'outbound': ['PDB', 'ihm_poly_probe_position_seq_id_fkey']
            }, 'RID']
        }, ['PDB', 'ihm_poly_probe_position_RCB_fkey'], ['PDB', 'ihm_poly_probe_position_RMB_fkey'],
        'RCT', 'RMT', ['PDB', 'ihm_poly_probe_position_Owner_fkey']
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
    em.Key.define(['RID'], constraint_names=[['PDB', 'ihm_poly_probe_position_RIDkey1']],
                  ),
    em.Key.define(
        ['structure_id', 'id'], constraint_names=[['PDB', 'ihm_poly_probe_position_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['modification_flag'],
        'Vocab',
        'ihm_poly_probe_position_modification_flag_term', ['ID'],
        constraint_names=[['PDB', 'ihm_poly_probe_position_modification_flag_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['mutation_flag'],
        'Vocab',
        'ihm_poly_probe_position_mutation_flag_term', ['ID'],
        constraint_names=[['PDB', 'ihm_poly_probe_position_mutation_flag_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_poly_probe_position_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_poly_probe_position_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_poly_probe_position_structure_id_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['mod_res_chem_comp_descriptor_id', 'structure_id'],
        'PDB',
        'ihm_chemical_component_descriptor', ['id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_poly_probe_position_mod_res_chem_comp_descriptor_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['mut_res_chem_comp_id', 'structure_id'],
        'PDB',
        'chem_comp', ['id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_poly_probe_position_mut_res_chem_comp_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['comp_id', 'structure_id', 'seq_id', 'entity_id'],
        'PDB',
        'entity_poly_seq', ['mon_id', 'structure_id', 'num', 'entity_id'],
        constraint_names=[['PDB', 'ihm_poly_probe_position_mm_poly_res_label_fkey']],
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
