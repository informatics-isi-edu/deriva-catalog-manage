import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'entity'

schema_name = 'PDB'

column_annotations = {
    'formula_weight': {},
    'src_method': {},
    'pdbx_number_of_molecules': {},
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
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
    'structure_id': {},
    'Owner': {},
    'details': {},
    'id': {},
    'type': {},
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    },
    'pdbx_description': {}
}

column_comment = {
    'formula_weight': 'type:float4\nFormula mass in daltons of the entity.',
    'src_method': 'type:text\nThe method by which the sample for the entity was produced.\n Entities isolated directly from natural sources (tissues, soil\n samples etc.) are expected to have further information in the\n ENTITY_SRC_NAT category. Entities isolated from genetically\n manipulated sources are expected to have further information in\n the ENTITY_SRC_GEN category.',
    'pdbx_number_of_molecules': 'type:int4\nA place holder for the number of molecules of the entity in\n the entry.\nexamples:1,2,3',
    'details': 'type:text\nA description of special aspects of the entity.',
    'id': 'type:text\nThe value of _entity.id must uniquely identify a record in the\n ENTITY list.\n\n Note that this item need not be a number; it can be any unique\n identifier.',
    'Owner': 'Group that can update the record.',
    'type': 'type:text\nDefines the type of the entity.\n\n Polymer entities are expected to have corresponding\n ENTITY_POLY and associated entries.\n\n Non-polymer entities are expected to have corresponding\n CHEM_COMP and associated entries.\n\n Water entities are not expected to have corresponding\n entries in the ENTITY category.',
    'structure_id': 'A reference to table entry.id.',
    'pdbx_description': "type:text\nA description of the entity.\n\n Corresponds to the compound name in the PDB format.\nexamples:Green fluorescent protein,DNA (5'-D(*GP*(CH3)CP*GP*(CH3)CP*GP*C)-3'),PROFLAVINE,PROTEIN (DEOXYRIBONUCLEASE I (E.C.3.1.21.1))"
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
    em.Column.define('details', em.builtin_types['text'], comment=column_comment['details'],
                     ),
    em.Column.define(
        'formula_weight', em.builtin_types['float4'], comment=column_comment['formula_weight'],
    ),
    em.Column.define('id', em.builtin_types['text'], nullok=False, comment=column_comment['id'],
                     ),
    em.Column.define(
        'pdbx_description', em.builtin_types['text'], comment=column_comment['pdbx_description'],
    ),
    em.Column.define(
        'pdbx_number_of_molecules',
        em.builtin_types['int4'],
        comment=column_comment['pdbx_number_of_molecules'],
    ),
    em.Column.define(
        'src_method', em.builtin_types['text'], comment=column_comment['src_method'],
    ),
    em.Column.define('type', em.builtin_types['text'], comment=column_comment['type'],
                     ),
    em.Column.define('Owner', em.builtin_types['text'], comment=column_comment['Owner'],
                     ),
]

table_display = {'row_name': {'row_markdown_pattern': '{{{id}}}'}}

visible_foreign_keys = {
    'detailed': [
        ['PDB', 'entity_name_com_entity_id_fkey'], ['PDB', 'entity_name_sys_entity_id_fkey'],
        ['PDB', 'entity_src_gen_entity_id_fkey'], ['PDB', 'entity_poly_entity_id_fkey'],
        ['PDB', 'pdbx_entity_nonpoly_entity_id_fkey'], ['PDB', 'struct_asym_entity_id_fkey'],
        ['PDB', 'ihm_model_representation_details_entity_id_fkey'],
        ['PDB', 'ihm_struct_assembly_details_entity_id_fkey'],
        ['PDB', 'ihm_starting_model_details_entity_id_fkey'],
        ['PDB', 'ihm_ligand_probe_entity_id_fkey'], ['PDB', 'ihm_non_poly_feature_entity_id_fkey'],
        ['PDB', 'ihm_interface_residue_feature_binding_partner_entity_id_fkey'],
        ['PDB', 'ihm_localization_density_files_entity_id_fkey'],
        ['PDB', 'pdbx_entity_poly_na_type_entity_id_fkey']
    ],
    'filter': 'detailed'
}

visible_columns = {
    'entry': [
        {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'entity_structure_id_fkey']
            }, 'RID']
        }, 'details', 'formula_weight', 'id', 'pdbx_description', 'pdbx_number_of_molecules',
        ['PDB', 'entity_src_method_term_fkey'], ['PDB', 'entity_type_term_fkey']
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'entity_structure_id_fkey']
            }, 'RID']
        }, 'details', 'formula_weight', 'id', 'pdbx_description', 'pdbx_number_of_molecules',
        ['PDB', 'entity_src_method_term_fkey'], ['PDB', 'entity_type_term_fkey'],
        ['PDB', 'entity_RCB_fkey'], ['PDB', 'entity_RMB_fkey'], 'RCT', 'RMT',
        ['PDB', 'entity_Owner_fkey']
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
    em.Key.define(['RID'], constraint_names=[['PDB', 'entity_RIDkey1']],
                  ),
    em.Key.define(['structure_id', 'id'], constraint_names=[['PDB', 'entity_primary_key']],
                  ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['src_method'],
        'Vocab',
        'entity_src_method_term', ['ID'],
        constraint_names=[['PDB', 'entity_src_method_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['type'],
        'Vocab',
        'entity_type_term', ['ID'],
        constraint_names=[['PDB', 'entity_type_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'entity_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'entity_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'entity_structure_id_fkey']],
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
