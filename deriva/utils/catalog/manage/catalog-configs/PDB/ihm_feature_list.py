import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_feature_list'

schema_name = 'PDB'

column_annotations = {
    'Owner': {},
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'feature_id': {},
    'details': {},
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    },
    'feature_type': {},
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
    'entity_type': {},
    'structure_id': {}
}

column_comment = {
    'Owner': 'Group that can update the record.',
    'details': 'type:text\nAdditional details regarding the feature.',
    'feature_type': 'type:text\nThe type of feature.',
    'feature_id': 'type:int4\nA unique identifier for the feature.',
    'structure_id': 'A reference to table entry.id.',
    'entity_type': 'type:text\nThe type of entity.'
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
        'entity_type',
        em.builtin_types['text'],
        nullok=False,
        comment=column_comment['entity_type'],
    ),
    em.Column.define(
        'feature_id',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['feature_id'],
    ),
    em.Column.define(
        'feature_type',
        em.builtin_types['text'],
        nullok=False,
        comment=column_comment['feature_type'],
    ),
    em.Column.define('Owner', em.builtin_types['text'], comment=column_comment['Owner'],
                     ),
]

table_display = {'row_name': {'row_markdown_pattern': '{{{feature_id}}}'}}

visible_foreign_keys = {
    'detailed': [
        ['PDB', 'ihm_poly_atom_feature_feature_id_fkey'],
        ['PDB', 'ihm_poly_residue_feature_feature_id_fkey'],
        ['PDB', 'ihm_non_poly_feature_feature_id_fkey'],
        ['PDB', 'ihm_interface_residue_feature_feature_id_fkey'],
        ['PDB', 'ihm_pseudo_site_feature_feature_id_fkey'],
        ['PDB', 'ihm_derived_distance_restraint_feature_id_1_fkey'],
        ['PDB', 'ihm_derived_distance_restraint_feature_id_2_fkey'],
        ['PDB', 'ihm_geometric_object_distance_restraint_feature_id_fkey']
    ],
    'filter': 'detailed'
}

visible_columns = {
    'entry': [
        {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_feature_list_structure_id_fkey']
            }, 'RID']
        }, 'details', ['PDB', 'ihm_feature_list_entity_type_term_fkey'], 'feature_id',
        ['PDB', 'ihm_feature_list_feature_type_term_fkey']
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_feature_list_structure_id_fkey']
            }, 'RID']
        }, 'details', ['PDB', 'ihm_feature_list_entity_type_term_fkey'], 'feature_id',
        ['PDB', 'ihm_feature_list_feature_type_term_fkey'], ['PDB', 'ihm_feature_list_RCB_fkey'],
        ['PDB', 'ihm_feature_list_RMB_fkey'], 'RCT', 'RMT', ['PDB', 'ihm_feature_list_Owner_fkey']
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
    em.Key.define(['RID'], constraint_names=[['PDB', 'ihm_feature_list_RIDkey1']],
                  ),
    em.Key.define(
        ['structure_id', 'feature_id'],
        constraint_names=[['PDB', 'ihm_feature_list_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['entity_type'],
        'Vocab',
        'ihm_feature_list_entity_type_term', ['ID'],
        constraint_names=[['PDB', 'ihm_feature_list_entity_type_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['feature_type'],
        'Vocab',
        'ihm_feature_list_feature_type_term', ['ID'],
        constraint_names=[['PDB', 'ihm_feature_list_feature_type_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_feature_list_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_feature_list_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_feature_list_structure_id_fkey']],
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
