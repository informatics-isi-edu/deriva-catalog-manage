import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_interface_residue_feature'

schema_name = 'PDB'

column_annotations = {
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'binding_partner_asym_id': {},
    'feature_id': {},
    'structure_id': {},
    'RMB': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Modified By'
        },
        chaise_tags.immutable: None
    },
    'binding_partner_entity_id': {},
    'RCB': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Created By'
        },
        chaise_tags.immutable: None
    },
    'dataset_list_id': {},
    'Owner': {},
    'details': {},
    'ordinal_id': {},
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    }
}

column_comment = {
    'dataset_list_id': 'A reference to table ihm_dataset_list.id.',
    'Owner': 'Group that can update the record.',
    'details': 'type:text\nAdditional details regarding the interface residue.',
    'ordinal_id': 'type:int4\nA unique identifier for the category.',
    'binding_partner_asym_id': 'A reference to table struct_asym.id.',
    'feature_id': 'A reference to table ihm_feature_list.feature_id.',
    'structure_id': 'A reference to table entry.id.',
    'binding_partner_entity_id': 'A reference to table entity.id.'
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
        'binding_partner_asym_id',
        em.builtin_types['text'],
        comment=column_comment['binding_partner_asym_id'],
    ),
    em.Column.define(
        'binding_partner_entity_id',
        em.builtin_types['text'],
        nullok=False,
        comment=column_comment['binding_partner_entity_id'],
    ),
    em.Column.define(
        'dataset_list_id', em.builtin_types['int4'], comment=column_comment['dataset_list_id'],
    ),
    em.Column.define('details', em.builtin_types['text'], comment=column_comment['details'],
                     ),
    em.Column.define(
        'feature_id',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['feature_id'],
    ),
    em.Column.define(
        'ordinal_id',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['ordinal_id'],
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
                    'outbound': ['PDB', 'ihm_interface_residue_feature_structure_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'binding partner asym id',
            'comment': 'A reference to table struct_asym.id.',
            'source': [
                {
                    'outbound': [
                        'PDB', 'ihm_interface_residue_feature_binding_partner_asym_id_fkey'
                    ]
                }, 'RID'
            ]
        }, {
            'markdown_name': 'binding partner entity id',
            'comment': 'A reference to table entity.id.',
            'source': [
                {
                    'outbound': [
                        'PDB', 'ihm_interface_residue_feature_binding_partner_entity_id_fkey'
                    ]
                }, 'RID'
            ]
        }, {
            'markdown_name': 'dataset list id',
            'comment': 'A reference to table ihm_dataset_list.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_interface_residue_feature_dataset_list_id_fkey']
                }, 'RID'
            ]
        }, 'details', {
            'markdown_name': 'feature id',
            'comment': 'A reference to table ihm_feature_list.feature_id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_interface_residue_feature_feature_id_fkey']
                }, 'RID'
            ]
        }, 'ordinal_id'
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_interface_residue_feature_structure_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'binding partner asym id',
            'comment': 'A reference to table struct_asym.id.',
            'source': [
                {
                    'outbound': [
                        'PDB', 'ihm_interface_residue_feature_binding_partner_asym_id_fkey'
                    ]
                }, 'RID'
            ]
        }, {
            'markdown_name': 'binding partner entity id',
            'comment': 'A reference to table entity.id.',
            'source': [
                {
                    'outbound': [
                        'PDB', 'ihm_interface_residue_feature_binding_partner_entity_id_fkey'
                    ]
                }, 'RID'
            ]
        }, {
            'markdown_name': 'dataset list id',
            'comment': 'A reference to table ihm_dataset_list.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_interface_residue_feature_dataset_list_id_fkey']
                }, 'RID'
            ]
        }, 'details', {
            'markdown_name': 'feature id',
            'comment': 'A reference to table ihm_feature_list.feature_id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_interface_residue_feature_feature_id_fkey']
                }, 'RID'
            ]
        }, 'ordinal_id', ['PDB', 'ihm_interface_residue_feature_RCB_fkey'],
        ['PDB', 'ihm_interface_residue_feature_RMB_fkey'], 'RCT', 'RMT',
        ['PDB', 'ihm_interface_residue_feature_Owner_fkey']
    ]
}

table_annotations = {chaise_tags.visible_columns: visible_columns, }

table_comment = None

table_acls = {}

table_acl_bindings = {}

key_defs = [
    em.Key.define(['RID'], constraint_names=[['PDB', 'ihm_interface_residue_feature_RIDkey1']],
                  ),
    em.Key.define(
        ['ordinal_id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_interface_residue_feature_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_interface_residue_feature_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_interface_residue_feature_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_interface_residue_feature_structure_id_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['binding_partner_asym_id', 'structure_id'],
        'PDB',
        'struct_asym', ['id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_interface_residue_feature_binding_partner_asym_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'binding_partner_entity_id'],
        'PDB',
        'entity', ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_interface_residue_feature_binding_partner_entity_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'dataset_list_id'],
        'PDB',
        'ihm_dataset_list', ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_interface_residue_feature_dataset_list_id_fkey']],
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
        constraint_names=[['PDB', 'ihm_interface_residue_feature_feature_id_fkey']],
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
