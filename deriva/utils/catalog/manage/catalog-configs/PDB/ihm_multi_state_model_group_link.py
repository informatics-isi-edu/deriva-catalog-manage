import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_multi_state_model_group_link'

schema_name = 'PDB'

column_annotations = {
    'state_id': {},
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'Owner': {},
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    },
    'model_group_id': {},
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
    'model_group_id': 'A reference to table ihm_model_group.id.',
    'structure_id': 'A reference to table entry.id.',
    'Owner': 'Group that can update the record.',
    'state_id': 'A reference to table ihm_multi_state_modeling.state_id.'
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
        'model_group_id',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['model_group_id'],
    ),
    em.Column.define(
        'state_id', em.builtin_types['int4'], nullok=False, comment=column_comment['state_id'],
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
                    'outbound': ['PDB', 'ihm_multi_state_model_group_link_structure_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'model group id',
            'comment': 'A reference to table ihm_model_group.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_multi_state_model_group_link_model_group_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'state id',
            'comment': 'A reference to table ihm_multi_state_modeling.state_id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_multi_state_model_group_link_state_id_fkey']
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
                    'outbound': ['PDB', 'ihm_multi_state_model_group_link_structure_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'model group id',
            'comment': 'A reference to table ihm_model_group.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_multi_state_model_group_link_model_group_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'state id',
            'comment': 'A reference to table ihm_multi_state_modeling.state_id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_multi_state_model_group_link_state_id_fkey']
                }, 'RID'
            ]
        }, ['PDB', 'ihm_multi_state_model_group_link_RCB_fkey'],
        ['PDB', 'ihm_multi_state_model_group_link_RMB_fkey'], 'RCT', 'RMT',
        ['PDB', 'ihm_multi_state_model_group_link_Owner_fkey']
    ]
}

table_annotations = {chaise_tags.visible_columns: visible_columns, }

table_comment = None

table_acls = {}

table_acl_bindings = {}

key_defs = [
    em.Key.define(
        ['RID'], constraint_names=[['PDB', 'ihm_multi_state_model_group_link_RIDkey1']],
    ),
    em.Key.define(
        ['model_group_id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_multi_state_model_group_link_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_multi_state_model_group_link_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_multi_state_model_group_link_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_multi_state_model_group_link_structure_id_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['model_group_id', 'structure_id'],
        'PDB',
        'ihm_model_group', ['id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_multi_state_model_group_link_model_group_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'state_id'],
        'PDB',
        'ihm_multi_state_modeling', ['structure_id', 'state_id'],
        constraint_names=[['PDB', 'ihm_multi_state_model_group_link_state_id_fkey']],
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
