import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_model_representative'

schema_name = 'PDB'

column_annotations = {
    'model_id': {},
    'selection_criteria': {},
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
    'id': {},
    'structure_id': {}
}

column_comment = {
    'model_id': 'A reference to table ihm_model_list.model_id.',
    'selection_criteria': 'type:text\nThe selection criteria based on which the representative is chosen.',
    'Owner': 'Group that can update the record.',
    'model_group_id': 'A reference to table ihm_model_group.id.',
    'structure_id': 'A reference to table entry.id.',
    'id': 'type:int4\nA unique identifier for the representative of the model group.'
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
    em.Column.define('id', em.builtin_types['int4'], nullok=False, comment=column_comment['id'],
                     ),
    em.Column.define(
        'model_group_id',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['model_group_id'],
    ),
    em.Column.define(
        'model_id', em.builtin_types['int4'], nullok=False, comment=column_comment['model_id'],
    ),
    em.Column.define(
        'selection_criteria',
        em.builtin_types['text'],
        comment=column_comment['selection_criteria'],
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
                'outbound': ['PDB', 'ihm_model_representative_structure_id_fkey']
            }, 'RID']
        }, 'id', {
            'markdown_name': 'model group id',
            'comment': 'A reference to table ihm_model_group.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_model_representative_model_group_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'model id',
            'comment': 'A reference to table ihm_model_list.model_id.',
            'source': [{
                'outbound': ['PDB', 'ihm_model_representative_model_id_fkey']
            }, 'RID']
        }, ['PDB', 'ihm_model_representative_selection_criteria_term_fkey']
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_model_representative_structure_id_fkey']
            }, 'RID']
        }, 'id', {
            'markdown_name': 'model group id',
            'comment': 'A reference to table ihm_model_group.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_model_representative_model_group_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'model id',
            'comment': 'A reference to table ihm_model_list.model_id.',
            'source': [{
                'outbound': ['PDB', 'ihm_model_representative_model_id_fkey']
            }, 'RID']
        }, ['PDB', 'ihm_model_representative_selection_criteria_term_fkey'],
        ['PDB', 'ihm_model_representative_RCB_fkey'], ['PDB', 'ihm_model_representative_RMB_fkey'],
        'RCT', 'RMT', ['PDB', 'ihm_model_representative_Owner_fkey']
    ]
}

table_annotations = {chaise_tags.visible_columns: visible_columns, }

table_comment = None

table_acls = {}

table_acl_bindings = {}

key_defs = [
    em.Key.define(['RID'], constraint_names=[['PDB', 'ihm_model_representative_RIDkey1']],
                  ),
    em.Key.define(
        ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_model_representative_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['selection_criteria'],
        'Vocab',
        'ihm_model_representative_selection_criteria_term', ['ID'],
        constraint_names=[['PDB', 'ihm_model_representative_selection_criteria_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_model_representative_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_model_representative_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_model_representative_structure_id_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'model_group_id'],
        'PDB',
        'ihm_model_group', ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_model_representative_model_group_id_fkey']],
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
        constraint_names=[['PDB', 'ihm_model_representative_model_id_fkey']],
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
