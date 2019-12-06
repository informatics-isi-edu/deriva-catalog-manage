import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_ordered_ensemble'

schema_name = 'PDB'

column_annotations = {
    'model_group_id_begin': {},
    'ordered_by': {},
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'step_description': {},
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
    'edge_description': {},
    'process_id': {},
    'Owner': {},
    'process_description': {},
    'step_id': {},
    'edge_id': {},
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    },
    'model_group_id_end': {}
}

column_comment = {
    'edge_description': 'type:text\nDescription of the edge.',
    'model_group_id_begin': 'A reference to table ihm_model_group.id.',
    'Owner': 'Group that can update the record.',
    'process_description': 'type:text\nDescription of the ordered process.',
    'step_id': 'type:int4\nIdentifier for a particular step in the ordered process.',
    'step_description': 'type:text\nDescription of the step.',
    'edge_id': 'type:int4\nAn identifier that describes an edge in a directed graph, which\n represents an ordered ensemble. \n Forms the category key together with _ihm_ordered_ensemble.process_id.',
    'process_id': 'type:int4\nAn identifier for the ordered process. \n Forms the category key together with _ihm_ordered_ensemble.edge_id.',
    'ordered_by': 'type:text\nThe parameter based on which the ordering is carried out.\nexamples:time steps,steps in an assembly process,steps in a metabolic pathway,steps in an interaction pathway',
    'structure_id': 'A reference to table entry.id.',
    'model_group_id_end': 'A reference to table ihm_model_group.id.'
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
        'edge_description', em.builtin_types['text'], comment=column_comment['edge_description'],
    ),
    em.Column.define(
        'edge_id', em.builtin_types['int4'], nullok=False, comment=column_comment['edge_id'],
    ),
    em.Column.define(
        'model_group_id_begin',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['model_group_id_begin'],
    ),
    em.Column.define(
        'model_group_id_end',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['model_group_id_end'],
    ),
    em.Column.define(
        'ordered_by', em.builtin_types['text'], comment=column_comment['ordered_by'],
    ),
    em.Column.define(
        'process_description',
        em.builtin_types['text'],
        comment=column_comment['process_description'],
    ),
    em.Column.define(
        'process_id',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['process_id'],
    ),
    em.Column.define(
        'step_description', em.builtin_types['text'], comment=column_comment['step_description'],
    ),
    em.Column.define(
        'step_id', em.builtin_types['int4'], nullok=False, comment=column_comment['step_id'],
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
                'outbound': ['PDB', 'ihm_ordered_ensemble_structure_id_fkey']
            }, 'RID']
        }, 'edge_description', 'edge_id', {
            'markdown_name': 'model group id begin',
            'comment': 'A reference to table ihm_model_group.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_ordered_ensemble_model_group_id_begin_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'model group id end',
            'comment': 'A reference to table ihm_model_group.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_ordered_ensemble_model_group_id_end_fkey']
                }, 'RID'
            ]
        }, 'ordered_by', 'process_description', 'process_id', 'step_description', 'step_id'
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_ordered_ensemble_structure_id_fkey']
            }, 'RID']
        }, 'edge_description', 'edge_id', {
            'markdown_name': 'model group id begin',
            'comment': 'A reference to table ihm_model_group.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_ordered_ensemble_model_group_id_begin_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'model group id end',
            'comment': 'A reference to table ihm_model_group.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_ordered_ensemble_model_group_id_end_fkey']
                }, 'RID'
            ]
        }, 'ordered_by', 'process_description', 'process_id', 'step_description', 'step_id',
        ['PDB', 'ihm_ordered_ensemble_RCB_fkey'], ['PDB', 'ihm_ordered_ensemble_RMB_fkey'], 'RCT',
        'RMT', ['PDB', 'ihm_ordered_ensemble_Owner_fkey']
    ]
}

table_annotations = {chaise_tags.visible_columns: visible_columns, }

table_comment = None

table_acls = {}

table_acl_bindings = {}

key_defs = [
    em.Key.define(['RID'], constraint_names=[['PDB', 'ihm_ordered_ensemble_RIDkey1']],
                  ),
    em.Key.define(
        ['edge_id', 'process_id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_ordered_ensemble_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_ordered_ensemble_structure_id_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_ordered_ensemble_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_ordered_ensemble_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['model_group_id_begin', 'structure_id'],
        'PDB',
        'ihm_model_group', ['id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_ordered_ensemble_model_group_id_begin_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'model_group_id_end'],
        'PDB',
        'ihm_model_group', ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_ordered_ensemble_model_group_id_end_fkey']],
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
