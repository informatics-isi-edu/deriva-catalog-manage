import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_cross_link_result'

schema_name = 'PDB'

column_annotations = {
    'distance_threshold': {},
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'structure_id': {},
    'RMB': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Modified By'
        },
        chaise_tags.immutable: None
    },
    'num_models': {},
    'ensemble_id': {},
    'RCB': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Created By'
        },
        chaise_tags.immutable: None
    },
    'Owner': {},
    'details': {},
    'restraint_id': {},
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    },
    'median_distance': {},
    'id': {}
}

column_comment = {
    'distance_threshold': 'type:float4\nThe distance threshold applied to this crosslink in the integrative modeling task.',
    'Owner': 'Group that can update the record.',
    'details': 'type:text\nThis records holds any associated details of the results of the particular \n crosslink restraint in the integrative modeling task.',
    'restraint_id': 'A reference to table ihm_cross_link_restraint.id.',
    'num_models': 'type:int4\nNumber of models sampled in the integrative modeling task, for which\n the crosslinking distance is provided.',
    'median_distance': 'type:float4\nThe median distance between the crosslinked residues in the sampled models.',
    'structure_id': 'A reference to table entry.id.',
    'id': 'type:int4\nA unique identifier for the restraint/ensemble combination.',
    'ensemble_id': 'A reference to table ihm_ensemble_info.ensemble_id.'
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
        'distance_threshold',
        em.builtin_types['float4'],
        nullok=False,
        comment=column_comment['distance_threshold'],
    ),
    em.Column.define(
        'ensemble_id',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['ensemble_id'],
    ),
    em.Column.define('id', em.builtin_types['int4'], nullok=False, comment=column_comment['id'],
                     ),
    em.Column.define(
        'median_distance',
        em.builtin_types['float4'],
        nullok=False,
        comment=column_comment['median_distance'],
    ),
    em.Column.define(
        'num_models',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['num_models'],
    ),
    em.Column.define(
        'restraint_id',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['restraint_id'],
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
                'outbound': ['PDB', 'ihm_cross_link_result_structure_id_fkey']
            }, 'RID']
        }, 'details', 'distance_threshold', {
            'markdown_name': 'ensemble id',
            'comment': 'A reference to table ihm_ensemble_info.ensemble_id.',
            'source': [{
                'outbound': ['PDB', 'ihm_cross_link_result_ensemble_id_fkey']
            }, 'RID']
        }, 'id', 'median_distance', 'num_models', {
            'markdown_name': 'restraint id',
            'comment': 'A reference to table ihm_cross_link_restraint.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_cross_link_result_restraint_id_fkey']
            }, 'RID']
        }
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_cross_link_result_structure_id_fkey']
            }, 'RID']
        }, 'details', 'distance_threshold', {
            'markdown_name': 'ensemble id',
            'comment': 'A reference to table ihm_ensemble_info.ensemble_id.',
            'source': [{
                'outbound': ['PDB', 'ihm_cross_link_result_ensemble_id_fkey']
            }, 'RID']
        }, 'id', 'median_distance', 'num_models', {
            'markdown_name': 'restraint id',
            'comment': 'A reference to table ihm_cross_link_restraint.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_cross_link_result_restraint_id_fkey']
            }, 'RID']
        }, ['PDB', 'ihm_cross_link_result_RCB_fkey'], ['PDB', 'ihm_cross_link_result_RMB_fkey'],
        'RCT', 'RMT', ['PDB', 'ihm_cross_link_result_Owner_fkey']
    ]
}

table_annotations = {chaise_tags.visible_columns: visible_columns, }

table_comment = None

table_acls = {}

table_acl_bindings = {}

key_defs = [
    em.Key.define(['RID'], constraint_names=[['PDB', 'ihm_cross_link_result_RIDkey1']],
                  ),
    em.Key.define(
        ['id', 'structure_id'], constraint_names=[['PDB', 'ihm_cross_link_result_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_cross_link_result_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_cross_link_result_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_cross_link_result_structure_id_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['ensemble_id', 'structure_id'],
        'PDB',
        'ihm_ensemble_info', ['ensemble_id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_cross_link_result_ensemble_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'restraint_id'],
        'PDB',
        'ihm_cross_link_restraint', ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_cross_link_result_restraint_id_fkey']],
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
