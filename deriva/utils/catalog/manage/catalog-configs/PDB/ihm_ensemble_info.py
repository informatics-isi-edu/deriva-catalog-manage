import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_ensemble_info'

schema_name = 'PDB'

column_annotations = {
    'ensemble_file_id': {},
    'Owner': {},
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'ensemble_clustering_method': {},
    'post_process_id': {},
    'ensemble_clustering_feature': {},
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
    'ensemble_id': {},
    'structure_id': {},
    'details': {},
    'num_ensemble_models_deposited': {},
    'ensemble_precision_value': {},
    'num_ensemble_models': {},
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    },
    'ensemble_name': {},
    'model_group_id': {}
}

column_comment = {
    'ensemble_file_id': 'A reference to table ihm_external_files.id.',
    'Owner': 'Group that can update the record.',
    'ensemble_clustering_method': 'type:text\nThe clustering method used to obtain the ensemble, if applicable.',
    'post_process_id': 'A reference to table ihm_modeling_post_process.id.',
    'ensemble_clustering_feature': 'type:text\nThe parameter/feature used for clustering the models, if applicable.',
    'ensemble_precision_value': 'type:float4\nThe precision of each cluster or ensemble is calculated as dRMSD, which\n is the average C-alpha distance root mean square deviation (dRMSD) \n between the individual models in the cluster and the cluster centroid.\n The cluster centroid is defined as the model with the minimal sum of\n dRMSDs to the other models in the cluster or ensemble.',
    'num_ensemble_models': 'type:int4\nThe number of models in the current ensemble being described.',
    'structure_id': 'A reference to table entry.id.',
    'details': 'type:text\nAdditional details regarding the ensemble.',
    'num_ensemble_models_deposited': 'type:int4\nThe number of models from the current ensemble that is deposited.',
    'model_group_id': 'A reference to table ihm_model_group.id.',
    'ensemble_id': 'type:int4\nA unique id for the ensemble.',
    'ensemble_name': 'type:text\nAn optional name for the cluster or ensemble for better description.\nexamples:ensemble1,ensemble2,cluster1,cluster2,open state,closed state'
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
        'ensemble_clustering_feature',
        em.builtin_types['text'],
        comment=column_comment['ensemble_clustering_feature'],
    ),
    em.Column.define(
        'ensemble_clustering_method',
        em.builtin_types['text'],
        comment=column_comment['ensemble_clustering_method'],
    ),
    em.Column.define(
        'ensemble_file_id', em.builtin_types['int4'], comment=column_comment['ensemble_file_id'],
    ),
    em.Column.define(
        'ensemble_id',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['ensemble_id'],
    ),
    em.Column.define(
        'ensemble_name', em.builtin_types['text'], comment=column_comment['ensemble_name'],
    ),
    em.Column.define(
        'ensemble_precision_value',
        em.builtin_types['float4'],
        comment=column_comment['ensemble_precision_value'],
    ),
    em.Column.define(
        'model_group_id', em.builtin_types['int4'], comment=column_comment['model_group_id'],
    ),
    em.Column.define(
        'num_ensemble_models',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['num_ensemble_models'],
    ),
    em.Column.define(
        'num_ensemble_models_deposited',
        em.builtin_types['int4'],
        comment=column_comment['num_ensemble_models_deposited'],
    ),
    em.Column.define(
        'post_process_id', em.builtin_types['int4'], comment=column_comment['post_process_id'],
    ),
    em.Column.define('Owner', em.builtin_types['text'], comment=column_comment['Owner'],
                     ),
]

table_display = {'row_name': {'row_markdown_pattern': '{{{ensemble_id}}}'}}

visible_foreign_keys = {
    'detailed': [
        ['PDB', 'ihm_cross_link_result_ensemble_id_fkey'],
        ['PDB', 'ihm_localization_density_files_ensemble_id_fkey']
    ],
    'filter': 'detailed'
}

visible_columns = {
    'entry': [
        {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_ensemble_info_structure_id_fkey']
            }, 'RID']
        }, 'details', ['PDB', 'ihm_ensemble_info_ensemble_clustering_feature_term_fkey'],
        ['PDB', 'ihm_ensemble_info_ensemble_clustering_method_term_fkey'], {
            'markdown_name': 'ensemble file id',
            'comment': 'A reference to table ihm_external_files.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_ensemble_info_ensemble_file_id_fkey']
            }, 'RID']
        }, 'ensemble_id', 'ensemble_name', 'ensemble_precision_value', {
            'markdown_name': 'model group id',
            'comment': 'A reference to table ihm_model_group.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_ensemble_info_model_group_id_fkey']
            }, 'RID']
        }, 'num_ensemble_models', 'num_ensemble_models_deposited', {
            'markdown_name': 'post process id',
            'comment': 'A reference to table ihm_modeling_post_process.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_ensemble_info_post_process_id_fkey']
            }, 'RID']
        }
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_ensemble_info_structure_id_fkey']
            }, 'RID']
        }, 'details', ['PDB', 'ihm_ensemble_info_ensemble_clustering_feature_term_fkey'],
        ['PDB', 'ihm_ensemble_info_ensemble_clustering_method_term_fkey'], {
            'markdown_name': 'ensemble file id',
            'comment': 'A reference to table ihm_external_files.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_ensemble_info_ensemble_file_id_fkey']
            }, 'RID']
        }, 'ensemble_id', 'ensemble_name', 'ensemble_precision_value', {
            'markdown_name': 'model group id',
            'comment': 'A reference to table ihm_model_group.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_ensemble_info_model_group_id_fkey']
            }, 'RID']
        }, 'num_ensemble_models', 'num_ensemble_models_deposited', {
            'markdown_name': 'post process id',
            'comment': 'A reference to table ihm_modeling_post_process.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_ensemble_info_post_process_id_fkey']
            }, 'RID']
        }, ['PDB', 'ihm_ensemble_info_RCB_fkey'], ['PDB', 'ihm_ensemble_info_RMB_fkey'], 'RCT',
        'RMT', ['PDB', 'ihm_ensemble_info_Owner_fkey']
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
    em.Key.define(['RID'], constraint_names=[['PDB', 'ihm_ensemble_info_RIDkey1']],
                  ),
    em.Key.define(
        ['structure_id', 'ensemble_id'],
        constraint_names=[['PDB', 'ihm_ensemble_info_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['ensemble_clustering_feature'],
        'Vocab',
        'ihm_ensemble_info_ensemble_clustering_feature_term', ['ID'],
        constraint_names=[['PDB', 'ihm_ensemble_info_ensemble_clustering_feature_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['ensemble_clustering_method'],
        'Vocab',
        'ihm_ensemble_info_ensemble_clustering_method_term', ['ID'],
        constraint_names=[['PDB', 'ihm_ensemble_info_ensemble_clustering_method_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_ensemble_info_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_ensemble_info_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_ensemble_info_structure_id_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['ensemble_file_id', 'structure_id'],
        'PDB',
        'ihm_external_files', ['id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_ensemble_info_ensemble_file_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'model_group_id'],
        'PDB',
        'ihm_model_group', ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_ensemble_info_model_group_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'post_process_id'],
        'PDB',
        'ihm_modeling_post_process', ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_ensemble_info_post_process_id_fkey']],
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
