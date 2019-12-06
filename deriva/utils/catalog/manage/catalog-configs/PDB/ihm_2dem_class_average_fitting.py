import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_2dem_class_average_fitting'

schema_name = 'PDB'

column_annotations = {
    'tr_vector_2': {},
    'rot_matrix_2_3': {},
    'rot_matrix_3_3': {},
    'rot_matrix_3_1': {},
    'rot_matrix_1_1': {},
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
    'tr_vector_1': {},
    'model_id': {},
    'Owner': {},
    'rot_matrix_1_2': {},
    'restraint_id': {},
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    },
    'rot_matrix_2_2': {},
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'tr_vector_3': {},
    'rot_matrix_2_1': {},
    'rot_matrix_1_3': {},
    'structure_id': {},
    'rot_matrix_3_2': {},
    'id': {},
    'cross_correlation_coefficient': {}
}

column_comment = {
    'rot_matrix_2_2': 'type:float4\nData item [2][2] of the rotation matrix used in the fitting of the model to the image.',
    'rot_matrix_3_2': 'type:float4\nData item [3][2] of the rotation matrix used in the fitting of the model to the image.',
    'tr_vector_2': 'type:float4\nData item [2] of the tranlation vector used in the fitting of the model to the image.',
    'rot_matrix_2_3': 'type:float4\nData item [2][3] of the rotation matrix used in the fitting of the model to the image.',
    'rot_matrix_3_3': 'type:float4\nData item [3][3] of the rotation matrix used in the fitting of the model to the image.',
    'rot_matrix_3_1': 'type:float4\nData item [3][1] of the rotation matrix used in the fitting of the model to the image.',
    'rot_matrix_2_1': 'type:float4\nData item [2][1] of the rotation matrix used in the fitting of the model to the image.',
    'rot_matrix_1_3': 'type:float4\nData item [1][3] of the rotation matrix used in the fitting of the model to the image.',
    'rot_matrix_1_1': 'type:float4\nData item [1][1] of the rotation matrix used in the fitting of the model to the image.',
    'structure_id': 'A reference to table entry.id.',
    'tr_vector_1': 'type:float4\nData item [1] of the tranlation vector used in the fitting of the model to the image.',
    'model_id': 'A reference to table ihm_model_list.model_id.',
    'tr_vector_3': 'type:float4\nData item [3] of the tranlation vector used in the fitting of the model to the image.',
    'Owner': 'Group that can update the record.',
    'rot_matrix_1_2': 'type:float4\nData item [1][2] of the rotation matrix used in the fitting of the model to the image.',
    'restraint_id': 'A reference to table ihm_2dem_class_average_restraint.id.',
    'id': 'type:int4\nA unique identifier for the 2dem class average fitting data.',
    'cross_correlation_coefficient': 'type:float4\nThe cross correlation coefficient corresponding to the model to image fitting.'
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
        'cross_correlation_coefficient',
        em.builtin_types['float4'],
        comment=column_comment['cross_correlation_coefficient'],
    ),
    em.Column.define('id', em.builtin_types['int4'], nullok=False, comment=column_comment['id'],
                     ),
    em.Column.define(
        'model_id', em.builtin_types['int4'], nullok=False, comment=column_comment['model_id'],
    ),
    em.Column.define(
        'restraint_id',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['restraint_id'],
    ),
    em.Column.define(
        'rot_matrix_1_1', em.builtin_types['float4'], comment=column_comment['rot_matrix_1_1'],
    ),
    em.Column.define(
        'rot_matrix_1_2', em.builtin_types['float4'], comment=column_comment['rot_matrix_1_2'],
    ),
    em.Column.define(
        'rot_matrix_1_3', em.builtin_types['float4'], comment=column_comment['rot_matrix_1_3'],
    ),
    em.Column.define(
        'rot_matrix_2_1', em.builtin_types['float4'], comment=column_comment['rot_matrix_2_1'],
    ),
    em.Column.define(
        'rot_matrix_2_2', em.builtin_types['float4'], comment=column_comment['rot_matrix_2_2'],
    ),
    em.Column.define(
        'rot_matrix_2_3', em.builtin_types['float4'], comment=column_comment['rot_matrix_2_3'],
    ),
    em.Column.define(
        'rot_matrix_3_1', em.builtin_types['float4'], comment=column_comment['rot_matrix_3_1'],
    ),
    em.Column.define(
        'rot_matrix_3_2', em.builtin_types['float4'], comment=column_comment['rot_matrix_3_2'],
    ),
    em.Column.define(
        'rot_matrix_3_3', em.builtin_types['float4'], comment=column_comment['rot_matrix_3_3'],
    ),
    em.Column.define(
        'tr_vector_1', em.builtin_types['float4'], comment=column_comment['tr_vector_1'],
    ),
    em.Column.define(
        'tr_vector_2', em.builtin_types['float4'], comment=column_comment['tr_vector_2'],
    ),
    em.Column.define(
        'tr_vector_3', em.builtin_types['float4'], comment=column_comment['tr_vector_3'],
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
                    'outbound': ['PDB', 'ihm_2dem_class_average_fitting_structure_id_fkey']
                }, 'RID'
            ]
        }, 'cross_correlation_coefficient', 'id', {
            'markdown_name': 'model id',
            'comment': 'A reference to table ihm_model_list.model_id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_2dem_class_average_fitting_model_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'restraint id',
            'comment': 'A reference to table ihm_2dem_class_average_restraint.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_2dem_class_average_fitting_restraint_id_fkey']
                }, 'RID'
            ]
        }, 'rot_matrix_1_1', 'rot_matrix_1_2', 'rot_matrix_1_3', 'rot_matrix_2_1', 'rot_matrix_2_2',
        'rot_matrix_2_3', 'rot_matrix_3_1', 'rot_matrix_3_2', 'rot_matrix_3_3', 'tr_vector_1',
        'tr_vector_2', 'tr_vector_3'
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_2dem_class_average_fitting_structure_id_fkey']
                }, 'RID'
            ]
        }, 'cross_correlation_coefficient', 'id', {
            'markdown_name': 'model id',
            'comment': 'A reference to table ihm_model_list.model_id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_2dem_class_average_fitting_model_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'restraint id',
            'comment': 'A reference to table ihm_2dem_class_average_restraint.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_2dem_class_average_fitting_restraint_id_fkey']
                }, 'RID'
            ]
        }, 'rot_matrix_1_1', 'rot_matrix_1_2', 'rot_matrix_1_3', 'rot_matrix_2_1', 'rot_matrix_2_2',
        'rot_matrix_2_3', 'rot_matrix_3_1', 'rot_matrix_3_2', 'rot_matrix_3_3', 'tr_vector_1',
        'tr_vector_2', 'tr_vector_3', ['PDB', 'ihm_2dem_class_average_fitting_RCB_fkey'],
        ['PDB', 'ihm_2dem_class_average_fitting_RMB_fkey'], 'RCT', 'RMT',
        ['PDB', 'ihm_2dem_class_average_fitting_Owner_fkey']
    ]
}

table_annotations = {chaise_tags.visible_columns: visible_columns, }

table_comment = None

table_acls = {}

table_acl_bindings = {}

key_defs = [
    em.Key.define(['RID'], constraint_names=[['PDB', 'ihm_2dem_class_average_fitting_RIDkey1']],
                  ),
    em.Key.define(
        ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_2dem_class_average_fitting_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_2dem_class_average_fitting_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_2dem_class_average_fitting_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_2dem_class_average_fitting_structure_id_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'model_id'],
        'PDB',
        'ihm_model_list', ['structure_id', 'model_id'],
        constraint_names=[['PDB', 'ihm_2dem_class_average_fitting_model_id_fkey']],
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
        'ihm_2dem_class_average_restraint', ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_2dem_class_average_fitting_restraint_id_fkey']],
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
