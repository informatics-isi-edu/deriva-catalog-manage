import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_geometric_object_distance_restraint'

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
    'object_id': {},
    'distance_probability': {},
    'feature_id': {},
    'distance_lower_limit_esd': {},
    'object_characteristic': {},
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
    'dataset_list_id': {},
    'harmonic_force_constant': {},
    'details': {},
    'id': {},
    'distance_upper_limit_esd': {},
    'group_conditionality': {},
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    },
    'restraint_type': {},
    'distance_lower_limit': {},
    'distance_upper_limit': {}
}

column_comment = {
    'Owner': 'Group that can update the record.',
    'object_id': 'A reference to table ihm_geometric_object_list.object_id.',
    'distance_probability': 'type:float4\nThe real number that indicates the probability that the distance restraint\n is correct. This number should fall between 0.0 and 1.0.',
    'distance_lower_limit_esd': 'type:float4\nThe estimated standard deviation of the lower limit distance threshold, if applicable.',
    'object_characteristic': 'type:text\nThe characteristic of the geometric object used in the restraint.',
    'feature_id': 'A reference to table ihm_feature_list.feature_id.',
    'structure_id': 'A reference to table entry.id.',
    'dataset_list_id': 'A reference to table ihm_dataset_list.id.',
    'harmonic_force_constant': 'type:float4\nThe harmonic force constant, if applicable.',
    'details': 'type:text\nAdditional details about the geometric object distance restraints, \n especially if _ihm_geometric_object_distance_restraint.restraint_type or\n _ihm_geometric_object_distance_restraint.object_characteristic is "other".',
    'id': 'type:int4\nA unique id for the geometric object distance restraint.',
    'distance_upper_limit_esd': 'type:float4\nThe estimated standard deviation of the upper limit distance threshold, if applicable.',
    'group_conditionality': 'type:text\nIf a group of atoms or residues are restrained, this data item defines\n the conditionality based on which the restraint is applied in the modeling.',
    'restraint_type': 'type:text\nThe type of restraint applied.',
    'distance_lower_limit': 'type:float4\nThe lower limit to the distance threshold, if applicable.',
    'distance_upper_limit': 'type:float4\nThe upper limit to the distance threshold, if applicable.'
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
        'dataset_list_id', em.builtin_types['int4'], comment=column_comment['dataset_list_id'],
    ),
    em.Column.define('details', em.builtin_types['text'], comment=column_comment['details'],
                     ),
    em.Column.define(
        'distance_lower_limit',
        em.builtin_types['float4'],
        comment=column_comment['distance_lower_limit'],
    ),
    em.Column.define(
        'distance_lower_limit_esd',
        em.builtin_types['float4'],
        comment=column_comment['distance_lower_limit_esd'],
    ),
    em.Column.define(
        'distance_probability',
        em.builtin_types['float4'],
        comment=column_comment['distance_probability'],
    ),
    em.Column.define(
        'distance_upper_limit',
        em.builtin_types['float4'],
        comment=column_comment['distance_upper_limit'],
    ),
    em.Column.define(
        'distance_upper_limit_esd',
        em.builtin_types['float4'],
        comment=column_comment['distance_upper_limit_esd'],
    ),
    em.Column.define(
        'feature_id',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['feature_id'],
    ),
    em.Column.define(
        'group_conditionality',
        em.builtin_types['text'],
        comment=column_comment['group_conditionality'],
    ),
    em.Column.define(
        'harmonic_force_constant',
        em.builtin_types['float4'],
        comment=column_comment['harmonic_force_constant'],
    ),
    em.Column.define('id', em.builtin_types['int4'], nullok=False, comment=column_comment['id'],
                     ),
    em.Column.define(
        'object_characteristic',
        em.builtin_types['text'],
        comment=column_comment['object_characteristic'],
    ),
    em.Column.define(
        'object_id', em.builtin_types['int4'], nullok=False, comment=column_comment['object_id'],
    ),
    em.Column.define(
        'restraint_type',
        em.builtin_types['text'],
        nullok=False,
        comment=column_comment['restraint_type'],
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
                    'outbound': [
                        'PDB', 'ihm_geometric_object_distance_restraint_structure_id_fkey'
                    ]
                }, 'RID'
            ]
        }, {
            'markdown_name': 'dataset list id',
            'comment': 'A reference to table ihm_dataset_list.id.',
            'source': [
                {
                    'outbound': [
                        'PDB', 'ihm_geometric_object_distance_restraint_dataset_list_id_fkey'
                    ]
                }, 'RID'
            ]
        }, 'details', 'distance_lower_limit', 'distance_lower_limit_esd', 'distance_probability',
        'distance_upper_limit', 'distance_upper_limit_esd', {
            'markdown_name': 'feature id',
            'comment': 'A reference to table ihm_feature_list.feature_id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_geometric_object_distance_restraint_feature_id_fkey']
                }, 'RID'
            ]
        }, ['PDB', 'bject_distance_restraint_group_conditionality_term_fkey'],
        'harmonic_force_constant', 'id',
        ['PDB', 'ject_distance_restraint_object_characteristic_term_fkey'], {
            'markdown_name': 'object id',
            'comment': 'A reference to table ihm_geometric_object_list.object_id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_geometric_object_distance_restraint_object_id_fkey']
                }, 'RID'
            ]
        }, ['PDB', 'tric_object_distance_restraint_restraint_type_term_fkey']
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [
                {
                    'outbound': [
                        'PDB', 'ihm_geometric_object_distance_restraint_structure_id_fkey'
                    ]
                }, 'RID'
            ]
        }, {
            'markdown_name': 'dataset list id',
            'comment': 'A reference to table ihm_dataset_list.id.',
            'source': [
                {
                    'outbound': [
                        'PDB', 'ihm_geometric_object_distance_restraint_dataset_list_id_fkey'
                    ]
                }, 'RID'
            ]
        }, 'details', 'distance_lower_limit', 'distance_lower_limit_esd', 'distance_probability',
        'distance_upper_limit', 'distance_upper_limit_esd', {
            'markdown_name': 'feature id',
            'comment': 'A reference to table ihm_feature_list.feature_id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_geometric_object_distance_restraint_feature_id_fkey']
                }, 'RID'
            ]
        }, ['PDB', 'bject_distance_restraint_group_conditionality_term_fkey'],
        'harmonic_force_constant', 'id',
        ['PDB', 'ject_distance_restraint_object_characteristic_term_fkey'], {
            'markdown_name': 'object id',
            'comment': 'A reference to table ihm_geometric_object_list.object_id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_geometric_object_distance_restraint_object_id_fkey']
                }, 'RID'
            ]
        }, ['PDB', 'tric_object_distance_restraint_restraint_type_term_fkey'],
        ['PDB', 'ihm_geometric_object_distance_restraint_RCB_fkey'],
        ['PDB', 'ihm_geometric_object_distance_restraint_RMB_fkey'], 'RCT', 'RMT',
        ['PDB', 'ihm_geometric_object_distance_restraint_Owner_fkey']
    ]
}

table_annotations = {chaise_tags.visible_columns: visible_columns, }

table_comment = None

table_acls = {}

table_acl_bindings = {}

key_defs = [
    em.Key.define(
        ['RID'], constraint_names=[['PDB', 'ihm_geometric_object_distance_restraint_RIDkey1']],
    ),
    em.Key.define(
        ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_geometric_object_distance_restraint_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['group_conditionality'],
        'Vocab',
        'bject_distance_restraint_group_conditionality_term', ['ID'],
        constraint_names=[['PDB', 'bject_distance_restraint_group_conditionality_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['object_characteristic'],
        'Vocab',
        'ject_distance_restraint_object_characteristic_term', ['ID'],
        constraint_names=[['PDB', 'ject_distance_restraint_object_characteristic_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['restraint_type'],
        'Vocab',
        'tric_object_distance_restraint_restraint_type_term', ['ID'],
        constraint_names=[['PDB', 'tric_object_distance_restraint_restraint_type_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_geometric_object_distance_restraint_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_geometric_object_distance_restraint_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_geometric_object_distance_restraint_structure_id_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'dataset_list_id'],
        'PDB',
        'ihm_dataset_list', ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_geometric_object_distance_restraint_dataset_list_id_fkey']],
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
        constraint_names=[['PDB', 'ihm_geometric_object_distance_restraint_feature_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'object_id'],
        'PDB',
        'ihm_geometric_object_list', ['structure_id', 'object_id'],
        constraint_names=[['PDB', 'ihm_geometric_object_distance_restraint_object_id_fkey']],
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
