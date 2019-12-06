import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_model_group'

schema_name = 'PDB'

column_annotations = {
    'name': {},
    'Owner': {},
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'details': {},
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
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
    'id': {},
    'structure_id': {}
}

column_comment = {
    'name': 'type:text\nA name for the collection of models.\nexamples:cluster1,cluster2,ensemble1,ensemble2,open state,closed state,bound state,unbound state,bound state ensemble 1,unbound state ensemble 2',
    'Owner': 'Group that can update the record.',
    'details': 'type:text\nAdditional details about the collection of models.',
    'structure_id': 'A reference to table entry.id.',
    'id': 'type:int4\nA unique identifier for a collection or group of structural models. \n This data item can be used to group models into structural clusters\n or using other criteria based on experimental data or other\n relationships such as those belonging to the same state or time stamp.\n An ensemble of models and its representative can either be grouped together\n or can be separate groups in the ihm_model_group table. The choice between\n the two options should be decided based on how the modeling was carried out\n and how the representative was chosen. If the representative is a member of\n the ensemble (i.e., best scoring model), then it is recommended that the\n representative and the ensemble belong to the same model group. If the\n representative is calculated from the ensemble (i.e., centroid), then it is\n recommended that the representative be separated into a different group.'
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
    em.Column.define('id', em.builtin_types['int4'], nullok=False, comment=column_comment['id'],
                     ),
    em.Column.define('name', em.builtin_types['text'], comment=column_comment['name'],
                     ),
    em.Column.define('Owner', em.builtin_types['text'], comment=column_comment['Owner'],
                     ),
]

table_display = {'row_name': {'row_markdown_pattern': '{{{id}}}'}}

visible_foreign_keys = {
    'detailed': [
        ['PDB', 'ihm_model_group_link_group_id_fkey'],
        ['PDB', 'ihm_model_representative_model_group_id_fkey'],
        ['PDB', 'ihm_multi_state_model_group_link_model_group_id_fkey'],
        ['PDB', 'ihm_ensemble_info_model_group_id_fkey'],
        ['PDB', 'ihm_ordered_ensemble_model_group_id_begin_fkey'],
        ['PDB', 'ihm_ordered_ensemble_model_group_id_end_fkey']
    ],
    'filter': 'detailed'
}

visible_columns = {
    'entry': [
        {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_model_group_structure_id_fkey']
            }, 'RID']
        }, 'details', 'id', 'name'
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_model_group_structure_id_fkey']
            }, 'RID']
        }, 'details', 'id', 'name', ['PDB', 'ihm_model_group_RCB_fkey'],
        ['PDB', 'ihm_model_group_RMB_fkey'], 'RCT', 'RMT', ['PDB', 'ihm_model_group_Owner_fkey']
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
    em.Key.define(['RID'], constraint_names=[['PDB', 'ihm_model_group_RIDkey1']],
                  ),
    em.Key.define(
        ['id', 'structure_id'], constraint_names=[['PDB', 'ihm_model_group_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_model_group_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_model_group_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_model_group_structure_id_fkey']],
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
