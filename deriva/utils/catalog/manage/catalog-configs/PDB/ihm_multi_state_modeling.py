import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_multi_state_modeling'

schema_name = 'PDB'

column_annotations = {
    'population_fraction': {},
    'state_id': {},
    'state_group_id': {},
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'state_name': {},
    'Owner': {},
    'structure_id': {},
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
    'population_fraction_sd': {},
    'state_type': {},
    'details': {},
    'experiment_type': {},
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    }
}

column_comment = {
    'population_fraction': 'type:float4\nA fraction representing the population of the particular state.',
    'state_id': 'type:int4\nA unique identifier for a particular state in the multi-state modeling.',
    'state_group_id': 'type:int4\nAn identifier for a collections of states in the multi-state modeling.\n This data item can be used when structural models belong to diffent\n multi-state modeling types.',
    'details': 'type:text\nAdditional textual details of the multi-state modeling, if required.\nexamples:open state ensemble 1,closed state ensemble 2,bound to heme',
    'state_name': 'type:text\nA descriptive name for the state.\nexamples:open,closed,bound,unbound,active,inactive,relaxed,tensed',
    'Owner': 'Group that can update the record.',
    'experiment_type': 'type:text\nThe type of multi-state modeling experiment carried out.',
    'state_type': 'type:text\nThe type that the multiple states being modeled belong to.\nexamples:conformational change,ligand binding,complex formation,complex dissociation',
    'structure_id': 'A reference to table entry.id.',
    'population_fraction_sd': 'type:float4\nThe standard deviation of the population fraction.'
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
        'experiment_type', em.builtin_types['text'], comment=column_comment['experiment_type'],
    ),
    em.Column.define(
        'population_fraction',
        em.builtin_types['float4'],
        comment=column_comment['population_fraction'],
    ),
    em.Column.define(
        'population_fraction_sd',
        em.builtin_types['float4'],
        comment=column_comment['population_fraction_sd'],
    ),
    em.Column.define(
        'state_group_id', em.builtin_types['int4'], comment=column_comment['state_group_id'],
    ),
    em.Column.define(
        'state_id', em.builtin_types['int4'], nullok=False, comment=column_comment['state_id'],
    ),
    em.Column.define(
        'state_name', em.builtin_types['text'], comment=column_comment['state_name'],
    ),
    em.Column.define(
        'state_type', em.builtin_types['text'], comment=column_comment['state_type'],
    ),
    em.Column.define('Owner', em.builtin_types['text'], comment=column_comment['Owner'],
                     ),
]

table_display = {'row_name': {'row_markdown_pattern': '{{{state_id}}}'}}

visible_foreign_keys = {
    'detailed': [['PDB', 'ihm_multi_state_model_group_link_state_id_fkey']],
    'filter': 'detailed'
}

visible_columns = {
    'entry': [
        {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_multi_state_modeling_structure_id_fkey']
            }, 'RID']
        }, 'details', ['PDB',
                       'ihm_multi_state_modeling_experiment_type_term_fkey'], 'population_fraction',
        'population_fraction_sd', 'state_group_id', 'state_id', 'state_name', 'state_type'
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_multi_state_modeling_structure_id_fkey']
            }, 'RID']
        }, 'details', ['PDB', 'ihm_multi_state_modeling_experiment_type_term_fkey'],
        'population_fraction', 'population_fraction_sd', 'state_group_id', 'state_id', 'state_name',
        'state_type', ['PDB', 'ihm_multi_state_modeling_RCB_fkey'],
        ['PDB', 'ihm_multi_state_modeling_RMB_fkey'], 'RCT', 'RMT',
        ['PDB', 'ihm_multi_state_modeling_Owner_fkey']
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
    em.Key.define(['RID'], constraint_names=[['PDB', 'ihm_multi_state_modeling_RIDkey1']],
                  ),
    em.Key.define(
        ['structure_id', 'state_id'],
        constraint_names=[['PDB', 'ihm_multi_state_modeling_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['experiment_type'],
        'Vocab',
        'ihm_multi_state_modeling_experiment_type_term', ['ID'],
        constraint_names=[['PDB', 'ihm_multi_state_modeling_experiment_type_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_multi_state_modeling_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_multi_state_modeling_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_multi_state_modeling_structure_id_fkey']],
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
