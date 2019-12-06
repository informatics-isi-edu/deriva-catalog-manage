import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_model_list'

schema_name = 'PDB'

column_annotations = {
    'model_id': {},
    'Owner': {},
    'representation_id': {},
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'assembly_id': {},
    'protocol_id': {},
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    },
    'model_name': {},
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
    'model_id': 'type:int4\nA unique identifier for the structural model being deposited.',
    'Owner': 'Group that can update the record.',
    'representation_id': 'A reference to table ihm_model_representation.id.',
    'assembly_id': 'A reference to table ihm_struct_assembly.id.',
    'protocol_id': 'A reference to table ihm_modeling_protocol.id.',
    'model_name': 'type:text\nA decsriptive name for the model.\nexamples:Best scoring model,2nd best scoring model,Cluster center',
    'structure_id': 'A reference to table entry.id.'
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
        'assembly_id',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['assembly_id'],
    ),
    em.Column.define(
        'model_id', em.builtin_types['int4'], nullok=False, comment=column_comment['model_id'],
    ),
    em.Column.define(
        'model_name', em.builtin_types['text'], comment=column_comment['model_name'],
    ),
    em.Column.define(
        'protocol_id',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['protocol_id'],
    ),
    em.Column.define(
        'representation_id',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['representation_id'],
    ),
    em.Column.define('Owner', em.builtin_types['text'], comment=column_comment['Owner'],
                     ),
]

table_display = {'row_name': {'row_markdown_pattern': '{{{model_id}}}'}}

visible_foreign_keys = {
    'detailed': [
        ['PDB', 'ihm_cross_link_result_parameters_model_id_fkey'],
        ['PDB', 'ihm_2dem_class_average_fitting_model_id_fkey'],
        ['PDB', 'ihm_3dem_restraint_model_id_fkey'], ['PDB', 'ihm_sas_restraint_model_id_fkey'],
        ['PDB', 'ihm_epr_restraint_model_id_fkey'], ['PDB', 'ihm_model_group_link_model_id_fkey'],
        ['PDB', 'ihm_model_representative_model_id_fkey'],
        ['PDB', 'ihm_residues_not_modeled_model_id_fkey']
    ],
    'filter': 'detailed'
}

visible_columns = {
    'entry': [
        {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_model_list_structure_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'assembly id',
            'comment': 'A reference to table ihm_struct_assembly.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_model_list_assembly_id_fkey']
            }, 'RID']
        }, 'model_id', 'model_name', {
            'markdown_name': 'protocol id',
            'comment': 'A reference to table ihm_modeling_protocol.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_model_list_protocol_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'representation id',
            'comment': 'A reference to table ihm_model_representation.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_model_list_representation_id_fkey']
            }, 'RID']
        }
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_model_list_structure_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'assembly id',
            'comment': 'A reference to table ihm_struct_assembly.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_model_list_assembly_id_fkey']
            }, 'RID']
        }, 'model_id', 'model_name', {
            'markdown_name': 'protocol id',
            'comment': 'A reference to table ihm_modeling_protocol.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_model_list_protocol_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'representation id',
            'comment': 'A reference to table ihm_model_representation.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_model_list_representation_id_fkey']
            }, 'RID']
        }, ['PDB', 'ihm_model_list_RCB_fkey'], ['PDB', 'ihm_model_list_RMB_fkey'], 'RCT', 'RMT',
        ['PDB', 'ihm_model_list_Owner_fkey']
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
    em.Key.define(['RID'], constraint_names=[['PDB', 'ihm_model_list_RIDkey1']],
                  ),
    em.Key.define(
        ['model_id', 'structure_id'], constraint_names=[['PDB', 'ihm_model_list_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_model_list_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_model_list_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_model_list_structure_id_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['assembly_id', 'structure_id'],
        'PDB',
        'ihm_struct_assembly', ['id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_model_list_assembly_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'protocol_id'],
        'PDB',
        'ihm_modeling_protocol', ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_model_list_protocol_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'representation_id'],
        'PDB',
        'ihm_model_representation', ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_model_list_representation_id_fkey']],
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
