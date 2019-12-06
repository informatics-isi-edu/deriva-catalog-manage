import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_struct_assembly'

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
    'id': {},
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
    'description': {},
    'structure_id': {}
}

column_comment = {
    'name': 'type:text\nA name for the structural assembly.',
    'Owner': 'Group that can update the record.',
    'structure_id': 'A reference to table entry.id.',
    'description': 'type:text\nDescription of the structural assembly.',
    'id': 'type:int4\nA unique identifier for the structural assembly.'
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
        'description', em.builtin_types['text'], comment=column_comment['description'],
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
        ['PDB', 'ihm_struct_assembly_details_assembly_id_fkey'],
        ['PDB', 'ihm_struct_assembly_details_parent_assembly_id_fkey'],
        ['PDB', 'ihm_struct_assembly_class_link_assembly_id_fkey'],
        ['PDB', 'ihm_2dem_class_average_restraint_struct_assembly_id_fkey'],
        ['PDB', 'ihm_3dem_restraint_struct_assembly_id_fkey'],
        ['PDB', 'ihm_sas_restraint_struct_assembly_id_fkey'],
        ['PDB', 'ihm_modeling_protocol_details_struct_assembly_id_fkey'],
        ['PDB', 'ihm_modeling_post_process_struct_assembly_id_fkey'],
        ['PDB', 'ihm_model_list_assembly_id_fkey']
    ],
    'filter': 'detailed'
}

visible_columns = {
    'entry': [
        {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_struct_assembly_structure_id_fkey']
            }, 'RID']
        }, 'description', 'id', 'name'
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_struct_assembly_structure_id_fkey']
            }, 'RID']
        }, 'description', 'id', 'name', ['PDB', 'ihm_struct_assembly_RCB_fkey'],
        ['PDB', 'ihm_struct_assembly_RMB_fkey'], 'RCT', 'RMT',
        ['PDB', 'ihm_struct_assembly_Owner_fkey']
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
    em.Key.define(['RID'], constraint_names=[['PDB', 'ihm_struct_assembly_RIDkey1']],
                  ),
    em.Key.define(
        ['id', 'structure_id'], constraint_names=[['PDB', 'ihm_struct_assembly_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_struct_assembly_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_struct_assembly_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_struct_assembly_structure_id_fkey']],
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
