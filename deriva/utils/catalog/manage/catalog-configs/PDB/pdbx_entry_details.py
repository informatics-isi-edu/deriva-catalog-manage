import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'pdbx_entry_details'

schema_name = 'PDB'

column_annotations = {
    'sequence_details': {},
    'Owner': {},
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'entry_id': {},
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
    'structure_id': {}
}

column_comment = {
    'entry_id': 'A reference to table entry.id.',
    'sequence_details': 'type:text\nAdditional details about the sequence or sequence database correspondences\n for this entry.',
    'Owner': 'Group that can update the record.',
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
        'entry_id', em.builtin_types['text'], nullok=False, comment=column_comment['entry_id'],
    ),
    em.Column.define(
        'sequence_details', em.builtin_types['text'], comment=column_comment['sequence_details'],
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
                'outbound': ['PDB', 'pdbx_entry_details_structure_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'entry id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'pdbx_entry_details_entry_id_fkey']
            }, 'RID']
        }, 'sequence_details'
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'pdbx_entry_details_structure_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'entry id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'pdbx_entry_details_entry_id_fkey']
            }, 'RID']
        }, 'sequence_details', ['PDB', 'pdbx_entry_details_RCB_fkey'],
        ['PDB', 'pdbx_entry_details_RMB_fkey'], 'RCT', 'RMT',
        ['PDB', 'pdbx_entry_details_Owner_fkey']
    ]
}

table_annotations = {chaise_tags.visible_columns: visible_columns, }

table_comment = None

table_acls = {}

table_acl_bindings = {}

key_defs = [
    em.Key.define(['RID'], constraint_names=[['PDB', 'pdbx_entry_details_RIDkey1']],
                  ),
    em.Key.define(
        ['structure_id', 'entry_id'],
        constraint_names=[['PDB', 'pdbx_entry_details_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'pdbx_entry_details_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'pdbx_entry_details_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'pdbx_entry_details_structure_id_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'entry_id'],
        'PDB',
        'entry', ['structure_id', 'id'],
        constraint_names=[['PDB', 'pdbx_entry_details_entry_id_fkey']],
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
