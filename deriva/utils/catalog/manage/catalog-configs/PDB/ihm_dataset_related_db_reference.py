import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_dataset_related_db_reference'

schema_name = 'PDB'

column_annotations = {
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'version': {},
    'accession_code': {},
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
    'Owner': {},
    'details': {},
    'db_name': {},
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    },
    'id': {}
}

column_comment = {
    'dataset_list_id': 'A reference to table ihm_dataset_list.id.',
    'Owner': 'Group that can update the record.',
    'details': 'type:text\nDetails regarding the dataset entry.\nexamples:Structural Analysis of a Prokaryotic Ribosome Using a Novel Amidinating Cross-Linker and Mass Spectrometry',
    'id': 'type:int4\nA unique identifier for the related database entry.',
    'version': 'type:text\nVersion of the database entry, if the database allows versioning.',
    'accession_code': 'type:text\nThe accession code for the database entry.\nexamples:5FM1,25766,EMD-2799,10049,SASDA82,PXD003381,MA-CO2KC',
    'structure_id': 'A reference to table entry.id.',
    'db_name': 'type:text\nThe name of the database containing the dataset entry.'
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
        'accession_code',
        em.builtin_types['text'],
        nullok=False,
        comment=column_comment['accession_code'],
    ),
    em.Column.define(
        'dataset_list_id',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['dataset_list_id'],
    ),
    em.Column.define(
        'db_name', em.builtin_types['text'], nullok=False, comment=column_comment['db_name'],
    ),
    em.Column.define('details', em.builtin_types['text'], comment=column_comment['details'],
                     ),
    em.Column.define('id', em.builtin_types['int4'], nullok=False, comment=column_comment['id'],
                     ),
    em.Column.define('version', em.builtin_types['text'], comment=column_comment['version'],
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
                    'outbound': ['PDB', 'ihm_dataset_related_db_reference_structure_id_fkey']
                }, 'RID'
            ]
        }, 'accession_code', {
            'markdown_name': 'dataset list id',
            'comment': 'A reference to table ihm_dataset_list.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_dataset_related_db_reference_dataset_list_id_fkey']
                }, 'RID'
            ]
        }, ['PDB', 'ihm_dataset_related_db_reference_db_name_term_fkey'], 'details', 'id', 'version'
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_dataset_related_db_reference_structure_id_fkey']
                }, 'RID'
            ]
        }, 'accession_code', {
            'markdown_name': 'dataset list id',
            'comment': 'A reference to table ihm_dataset_list.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_dataset_related_db_reference_dataset_list_id_fkey']
                }, 'RID'
            ]
        }, ['PDB', 'ihm_dataset_related_db_reference_db_name_term_fkey'], 'details', 'id',
        'version', ['PDB', 'ihm_dataset_related_db_reference_RCB_fkey'],
        ['PDB', 'ihm_dataset_related_db_reference_RMB_fkey'], 'RCT', 'RMT',
        ['PDB', 'ihm_dataset_related_db_reference_Owner_fkey']
    ]
}

table_annotations = {chaise_tags.visible_columns: visible_columns, }

table_comment = None

table_acls = {}

table_acl_bindings = {}

key_defs = [
    em.Key.define(
        ['RID'], constraint_names=[['PDB', 'ihm_dataset_related_db_reference_RIDkey1']],
    ),
    em.Key.define(
        ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_dataset_related_db_reference_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['db_name'],
        'Vocab',
        'ihm_dataset_related_db_reference_db_name_term', ['ID'],
        constraint_names=[['PDB', 'ihm_dataset_related_db_reference_db_name_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_dataset_related_db_reference_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_dataset_related_db_reference_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_dataset_related_db_reference_structure_id_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['dataset_list_id', 'structure_id'],
        'PDB',
        'ihm_dataset_list', ['id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_dataset_related_db_reference_dataset_list_id_fkey']],
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
