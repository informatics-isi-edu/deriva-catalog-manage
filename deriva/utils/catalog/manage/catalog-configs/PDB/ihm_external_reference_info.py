import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_external_reference_info'

schema_name = 'PDB'

column_annotations = {
    'reference_provider': {},
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'refers_to': {},
    'reference': {},
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
    'associated_url': {},
    'Owner': {},
    'reference_id': {},
    'details': {},
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    },
    'reference_type': {}
}

column_comment = {
    'reference_provider': 'type:text\nThe name of the reference provider.\nexamples:Zenodo,Figshare,Crossref',
    'Owner': 'Group that can update the record.',
    'reference_id': 'type:int4\nA unique identifier for the external reference.',
    'details': 'type:text\nAdditional details regarding the external reference.',
    'refers_to': 'type:text\nThe type of object that the external reference points to, usually\n a single file or an archive.',
    'reference': 'type:text\nThe external reference or the Digital Object Identifier (DOI).\n This field is not relevant for local files.\nexamples:10.5281/zenodo.46266',
    'reference_type': 'type:text\nThe type of external reference. \n Currently, only Digital Object Identifiers (DOIs) and supplementary files \n stored locally are supported.',
    'structure_id': 'A reference to table entry.id.',
    'associated_url': 'type:text\nThe Uniform Resource Locator (URL) corresponding to the external reference (DOI). \n This URL should link to the corresponding downloadable file or archive and is provided \n to enable automated software to download the referenced file or archive.'
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
        'associated_url', em.builtin_types['text'], comment=column_comment['associated_url'],
    ),
    em.Column.define('details', em.builtin_types['text'], comment=column_comment['details'],
                     ),
    em.Column.define(
        'reference', em.builtin_types['text'], nullok=False, comment=column_comment['reference'],
    ),
    em.Column.define(
        'reference_id',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['reference_id'],
    ),
    em.Column.define(
        'reference_provider',
        em.builtin_types['text'],
        comment=column_comment['reference_provider'],
    ),
    em.Column.define(
        'reference_type',
        em.builtin_types['text'],
        nullok=False,
        comment=column_comment['reference_type'],
    ),
    em.Column.define(
        'refers_to', em.builtin_types['text'], nullok=False, comment=column_comment['refers_to'],
    ),
    em.Column.define('Owner', em.builtin_types['text'], comment=column_comment['Owner'],
                     ),
]

table_display = {'row_name': {'row_markdown_pattern': '{{{reference_id}}}'}}

visible_foreign_keys = {
    'detailed': [['PDB', 'ihm_external_files_reference_id_fkey']],
    'filter': 'detailed'
}

visible_columns = {
    'entry': [
        {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_external_reference_info_structure_id_fkey']
                }, 'RID'
            ]
        }, 'associated_url', 'details', 'reference', 'reference_id', 'reference_provider',
        ['PDB', 'ihm_external_reference_info_reference_type_term_fkey'],
        ['PDB', 'ihm_external_reference_info_refers_to_term_fkey']
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_external_reference_info_structure_id_fkey']
                }, 'RID'
            ]
        }, 'associated_url', 'details', 'reference', 'reference_id', 'reference_provider',
        ['PDB', 'ihm_external_reference_info_reference_type_term_fkey'],
        ['PDB', 'ihm_external_reference_info_refers_to_term_fkey'],
        ['PDB', 'ihm_external_reference_info_RCB_fkey'],
        ['PDB', 'ihm_external_reference_info_RMB_fkey'], 'RCT', 'RMT',
        ['PDB', 'ihm_external_reference_info_Owner_fkey']
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
    em.Key.define(['RID'], constraint_names=[['PDB', 'ihm_external_reference_info_RIDkey1']],
                  ),
    em.Key.define(
        ['structure_id', 'reference_id'],
        constraint_names=[['PDB', 'ihm_external_reference_info_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['reference_type'],
        'Vocab',
        'ihm_external_reference_info_reference_type_term', ['ID'],
        constraint_names=[['PDB', 'ihm_external_reference_info_reference_type_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['refers_to'],
        'Vocab',
        'ihm_external_reference_info_refers_to_term', ['ID'],
        constraint_names=[['PDB', 'ihm_external_reference_info_refers_to_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_external_reference_info_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_external_reference_info_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_external_reference_info_structure_id_fkey']],
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
