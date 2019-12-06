import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_external_files'

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
    'file_size_bytes': {},
    'RMB': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Modified By'
        },
        chaise_tags.immutable: None
    },
    'file_format': {},
    'RCB': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Created By'
        },
        chaise_tags.immutable: None
    },
    'file_path': {},
    'content_type': {},
    'details': {},
    'reference_id': {},
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    },
    'structure_id': {},
    'id': {}
}

column_comment = {
    'file_path': 'type:text\nThe relative path (including filename) for each external file. \n Absolute paths (starting with "/") are not permitted. \n This is required for identifying individual files from within\n a tar-zipped archive file or for identifying supplementary local\n files organized within a directory structure.\n This data item assumes a POSIX-like directory structure or file path.\nexamples:integrativemodeling-nup84-a69f895/outputs/localization/cluster1/nup84.mrc,integrativemodeling-nup84-a69f895/scripts/MODELLER_scripts/Nup84/all_align_final2.ali,nup145.mrc,data/EDC_XL_122013.dat',
    'content_type': 'type:text\nThe type of content in the file.',
    'details': 'type:text\nAdditional textual details regarding the external file.\nexamples:Readme file,Nup84 multiple sequence alignment file,Nup84 starting comparative model file',
    'reference_id': 'A reference to table ihm_external_reference_info.reference_id.',
    'Owner': 'Group that can update the record.',
    'file_size_bytes': 'type:float4\nStorage size of the external file in bytes.',
    'structure_id': 'A reference to table entry.id.',
    'id': 'type:int4\nA unique identifier for each external file.',
    'file_format': 'type:text\nFormat of the external file.'
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
        'content_type',
        em.builtin_types['text'],
        nullok=False,
        comment=column_comment['content_type'],
    ),
    em.Column.define('details', em.builtin_types['text'], comment=column_comment['details'],
                     ),
    em.Column.define(
        'file_format', em.builtin_types['text'], comment=column_comment['file_format'],
    ),
    em.Column.define('file_path', em.builtin_types['text'], comment=column_comment['file_path'],
                     ),
    em.Column.define(
        'file_size_bytes', em.builtin_types['float4'], comment=column_comment['file_size_bytes'],
    ),
    em.Column.define('id', em.builtin_types['int4'], nullok=False, comment=column_comment['id'],
                     ),
    em.Column.define(
        'reference_id',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['reference_id'],
    ),
    em.Column.define('Owner', em.builtin_types['text'], comment=column_comment['Owner'],
                     ),
]

table_display = {'row_name': {'row_markdown_pattern': '{{{id}}}'}}

visible_foreign_keys = {
    'detailed': [
        ['PDB', 'ihm_dataset_external_reference_file_id_fkey'],
        ['PDB', 'ihm_starting_comparative_models_alignment_file_id_fkey'],
        ['PDB', 'ihm_starting_computational_models_script_file_id_fkey'],
        ['PDB', 'ihm_modeling_protocol_details_script_file_id_fkey'],
        ['PDB', 'ihm_modeling_post_process_script_file_id_fkey'],
        ['PDB', 'ihm_ensemble_info_ensemble_file_id_fkey'],
        ['PDB', 'ihm_localization_density_files_file_id_fkey']
    ],
    'filter': 'detailed'
}

visible_columns = {
    'entry': [
        {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_external_files_structure_id_fkey']
            }, 'RID']
        }, ['PDB', 'ihm_external_files_content_type_term_fkey'], 'details',
        ['PDB', 'ihm_external_files_file_format_term_fkey'], 'file_path', 'file_size_bytes', 'id', {
            'markdown_name': 'reference id',
            'comment': 'A reference to table ihm_external_reference_info.reference_id.',
            'source': [{
                'outbound': ['PDB', 'ihm_external_files_reference_id_fkey']
            }, 'RID']
        }
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_external_files_structure_id_fkey']
            }, 'RID']
        }, ['PDB', 'ihm_external_files_content_type_term_fkey'], 'details',
        ['PDB', 'ihm_external_files_file_format_term_fkey'], 'file_path', 'file_size_bytes', 'id', {
            'markdown_name': 'reference id',
            'comment': 'A reference to table ihm_external_reference_info.reference_id.',
            'source': [{
                'outbound': ['PDB', 'ihm_external_files_reference_id_fkey']
            }, 'RID']
        }, ['PDB', 'ihm_external_files_RCB_fkey'], ['PDB', 'ihm_external_files_RMB_fkey'], 'RCT',
        'RMT', ['PDB', 'ihm_external_files_Owner_fkey']
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
    em.Key.define(['RID'], constraint_names=[['PDB', 'ihm_external_files_RIDkey1']],
                  ),
    em.Key.define(
        ['structure_id', 'id'], constraint_names=[['PDB', 'ihm_external_files_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['content_type'],
        'Vocab',
        'ihm_external_files_content_type_term', ['ID'],
        constraint_names=[['PDB', 'ihm_external_files_content_type_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['file_format'],
        'Vocab',
        'ihm_external_files_file_format_term', ['ID'],
        constraint_names=[['PDB', 'ihm_external_files_file_format_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_external_files_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_external_files_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_external_files_structure_id_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['reference_id', 'structure_id'],
        'PDB',
        'ihm_external_reference_info', ['reference_id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_external_files_reference_id_fkey']],
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
