import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_localization_density_files'

schema_name = 'PDB'

column_annotations = {
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'file_id': {},
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
    'ensemble_id': {},
    'Owner': {},
    'asym_id': {},
    'entity_id': {},
    'entity_poly_segment_id': {},
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
    'Owner': 'Group that can update the record.',
    'asym_id': 'A reference to table struct_asym.id.',
    'file_id': 'A reference to table ihm_external_files.id.',
    'entity_id': 'A reference to table entity.id.',
    'entity_poly_segment_id': 'A reference to table ihm_entity_poly_segment.id.',
    'structure_id': 'A reference to table entry.id.',
    'id': 'type:int4\nA unique identifier.',
    'ensemble_id': 'A reference to table ihm_ensemble_info.ensemble_id.'
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
    em.Column.define('asym_id', em.builtin_types['text'], comment=column_comment['asym_id'],
                     ),
    em.Column.define(
        'ensemble_id',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['ensemble_id'],
    ),
    em.Column.define('entity_id', em.builtin_types['text'], comment=column_comment['entity_id'],
                     ),
    em.Column.define(
        'entity_poly_segment_id',
        em.builtin_types['int4'],
        comment=column_comment['entity_poly_segment_id'],
    ),
    em.Column.define(
        'file_id', em.builtin_types['int4'], nullok=False, comment=column_comment['file_id'],
    ),
    em.Column.define('id', em.builtin_types['int4'], nullok=False, comment=column_comment['id'],
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
                    'outbound': ['PDB', 'ihm_localization_density_files_structure_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'asym id',
            'comment': 'A reference to table struct_asym.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_localization_density_files_asym_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'ensemble id',
            'comment': 'A reference to table ihm_ensemble_info.ensemble_id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_localization_density_files_ensemble_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'entity id',
            'comment': 'A reference to table entity.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_localization_density_files_entity_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'entity poly segment id',
            'comment': 'A reference to table ihm_entity_poly_segment.id.',
            'source': [
                {
                    'outbound': [
                        'PDB', 'ihm_localization_density_files_entity_poly_segment_id_fkey'
                    ]
                }, 'RID'
            ]
        }, {
            'markdown_name': 'file id',
            'comment': 'A reference to table ihm_external_files.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_localization_density_files_file_id_fkey']
            }, 'RID']
        }, 'id'
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_localization_density_files_structure_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'asym id',
            'comment': 'A reference to table struct_asym.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_localization_density_files_asym_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'ensemble id',
            'comment': 'A reference to table ihm_ensemble_info.ensemble_id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_localization_density_files_ensemble_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'entity id',
            'comment': 'A reference to table entity.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_localization_density_files_entity_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'entity poly segment id',
            'comment': 'A reference to table ihm_entity_poly_segment.id.',
            'source': [
                {
                    'outbound': [
                        'PDB', 'ihm_localization_density_files_entity_poly_segment_id_fkey'
                    ]
                }, 'RID'
            ]
        }, {
            'markdown_name': 'file id',
            'comment': 'A reference to table ihm_external_files.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_localization_density_files_file_id_fkey']
            }, 'RID']
        }, 'id', ['PDB', 'ihm_localization_density_files_RCB_fkey'],
        ['PDB', 'ihm_localization_density_files_RMB_fkey'], 'RCT', 'RMT',
        ['PDB', 'ihm_localization_density_files_Owner_fkey']
    ]
}

table_annotations = {chaise_tags.visible_columns: visible_columns, }

table_comment = None

table_acls = {}

table_acl_bindings = {}

key_defs = [
    em.Key.define(['RID'], constraint_names=[['PDB', 'ihm_localization_density_files_RIDkey1']],
                  ),
    em.Key.define(
        ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_localization_density_files_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_localization_density_files_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_localization_density_files_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_localization_density_files_structure_id_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'asym_id'],
        'PDB',
        'struct_asym', ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_localization_density_files_asym_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'ensemble_id'],
        'PDB',
        'ihm_ensemble_info', ['structure_id', 'ensemble_id'],
        constraint_names=[['PDB', 'ihm_localization_density_files_ensemble_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['entity_id', 'structure_id'],
        'PDB',
        'entity', ['id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_localization_density_files_entity_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'entity_poly_segment_id'],
        'PDB',
        'ihm_entity_poly_segment', ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_localization_density_files_entity_poly_segment_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['file_id', 'structure_id'],
        'PDB',
        'ihm_external_files', ['id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_localization_density_files_file_id_fkey']],
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
