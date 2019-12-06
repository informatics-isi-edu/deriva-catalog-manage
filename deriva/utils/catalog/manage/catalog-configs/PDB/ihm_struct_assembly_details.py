import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_struct_assembly_details'

schema_name = 'PDB'

column_annotations = {
    'entity_description': {},
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'assembly_id': {},
    'parent_assembly_id': {},
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
    'Owner': {},
    'asym_id': {},
    'entity_id': {},
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    },
    'entity_poly_segment_id': {},
    'id': {}
}

column_comment = {
    'entity_description': 'type:text\nA text description of the molecular entity',
    'parent_assembly_id': 'A reference to table ihm_struct_assembly.id.',
    'assembly_id': 'A reference to table ihm_struct_assembly.id.',
    'asym_id': 'A reference to table struct_asym.id.',
    'entity_id': 'A reference to table entity.id.',
    'entity_poly_segment_id': 'A reference to table ihm_entity_poly_segment.id.',
    'Owner': 'Group that can update the record.',
    'structure_id': 'A reference to table entry.id.',
    'id': 'type:int4\nA unique identifier for the structural assembly description.'
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
        'asym_id', em.builtin_types['text'], nullok=False, comment=column_comment['asym_id'],
    ),
    em.Column.define(
        'entity_description',
        em.builtin_types['text'],
        comment=column_comment['entity_description'],
    ),
    em.Column.define(
        'entity_id', em.builtin_types['text'], nullok=False, comment=column_comment['entity_id'],
    ),
    em.Column.define(
        'entity_poly_segment_id',
        em.builtin_types['int4'],
        comment=column_comment['entity_poly_segment_id'],
    ),
    em.Column.define('id', em.builtin_types['int4'], nullok=False, comment=column_comment['id'],
                     ),
    em.Column.define(
        'parent_assembly_id',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['parent_assembly_id'],
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
                    'outbound': ['PDB', 'ihm_struct_assembly_details_structure_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'assembly id',
            'comment': 'A reference to table ihm_struct_assembly.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_struct_assembly_details_assembly_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'asym id',
            'comment': 'A reference to table struct_asym.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_struct_assembly_details_asym_id_fkey']
            }, 'RID']
        }, 'entity_description', {
            'markdown_name': 'entity id',
            'comment': 'A reference to table entity.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_struct_assembly_details_entity_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'entity poly segment id',
            'comment': 'A reference to table ihm_entity_poly_segment.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_struct_assembly_details_entity_poly_segment_id_fkey']
                }, 'RID'
            ]
        }, 'id', {
            'markdown_name': 'parent assembly id',
            'comment': 'A reference to table ihm_struct_assembly.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_struct_assembly_details_parent_assembly_id_fkey']
                }, 'RID'
            ]
        }
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_struct_assembly_details_structure_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'assembly id',
            'comment': 'A reference to table ihm_struct_assembly.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_struct_assembly_details_assembly_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'asym id',
            'comment': 'A reference to table struct_asym.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_struct_assembly_details_asym_id_fkey']
            }, 'RID']
        }, 'entity_description', {
            'markdown_name': 'entity id',
            'comment': 'A reference to table entity.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_struct_assembly_details_entity_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'entity poly segment id',
            'comment': 'A reference to table ihm_entity_poly_segment.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_struct_assembly_details_entity_poly_segment_id_fkey']
                }, 'RID'
            ]
        }, 'id', {
            'markdown_name': 'parent assembly id',
            'comment': 'A reference to table ihm_struct_assembly.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_struct_assembly_details_parent_assembly_id_fkey']
                }, 'RID'
            ]
        }, ['PDB', 'ihm_struct_assembly_details_RCB_fkey'],
        ['PDB', 'ihm_struct_assembly_details_RMB_fkey'], 'RCT', 'RMT',
        ['PDB', 'ihm_struct_assembly_details_Owner_fkey']
    ]
}

table_annotations = {chaise_tags.visible_columns: visible_columns, }

table_comment = None

table_acls = {}

table_acl_bindings = {}

key_defs = [
    em.Key.define(['RID'], constraint_names=[['PDB', 'ihm_struct_assembly_details_RIDkey1']],
                  ),
    em.Key.define(
        ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_struct_assembly_details_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_struct_assembly_details_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_struct_assembly_details_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_struct_assembly_details_structure_id_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['assembly_id', 'structure_id'],
        'PDB',
        'ihm_struct_assembly', ['id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_struct_assembly_details_assembly_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['asym_id', 'structure_id'],
        'PDB',
        'struct_asym', ['id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_struct_assembly_details_asym_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'entity_id'],
        'PDB',
        'entity', ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_struct_assembly_details_entity_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['entity_poly_segment_id', 'structure_id'],
        'PDB',
        'ihm_entity_poly_segment', ['id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_struct_assembly_details_entity_poly_segment_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['parent_assembly_id', 'structure_id'],
        'PDB',
        'ihm_struct_assembly', ['id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_struct_assembly_details_parent_assembly_id_fkey']],
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
