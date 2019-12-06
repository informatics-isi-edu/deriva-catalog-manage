import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_entity_poly_segment'

schema_name = 'PDB'

column_annotations = {
    'seq_id_begin': {},
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
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
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    },
    'structure_id': {},
    'Owner': {},
    'entity_id': {},
    'seq_id_end': {},
    'comp_id_begin': {},
    'comp_id_end': {},
    'id': {}
}

column_comment = {
    'seq_id_begin': 'A reference to table entity_poly_seq.num.',
    'seq_id_end': 'A reference to table entity_poly_seq.num.',
    'Owner': 'Group that can update the record.',
    'entity_id': 'A reference to table entity.id.',
    'comp_id_begin': 'A reference to table chem_comp.id.',
    'structure_id': 'A reference to table entry.id.',
    'id': 'type:int4\nA unique identifier for the polymeric segment.',
    'comp_id_end': 'A reference to table chem_comp.id.'
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
        'comp_id_begin',
        em.builtin_types['text'],
        nullok=False,
        comment=column_comment['comp_id_begin'],
    ),
    em.Column.define(
        'comp_id_end',
        em.builtin_types['text'],
        nullok=False,
        comment=column_comment['comp_id_end'],
    ),
    em.Column.define(
        'entity_id', em.builtin_types['text'], nullok=False, comment=column_comment['entity_id'],
    ),
    em.Column.define('id', em.builtin_types['int4'], nullok=False, comment=column_comment['id'],
                     ),
    em.Column.define(
        'seq_id_begin',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['seq_id_begin'],
    ),
    em.Column.define(
        'seq_id_end',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['seq_id_end'],
    ),
    em.Column.define('Owner', em.builtin_types['text'], comment=column_comment['Owner'],
                     ),
]

table_display = {'row_name': {'row_markdown_pattern': '{{{id}}}'}}

visible_foreign_keys = {
    'detailed': [
        ['PDB', 'ihm_model_representation_details_entity_poly_segment_id_fkey'],
        ['PDB', 'ihm_struct_assembly_details_entity_poly_segment_id_fkey'],
        ['PDB', 'ihm_starting_model_details_entity_poly_segment_id_fkey'],
        ['PDB', 'ihm_localization_density_files_entity_poly_segment_id_fkey']
    ],
    'filter': 'detailed'
}

visible_columns = {
    '*': [
        {
            'source': 'RID'
        }, {
            'source': [{
                'outbound': ['PDB', 'ihm_entity_poly_segment_structure_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'comp id begin',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_entity_poly_segment_mm_poly_res_label_begin_fkey']
                }, 'mon_id'
            ]
        }, {
            'markdown_name': 'comp id end',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_entity_poly_segment_mm_poly_res_label_end_fkey']
                }, 'mon_id'
            ]
        }, {
            'markdown_name': 'entity id',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_entity_poly_segment_mm_poly_res_label_begin_fkey']
                }, 'entity_id'
            ]
        }, {
            'source': 'id'
        }, {
            'markdown_name': 'seq id begin',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_entity_poly_segment_mm_poly_res_label_begin_fkey']
                }, 'num'
            ]
        }, {
            'markdown_name': 'seq id end',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_entity_poly_segment_mm_poly_res_label_end_fkey']
                }, 'num'
            ]
        }, {
            'markdown_name': 'molecular entity begin',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_entity_poly_segment_mm_poly_res_label_begin_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'molecular entity end',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_entity_poly_segment_mm_poly_res_label_end_fkey']
                }, 'RID'
            ]
        }, {
            'source': 'RCT'
        }, {
            'source': 'RMT'
        }, {
            'source': [{
                'outbound': ['PDB', 'ihm_entity_poly_segment_RCB_fkey']
            }, 'RID']
        }, {
            'source': [{
                'outbound': ['PDB', 'ihm_entity_poly_segment_RMB_fkey']
            }, 'RID']
        }, {
            'source': [{
                'outbound': ['PDB', 'ihm_entity_poly_segment_Owner_fkey']
            }, 'RID']
        }
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
    em.Key.define(['RID'], constraint_names=[['PDB', 'ihm_entity_poly_segment_RIDkey1']],
                  ),
    em.Key.define(
        ['id', 'structure_id'], constraint_names=[['PDB', 'ihm_entity_poly_segment_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_entity_poly_segment_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_entity_poly_segment_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_entity_poly_segment_structure_id_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['comp_id_begin', 'structure_id', 'seq_id_begin', 'entity_id'],
        'PDB',
        'entity_poly_seq', ['mon_id', 'structure_id', 'num', 'entity_id'],
        constraint_names=[['PDB', 'ihm_entity_poly_segment_mm_poly_res_label_begin_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['entity_id', 'comp_id_end', 'structure_id', 'seq_id_end'],
        'PDB',
        'entity_poly_seq', ['entity_id', 'mon_id', 'structure_id', 'num'],
        constraint_names=[['PDB', 'ihm_entity_poly_segment_mm_poly_res_label_end_fkey']],
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
