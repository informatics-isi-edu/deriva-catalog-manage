import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_hydroxyl_radical_fp_restraint'

schema_name = 'PDB'

column_annotations = {
    'seq_id': {},
    'entity_description': {},
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'Owner': {},
    'group_id': {},
    'log_pf_error': {},
    'structure_id': {},
    'RMB': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Modified By'
        },
        chaise_tags.immutable: None
    },
    'fp_rate': {},
    'RCB': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Created By'
        },
        chaise_tags.immutable: None
    },
    'dataset_list_id': {},
    'comp_id': {},
    'predicted_sasa': {},
    'software_id': {},
    'asym_id': {},
    'log_pf': {},
    'entity_id': {},
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    },
    'fp_rate_error': {},
    'id': {}
}

column_comment = {
    'seq_id': 'A reference to table entity_poly_seq.num.',
    'entity_description': 'type:text\nA text description of the molecular entity. \n',
    'Owner': 'Group that can update the record.',
    'group_id': 'type:int4\nAn identifier to group the hydroxyl radical footprinting restraints.',
    'log_pf': 'type:float4\nLog (base 10) protection factor.',
    'log_pf_error': 'type:float4\nError of Log (base 10) protection factor.',
    'fp_rate': 'type:float4\nThe footprinting rate.',
    'fp_rate_error': 'type:float4\nThe footprinting rate error.',
    'dataset_list_id': 'A reference to table ihm_dataset_list.id.',
    'comp_id': 'A reference to table chem_comp.id.',
    'predicted_sasa': 'type:float4\nThe predicted solvent accessible surface area.',
    'software_id': 'A reference to table software.pdbx_ordinal.',
    'asym_id': 'A reference to table struct_asym.id.',
    'entity_id': 'A reference to table entity.id.',
    'structure_id': 'A reference to table entry.id.',
    'id': 'type:int4\nA unique identifier for the hydroxyl radical footprinting restraint.'
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
        'asym_id', em.builtin_types['text'], nullok=False, comment=column_comment['asym_id'],
    ),
    em.Column.define(
        'comp_id', em.builtin_types['text'], nullok=False, comment=column_comment['comp_id'],
    ),
    em.Column.define(
        'dataset_list_id',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['dataset_list_id'],
    ),
    em.Column.define(
        'entity_description',
        em.builtin_types['text'],
        comment=column_comment['entity_description'],
    ),
    em.Column.define(
        'entity_id', em.builtin_types['text'], nullok=False, comment=column_comment['entity_id'],
    ),
    em.Column.define('fp_rate', em.builtin_types['float4'], comment=column_comment['fp_rate'],
                     ),
    em.Column.define(
        'fp_rate_error', em.builtin_types['float4'], comment=column_comment['fp_rate_error'],
    ),
    em.Column.define('group_id', em.builtin_types['int4'], comment=column_comment['group_id'],
                     ),
    em.Column.define('id', em.builtin_types['int4'], nullok=False, comment=column_comment['id'],
                     ),
    em.Column.define('log_pf', em.builtin_types['float4'], comment=column_comment['log_pf'],
                     ),
    em.Column.define(
        'log_pf_error', em.builtin_types['float4'], comment=column_comment['log_pf_error'],
    ),
    em.Column.define(
        'predicted_sasa',
        em.builtin_types['float4'],
        nullok=False,
        comment=column_comment['predicted_sasa'],
    ),
    em.Column.define(
        'seq_id', em.builtin_types['int4'], nullok=False, comment=column_comment['seq_id'],
    ),
    em.Column.define(
        'software_id', em.builtin_types['int4'], comment=column_comment['software_id'],
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
                    'outbound': ['PDB', 'ihm_hydroxyl_radical_fp_restraint_structure_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'asym id',
            'comment': 'A reference to table struct_asym.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_hydroxyl_radical_fp_restraint_asym_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'comp id',
            'comment': 'A reference to table chem_comp.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_hydroxyl_radical_fp_restraint_comp_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'dataset list id',
            'comment': 'A reference to table ihm_dataset_list.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_hydroxyl_radical_fp_restraint_dataset_list_id_fkey']
                }, 'RID'
            ]
        }, 'entity_description', {
            'markdown_name': 'entity id',
            'comment': 'A reference to table entity.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_hydroxyl_radical_fp_restraint_entity_id_fkey']
                }, 'RID'
            ]
        }, 'fp_rate', 'fp_rate_error', 'group_id', 'id', 'log_pf', 'log_pf_error', 'predicted_sasa',
        {
            'markdown_name': 'seq id',
            'comment': 'A reference to table entity_poly_seq.num.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_hydroxyl_radical_fp_restraint_seq_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'software id',
            'comment': 'A reference to table software.pdbx_ordinal.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_hydroxyl_radical_fp_restraint_software_id_fkey']
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
                    'outbound': ['PDB', 'ihm_hydroxyl_radical_fp_restraint_structure_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'asym id',
            'comment': 'A reference to table struct_asym.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_hydroxyl_radical_fp_restraint_asym_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'comp id',
            'comment': 'A reference to table chem_comp.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_hydroxyl_radical_fp_restraint_comp_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'dataset list id',
            'comment': 'A reference to table ihm_dataset_list.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_hydroxyl_radical_fp_restraint_dataset_list_id_fkey']
                }, 'RID'
            ]
        }, 'entity_description', {
            'markdown_name': 'entity id',
            'comment': 'A reference to table entity.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_hydroxyl_radical_fp_restraint_entity_id_fkey']
                }, 'RID'
            ]
        }, 'fp_rate', 'fp_rate_error', 'group_id', 'id', 'log_pf', 'log_pf_error', 'predicted_sasa',
        {
            'markdown_name': 'seq id',
            'comment': 'A reference to table entity_poly_seq.num.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_hydroxyl_radical_fp_restraint_seq_id_fkey']
                }, 'RID'
            ]
        }, {
            'markdown_name': 'software id',
            'comment': 'A reference to table software.pdbx_ordinal.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_hydroxyl_radical_fp_restraint_software_id_fkey']
                }, 'RID'
            ]
        }, ['PDB', 'ihm_hydroxyl_radical_fp_restraint_RCB_fkey'],
        ['PDB', 'ihm_hydroxyl_radical_fp_restraint_RMB_fkey'], 'RCT', 'RMT',
        ['PDB', 'ihm_hydroxyl_radical_fp_restraint_Owner_fkey']
    ]
}

table_annotations = {chaise_tags.visible_columns: visible_columns, }

table_comment = None

table_acls = {}

table_acl_bindings = {}

key_defs = [
    em.Key.define(
        ['RID'], constraint_names=[['PDB', 'ihm_hydroxyl_radical_fp_restraint_RIDkey1']],
    ),
    em.Key.define(
        ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_hydroxyl_radical_fp_restraint_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_hydroxyl_radical_fp_restraint_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_hydroxyl_radical_fp_restraint_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_hydroxyl_radical_fp_restraint_structure_id_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'asym_id'],
        'PDB',
        'struct_asym', ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_hydroxyl_radical_fp_restraint_asym_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['dataset_list_id', 'structure_id'],
        'PDB',
        'ihm_dataset_list', ['id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_hydroxyl_radical_fp_restraint_dataset_list_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['software_id', 'structure_id'],
        'PDB',
        'software', ['pdbx_ordinal', 'structure_id'],
        constraint_names=[['PDB', 'ihm_hydroxyl_radical_fp_restraint_software_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['comp_id', 'structure_id', 'entity_id', 'seq_id'],
        'PDB',
        'entity_poly_seq', ['mon_id', 'structure_id', 'entity_id', 'num'],
        constraint_names=[['PDB', 'ihm_hydroxyl_radical_fp_restraint_mm_poly_res_label_fkey']],
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
