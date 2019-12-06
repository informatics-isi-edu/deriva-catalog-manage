import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_probe_list'

schema_name = 'PDB'

column_annotations = {
    'Owner': {},
    'reactive_probe_name': {},
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'probe_link_type': {},
    'probe_chem_comp_descriptor_id': {},
    'probe_id': {},
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
    'reactive_probe_chem_comp_descriptor_id': {},
    'reactive_probe_flag': {},
    'structure_id': {},
    'probe_origin': {},
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    },
    'probe_name': {}
}

column_comment = {
    'reactive_probe_flag': 'type:text\nIndicate whether the probe has a reactive form.',
    'reactive_probe_name': 'type:text\nAuthor provided name for the reactive_probe, if applicable.',
    'probe_name': 'type:text\nAuthor provided name for the probe.',
    'probe_link_type': 'type:text\nThe type of link between the probe and the biomolecule.',
    'Owner': 'Group that can update the record.',
    'probe_origin': 'type:text\nThe origin of the probe.',
    'probe_chem_comp_descriptor_id': 'A reference to table ihm_chemical_component_descriptor.id.',
    'probe_id': 'type:int4\nA unique identifier for the category.',
    'structure_id': 'A reference to table entry.id.',
    'reactive_probe_chem_comp_descriptor_id': 'A reference to table ihm_chemical_component_descriptor.id.'
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
        'probe_chem_comp_descriptor_id',
        em.builtin_types['int4'],
        comment=column_comment['probe_chem_comp_descriptor_id'],
    ),
    em.Column.define(
        'probe_id', em.builtin_types['int4'], nullok=False, comment=column_comment['probe_id'],
    ),
    em.Column.define(
        'probe_link_type',
        em.builtin_types['text'],
        nullok=False,
        comment=column_comment['probe_link_type'],
    ),
    em.Column.define(
        'probe_name',
        em.builtin_types['text'],
        nullok=False,
        comment=column_comment['probe_name'],
    ),
    em.Column.define(
        'probe_origin',
        em.builtin_types['text'],
        nullok=False,
        comment=column_comment['probe_origin'],
    ),
    em.Column.define(
        'reactive_probe_chem_comp_descriptor_id',
        em.builtin_types['int4'],
        comment=column_comment['reactive_probe_chem_comp_descriptor_id'],
    ),
    em.Column.define(
        'reactive_probe_flag',
        em.builtin_types['text'],
        comment=column_comment['reactive_probe_flag'],
    ),
    em.Column.define(
        'reactive_probe_name',
        em.builtin_types['text'],
        comment=column_comment['reactive_probe_name'],
    ),
    em.Column.define('Owner', em.builtin_types['text'], comment=column_comment['Owner'],
                     ),
]

visible_foreign_keys = {
    'detailed': [
        ['PDB', 'ihm_poly_probe_conjugate_probe_id_fkey'],
        ['PDB', 'ihm_ligand_probe_probe_id_fkey']
    ],
    'filter': 'detailed'
}

visible_columns = {
    'entry': [
        {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_probe_list_structure_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'probe chem comp descriptor id',
            'comment': 'A reference to table ihm_chemical_component_descriptor.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_probe_list_probe_chem_comp_descriptor_id_fkey']
                }, 'RID'
            ]
        }, 'probe_id', ['PDB', 'ihm_probe_list_probe_link_type_term_fkey'], 'probe_name',
        ['PDB', 'ihm_probe_list_probe_origin_term_fkey'], {
            'markdown_name': 'reactive probe chem comp descriptor id',
            'comment': 'A reference to table ihm_chemical_component_descriptor.id.',
            'source': [
                {
                    'outbound': [
                        'PDB', 'ihm_probe_list_reactive_probe_chem_comp_descriptor_id_fkey'
                    ]
                }, 'RID'
            ]
        }, ['PDB', 'ihm_probe_list_reactive_probe_flag_term_fkey'], 'reactive_probe_name'
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_probe_list_structure_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'probe chem comp descriptor id',
            'comment': 'A reference to table ihm_chemical_component_descriptor.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_probe_list_probe_chem_comp_descriptor_id_fkey']
                }, 'RID'
            ]
        }, 'probe_id', ['PDB', 'ihm_probe_list_probe_link_type_term_fkey'], 'probe_name',
        ['PDB', 'ihm_probe_list_probe_origin_term_fkey'], {
            'markdown_name': 'reactive probe chem comp descriptor id',
            'comment': 'A reference to table ihm_chemical_component_descriptor.id.',
            'source': [
                {
                    'outbound': [
                        'PDB', 'ihm_probe_list_reactive_probe_chem_comp_descriptor_id_fkey'
                    ]
                }, 'RID'
            ]
        }, ['PDB', 'ihm_probe_list_reactive_probe_flag_term_fkey'], 'reactive_probe_name',
        ['PDB', 'ihm_probe_list_RCB_fkey'], ['PDB', 'ihm_probe_list_RMB_fkey'], 'RCT', 'RMT',
        ['PDB', 'ihm_probe_list_Owner_fkey']
    ]
}

table_annotations = {
    chaise_tags.visible_columns: visible_columns,
    chaise_tags.visible_foreign_keys: visible_foreign_keys,
}

table_comment = None

table_acls = {}

table_acl_bindings = {}

key_defs = [
    em.Key.define(['RID'], constraint_names=[['PDB', 'ihm_probe_list_RIDkey1']],
                  ),
    em.Key.define(
        ['structure_id', 'probe_id'], constraint_names=[['PDB', 'ihm_probe_list_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['probe_link_type'],
        'Vocab',
        'ihm_probe_list_probe_link_type_term', ['ID'],
        constraint_names=[['PDB', 'ihm_probe_list_probe_link_type_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['probe_origin'],
        'Vocab',
        'ihm_probe_list_probe_origin_term', ['ID'],
        constraint_names=[['PDB', 'ihm_probe_list_probe_origin_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['reactive_probe_flag'],
        'Vocab',
        'ihm_probe_list_reactive_probe_flag_term', ['ID'],
        constraint_names=[['PDB', 'ihm_probe_list_reactive_probe_flag_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_probe_list_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_probe_list_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_probe_list_structure_id_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['probe_chem_comp_descriptor_id', 'structure_id'],
        'PDB',
        'ihm_chemical_component_descriptor', ['id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_probe_list_probe_chem_comp_descriptor_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'reactive_probe_chem_comp_descriptor_id'],
        'PDB',
        'ihm_chemical_component_descriptor', ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_probe_list_reactive_probe_chem_comp_descriptor_id_fkey']],
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
