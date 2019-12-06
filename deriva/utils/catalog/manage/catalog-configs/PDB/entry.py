import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'entry'

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
    'id': {},
    'structure_id': {}
}

column_comment = {
    'Owner': 'Group that can update the record.',
    'structure_id': 'A reference to table entry.id.',
    'id': 'type:text\nThe value of _entry.id identifies the data block.\n\n Note that this item need not be a number; it can be any unique\n identifier.'
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
    em.Column.define('id', em.builtin_types['text'], nullok=False, comment=column_comment['id'],
                     ),
    em.Column.define('Owner', em.builtin_types['text'], comment=column_comment['Owner'],
                     ),
]

table_display = {'row_name': {'row_markdown_pattern': '{{{id}}}'}}

visible_foreign_keys = {
    'detailed': [
        ['PDB', 'entry_structure_id_fkey'], ['PDB', 'struct_structure_id_fkey'],
        ['PDB', 'struct_entry_id_fkey'], ['PDB', 'audit_author_structure_id_fkey'],
        ['PDB', 'citation_structure_id_fkey'], ['PDB', 'citation_author_structure_id_fkey'],
        ['PDB', 'software_structure_id_fkey'], ['PDB', 'chem_comp_structure_id_fkey'],
        ['PDB', 'chem_comp_atom_structure_id_fkey'], ['PDB', 'entity_structure_id_fkey'],
        ['PDB', 'entity_name_com_structure_id_fkey'], ['PDB', 'entity_name_sys_structure_id_fkey'],
        ['PDB', 'entity_src_gen_structure_id_fkey'], ['PDB', 'entity_poly_structure_id_fkey'],
        ['PDB', 'pdbx_entity_nonpoly_structure_id_fkey'],
        ['PDB', 'entity_poly_seq_structure_id_fkey'], ['PDB', 'atom_type_structure_id_fkey'],
        ['PDB', 'struct_asym_structure_id_fkey'],
        ['PDB', 'ihm_entity_poly_segment_structure_id_fkey'],
        ['PDB', 'ihm_model_representation_structure_id_fkey'],
        ['PDB', 'ihm_model_representation_details_structure_id_fkey'],
        ['PDB', 'ihm_struct_assembly_structure_id_fkey'],
        ['PDB', 'ihm_struct_assembly_details_structure_id_fkey'],
        ['PDB', 'ihm_struct_assembly_class_structure_id_fkey'],
        ['PDB', 'ihm_struct_assembly_class_link_structure_id_fkey'],
        ['PDB',
         'ihm_dataset_list_structure_id_fkey'], ['PDB', 'ihm_dataset_group_structure_id_fkey'],
        ['PDB', 'ihm_dataset_group_link_structure_id_fkey'],
        ['PDB', 'ihm_dataset_related_db_reference_structure_id_fkey'],
        ['PDB', 'ihm_external_reference_info_structure_id_fkey'],
        ['PDB', 'ihm_external_files_structure_id_fkey'],
        ['PDB', 'ihm_dataset_external_reference_structure_id_fkey'],
        ['PDB', 'ihm_related_datasets_structure_id_fkey'],
        ['PDB', 'ihm_starting_model_details_structure_id_fkey'],
        ['PDB', 'ihm_starting_comparative_models_structure_id_fkey'],
        ['PDB', 'ihm_starting_computational_models_structure_id_fkey'],
        ['PDB', 'ihm_starting_model_seq_dif_structure_id_fkey'],
        ['PDB', 'ihm_chemical_component_descriptor_structure_id_fkey'],
        ['PDB', 'ihm_probe_list_structure_id_fkey'],
        ['PDB', 'ihm_poly_probe_position_structure_id_fkey'],
        ['PDB', 'ihm_poly_probe_conjugate_structure_id_fkey'],
        ['PDB', 'ihm_ligand_probe_structure_id_fkey'],
        ['PDB', 'ihm_cross_link_list_structure_id_fkey'],
        ['PDB', 'ihm_cross_link_restraint_structure_id_fkey'],
        ['PDB', 'ihm_cross_link_result_structure_id_fkey'],
        ['PDB', 'ihm_cross_link_result_parameters_structure_id_fkey'],
        ['PDB', 'ihm_2dem_class_average_restraint_structure_id_fkey'],
        ['PDB', 'ihm_2dem_class_average_fitting_structure_id_fkey'],
        ['PDB', 'ihm_3dem_restraint_structure_id_fkey'],
        ['PDB', 'ihm_sas_restraint_structure_id_fkey'],
        ['PDB', 'ihm_epr_restraint_structure_id_fkey'],
        ['PDB', 'ihm_hydroxyl_radical_fp_restraint_structure_id_fkey'],
        ['PDB', 'ihm_predicted_contact_restraint_structure_id_fkey'],
        ['PDB', 'ihm_feature_list_structure_id_fkey'],
        ['PDB', 'ihm_poly_atom_feature_structure_id_fkey'],
        ['PDB', 'ihm_poly_residue_feature_structure_id_fkey'],
        ['PDB', 'ihm_non_poly_feature_structure_id_fkey'],
        ['PDB', 'ihm_interface_residue_feature_structure_id_fkey'],
        ['PDB', 'ihm_pseudo_site_feature_structure_id_fkey'],
        ['PDB', 'ihm_derived_distance_restraint_structure_id_fkey'],
        ['PDB', 'ihm_geometric_object_list_structure_id_fkey'],
        ['PDB', 'ihm_geometric_object_center_structure_id_fkey'],
        ['PDB', 'ihm_geometric_object_transformation_structure_id_fkey'],
        ['PDB', 'ihm_geometric_object_sphere_structure_id_fkey'],
        ['PDB', 'ihm_geometric_object_torus_structure_id_fkey'],
        ['PDB', 'ihm_geometric_object_half_torus_structure_id_fkey'],
        ['PDB', 'ihm_geometric_object_axis_structure_id_fkey'],
        ['PDB', 'ihm_geometric_object_plane_structure_id_fkey'],
        ['PDB', 'ihm_geometric_object_distance_restraint_structure_id_fkey'],
        ['PDB', 'ihm_modeling_protocol_structure_id_fkey'],
        ['PDB', 'ihm_modeling_protocol_details_structure_id_fkey'],
        ['PDB', 'ihm_modeling_post_process_structure_id_fkey'],
        ['PDB', 'ihm_model_list_structure_id_fkey'], ['PDB', 'ihm_model_group_structure_id_fkey'],
        ['PDB', 'ihm_model_group_link_structure_id_fkey'],
        ['PDB', 'ihm_model_representative_structure_id_fkey'],
        ['PDB', 'ihm_residues_not_modeled_structure_id_fkey'],
        ['PDB', 'ihm_multi_state_modeling_structure_id_fkey'],
        ['PDB', 'ihm_multi_state_model_group_link_structure_id_fkey'],
        ['PDB', 'ihm_ensemble_info_structure_id_fkey'],
        ['PDB', 'ihm_ordered_ensemble_structure_id_fkey'],
        ['PDB', 'ihm_localization_density_files_structure_id_fkey'],
        ['PDB', 'audit_conform_structure_id_fkey'],
        ['PDB', 'pdbx_entity_poly_na_type_structure_id_fkey'],
        ['PDB',
         'pdbx_entry_details_structure_id_fkey'], ['PDB', 'pdbx_entry_details_entry_id_fkey'],
        ['PDB', 'pdbx_inhibitor_info_structure_id_fkey'],
        ['PDB', 'pdbx_ion_info_structure_id_fkey'], ['PDB', 'pdbx_protein_info_structure_id_fkey']
    ],
    'filter': 'detailed'
}

visible_columns = {
    'entry': [
        {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'entry_structure_id_fkey']
            }, 'RID']
        }, 'id'
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'entry_structure_id_fkey']
            }, 'RID']
        }, 'id', ['PDB', 'entry_RCB_fkey'], ['PDB', 'entry_RMB_fkey'], 'RCT', 'RMT',
        ['PDB', 'entry_Owner_fkey']
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
    em.Key.define(['id'], constraint_names=[['PDB', 'entry_id_unique_key']],
                  ),
    em.Key.define(['RID'], constraint_names=[['PDB', 'entry_RIDkey1']],
                  ),
    em.Key.define(['structure_id', 'id'], constraint_names=[['PDB', 'entry_primary_key']],
                  ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['RCB'], 'public', 'ERMrest_Client', ['ID'], constraint_names=[['PDB', 'entry_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'], 'public', 'ERMrest_Client', ['ID'], constraint_names=[['PDB', 'entry_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'entry_structure_id_fkey']],
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
