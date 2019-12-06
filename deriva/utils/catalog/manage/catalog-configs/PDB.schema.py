import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

schema_name = 'PDB'

table_names = [
    'ihm_dataset_related_db_reference', 'ihm_ligand_probe', 'ihm_starting_computational_models',
    'ihm_struct_assembly_class', 'ihm_struct_assembly', 'ihm_starting_model_details', 'chem_comp',
    'ihm_pseudo_site_feature', 'ihm_cross_link_result_parameters', 'ihm_poly_atom_feature',
    'entity_poly_seq', 'ihm_modeling_post_process', 'struct_asym', 'audit_conform',
    'ihm_ensemble_info', 'struct', 'ihm_derived_distance_restraint',
    'ihm_geometric_object_half_torus', 'ihm_model_representative', 'citation_author',
    'ihm_entity_poly_segment', 'ihm_dataset_external_reference', 'pdbx_entity_nonpoly',
    'ihm_model_representation_details', 'ihm_localization_density_files', 'ihm_3dem_restraint',
    'ihm_poly_probe_conjugate', 'entity_src_gen', 'ihm_geometric_object_list',
    'ihm_starting_comparative_models', 'ihm_struct_assembly_class_link', 'ihm_cross_link_restraint',
    'entry', 'entity_name_com', 'ihm_model_group', 'atom_type', 'ihm_geometric_object_plane',
    'entity_name_sys', 'ihm_dataset_group', 'ihm_geometric_object_center', 'ihm_model_list',
    'ihm_residues_not_modeled', 'pdbx_inhibitor_info', 'software', 'ihm_related_datasets',
    'ihm_cross_link_list', 'ihm_poly_probe_position', 'ihm_dataset_list', 'ihm_feature_list',
    'ihm_geometric_object_torus', 'ihm_modeling_protocol', 'pdbx_entity_poly_na_type',
    'ihm_geometric_object_transformation', 'entity', 'ihm_external_files',
    'ihm_chemical_component_descriptor', 'ihm_multi_state_modeling',
    'ihm_interface_residue_feature', 'ihm_starting_model_seq_dif',
    'ihm_multi_state_model_group_link', 'ihm_predicted_contact_restraint',
    'ihm_hydroxyl_radical_fp_restraint', 'ihm_modeling_protocol_details', 'pdbx_entry_details',
    'ihm_geometric_object_axis', 'audit_author', 'ihm_model_representation',
    'ihm_2dem_class_average_fitting', 'ihm_external_reference_info', 'ihm_epr_restraint',
    'ihm_struct_assembly_details', 'ihm_sas_restraint', 'chem_comp_atom', 'ihm_cross_link_result',
    'pdbx_ion_info', 'ihm_geometric_object_distance_restraint', 'ihm_probe_list', 'entity_poly',
    'ihm_dataset_group_link', 'citation', 'ihm_geometric_object_sphere', 'ihm_ordered_ensemble',
    'pdbx_protein_info', 'ihm_poly_residue_feature', 'ihm_non_poly_feature',
    'ihm_2dem_class_average_restraint', 'ihm_model_group_link',
]

annotations = {chaise_tags.display: {'name_style': {'title_case': True, 'underline_space': True}}}

acls = {}

comment = None

schema_def = em.Schema.define('PDB', comment=comment, acls=acls, annotations=annotations, )


def main(catalog, mode, replace=False):
    updater = CatalogUpdater(catalog)
    updater.update_schema(mode, schema_def, replace=replace)


if __name__ == "__main__":
    host = 'pdb.isrd.isi.edu'
    catalog_id = 9
    mode, replace, host, catalog_id = parse_args(host, catalog_id, is_catalog=True)
    catalog = DerivaCatalog(host, catalog_id=catalog_id, validate=False)
    main(catalog, mode, replace)
