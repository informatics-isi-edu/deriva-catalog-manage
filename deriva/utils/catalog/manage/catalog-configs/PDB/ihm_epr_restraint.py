import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_epr_restraint'

schema_name = 'PDB'

column_annotations = {
    'fitting_software_id': {},
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'structure_id': {},
    'RMB': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Modified By'
        },
        chaise_tags.immutable: None
    },
    'chi_value': {},
    'RCB': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Created By'
        },
        chaise_tags.immutable: None
    },
    'model_id': {},
    'dataset_list_id': {},
    'Owner': {},
    'details': {},
    'ordinal_id': {},
    'fitting_particle_type': {},
    'fitting_method_citation_id': {},
    'fitting_state': {},
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    },
    'fitting_method': {}
}

column_comment = {
    'fitting_software_id': 'A reference to table software.pdbx_ordinal.',
    'structure_id': 'A reference to table entry.id.',
    'chi_value': 'type:float4\nThe chi value resulting from fitting the model to the EPR data.',
    'model_id': 'A reference to table ihm_model_list.model_id.',
    'dataset_list_id': 'A reference to table ihm_dataset_list.id.',
    'Owner': 'Group that can update the record.',
    'details': 'type:text\nAdditional details regarding the EPR restraint used.',
    'ordinal_id': 'type:int4\nA unique identifier for the EPR restraint description.',
    'fitting_particle_type': 'type:text\nThe type of particle fit to the EPR data.\nexamples:Unpaired electrons of the probe',
    'fitting_method_citation_id': 'A reference to table citation.id.',
    'fitting_state': 'type:text\nAn indicator to single or multiple state fitting.',
    'fitting_method': 'type:text\nThe method used for fitting the model to the EPR data.\nexamples:Spin label rotamer refinement using DEER/PELDOR data'
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
        'chi_value', em.builtin_types['float4'], comment=column_comment['chi_value'],
    ),
    em.Column.define(
        'dataset_list_id',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['dataset_list_id'],
    ),
    em.Column.define('details', em.builtin_types['text'], comment=column_comment['details'],
                     ),
    em.Column.define(
        'fitting_method', em.builtin_types['text'], comment=column_comment['fitting_method'],
    ),
    em.Column.define(
        'fitting_method_citation_id',
        em.builtin_types['text'],
        comment=column_comment['fitting_method_citation_id'],
    ),
    em.Column.define(
        'fitting_particle_type',
        em.builtin_types['text'],
        comment=column_comment['fitting_particle_type'],
    ),
    em.Column.define(
        'fitting_software_id',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['fitting_software_id'],
    ),
    em.Column.define(
        'fitting_state', em.builtin_types['text'], comment=column_comment['fitting_state'],
    ),
    em.Column.define(
        'model_id', em.builtin_types['int4'], nullok=False, comment=column_comment['model_id'],
    ),
    em.Column.define(
        'ordinal_id',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['ordinal_id'],
    ),
    em.Column.define('Owner', em.builtin_types['text'], comment=column_comment['Owner'],
                     ),
]

visible_columns = {
    'entry': [
        {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_epr_restraint_structure_id_fkey']
            }, 'RID']
        }, 'chi_value', {
            'markdown_name': 'dataset list id',
            'comment': 'A reference to table ihm_dataset_list.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_epr_restraint_dataset_list_id_fkey']
            }, 'RID']
        }, 'details', 'fitting_method', {
            'markdown_name': 'fitting method citation id',
            'comment': 'A reference to table citation.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_epr_restraint_fitting_method_citation_id_fkey']
                }, 'RID'
            ]
        }, 'fitting_particle_type', {
            'markdown_name': 'fitting software id',
            'comment': 'A reference to table software.pdbx_ordinal.',
            'source': [{
                'outbound': ['PDB', 'ihm_epr_restraint_fitting_software_id_fkey']
            }, 'RID']
        }, ['PDB', 'ihm_epr_restraint_fitting_state_term_fkey'], {
            'markdown_name': 'model id',
            'comment': 'A reference to table ihm_model_list.model_id.',
            'source': [{
                'outbound': ['PDB', 'ihm_epr_restraint_model_id_fkey']
            }, 'RID']
        }, 'ordinal_id'
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_epr_restraint_structure_id_fkey']
            }, 'RID']
        }, 'chi_value', {
            'markdown_name': 'dataset list id',
            'comment': 'A reference to table ihm_dataset_list.id.',
            'source': [{
                'outbound': ['PDB', 'ihm_epr_restraint_dataset_list_id_fkey']
            }, 'RID']
        }, 'details', 'fitting_method', {
            'markdown_name': 'fitting method citation id',
            'comment': 'A reference to table citation.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_epr_restraint_fitting_method_citation_id_fkey']
                }, 'RID'
            ]
        }, 'fitting_particle_type', {
            'markdown_name': 'fitting software id',
            'comment': 'A reference to table software.pdbx_ordinal.',
            'source': [{
                'outbound': ['PDB', 'ihm_epr_restraint_fitting_software_id_fkey']
            }, 'RID']
        }, ['PDB', 'ihm_epr_restraint_fitting_state_term_fkey'], {
            'markdown_name': 'model id',
            'comment': 'A reference to table ihm_model_list.model_id.',
            'source': [{
                'outbound': ['PDB', 'ihm_epr_restraint_model_id_fkey']
            }, 'RID']
        }, 'ordinal_id',
        ['PDB', 'ihm_epr_restraint_RCB_fkey'], ['PDB', 'ihm_epr_restraint_RMB_fkey'], 'RCT', 'RMT',
        ['PDB', 'ihm_epr_restraint_Owner_fkey']
    ]
}

table_annotations = {chaise_tags.visible_columns: visible_columns, }

table_comment = None

table_acls = {}

table_acl_bindings = {}

key_defs = [
    em.Key.define(['RID'], constraint_names=[['PDB', 'ihm_epr_restraint_RIDkey1']],
                  ),
    em.Key.define(
        ['structure_id', 'ordinal_id'],
        constraint_names=[['PDB', 'ihm_epr_restraint_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['fitting_state'],
        'Vocab',
        'ihm_epr_restraint_fitting_state_term', ['ID'],
        constraint_names=[['PDB', 'ihm_epr_restraint_fitting_state_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_epr_restraint_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_epr_restraint_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_epr_restraint_structure_id_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['dataset_list_id', 'structure_id'],
        'PDB',
        'ihm_dataset_list', ['id', 'structure_id'],
        constraint_names=[['PDB', 'ihm_epr_restraint_dataset_list_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'fitting_method_citation_id'],
        'PDB',
        'citation', ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_epr_restraint_fitting_method_citation_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['fitting_software_id', 'structure_id'],
        'PDB',
        'software', ['pdbx_ordinal', 'structure_id'],
        constraint_names=[['PDB', 'ihm_epr_restraint_fitting_software_id_fkey']],
        annotations={
            chaise_tags.foreign_key: {
                'domain_filter_pattern': 'structure_id={{structure_id}}'
            }
        },
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'model_id'],
        'PDB',
        'ihm_model_list', ['structure_id', 'model_id'],
        constraint_names=[['PDB', 'ihm_epr_restraint_model_id_fkey']],
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
