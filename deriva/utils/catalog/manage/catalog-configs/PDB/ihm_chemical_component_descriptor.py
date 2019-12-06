import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'ihm_chemical_component_descriptor'

schema_name = 'PDB'

column_annotations = {
    'smiles': {},
    'Owner': {},
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'auth_name': {},
    'inchi': {},
    'RCB': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Created By'
        },
        chaise_tags.immutable: None
    },
    'structure_id': {},
    'inchi_key': {},
    'chemical_name': {},
    'details': {},
    'RMB': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Modified By'
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
    'smiles_canonical': {},
    'id': {},
    'common_name': {}
}

column_comment = {
    'smiles': 'type:text\nThe smile string of the component.',
    'inchi_key': 'type:text\nThe hashed INCHI key of the component.',
    'chemical_name': 'type:text\nThe chemical name of the component.',
    'details': 'type:text\nAdditional details regarding the chemical component.',
    'inchi': 'type:text\nThe IUPAC INCHI descriptor of the component.',
    'Owner': 'Group that can update the record.',
    'smiles_canonical': 'type:text\nThe canonical smile string of the component.',
    'auth_name': 'type:text\nThe author-provided name of the component.',
    'structure_id': 'A reference to table entry.id.',
    'id': 'type:int4\nAn identifier for the chemical descriptor.',
    'common_name': 'type:text\nThe common name of the component.'
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
        'auth_name', em.builtin_types['text'], nullok=False, comment=column_comment['auth_name'],
    ),
    em.Column.define(
        'chemical_name', em.builtin_types['text'], comment=column_comment['chemical_name'],
    ),
    em.Column.define(
        'common_name', em.builtin_types['text'], comment=column_comment['common_name'],
    ),
    em.Column.define('details', em.builtin_types['text'], comment=column_comment['details'],
                     ),
    em.Column.define('id', em.builtin_types['int4'], nullok=False, comment=column_comment['id'],
                     ),
    em.Column.define('inchi', em.builtin_types['text'], comment=column_comment['inchi'],
                     ),
    em.Column.define('inchi_key', em.builtin_types['text'], comment=column_comment['inchi_key'],
                     ),
    em.Column.define('smiles', em.builtin_types['text'], comment=column_comment['smiles'],
                     ),
    em.Column.define(
        'smiles_canonical', em.builtin_types['text'], comment=column_comment['smiles_canonical'],
    ),
    em.Column.define('Owner', em.builtin_types['text'], comment=column_comment['Owner'],
                     ),
]

visible_foreign_keys = {
    'detailed': [
        ['PDB', 'ihm_probe_list_probe_chem_comp_descriptor_id_fkey'],
        ['PDB', 'ihm_probe_list_reactive_probe_chem_comp_descriptor_id_fkey'],
        ['PDB', 'ihm_poly_probe_position_mod_res_chem_comp_descriptor_id_fkey'],
        ['PDB', 'ihm_poly_probe_conjugate_chem_comp_descriptor_id_fkey'],
        ['PDB', 'ihm_cross_link_list_linker_chem_comp_descriptor_id_fkey']
    ],
    'filter': 'detailed'
}

visible_columns = {
    'entry': [
        {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_chemical_component_descriptor_structure_id_fkey']
                }, 'RID'
            ]
        }, 'auth_name', 'chemical_name', 'common_name', 'details', 'id', 'inchi', 'inchi_key',
        'smiles', 'smiles_canonical'
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [
                {
                    'outbound': ['PDB', 'ihm_chemical_component_descriptor_structure_id_fkey']
                }, 'RID'
            ]
        }, 'auth_name', 'chemical_name', 'common_name', 'details', 'id', 'inchi', 'inchi_key',
        'smiles', 'smiles_canonical', ['PDB', 'ihm_chemical_component_descriptor_RCB_fkey'],
        ['PDB', 'ihm_chemical_component_descriptor_RMB_fkey'], 'RCT', 'RMT',
        ['PDB', 'ihm_chemical_component_descriptor_Owner_fkey']
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
    em.Key.define(
        ['RID'], constraint_names=[['PDB', 'ihm_chemical_component_descriptor_RIDkey1']],
    ),
    em.Key.define(
        ['structure_id', 'id'],
        constraint_names=[['PDB', 'ihm_chemical_component_descriptor_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_chemical_component_descriptor_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'ihm_chemical_component_descriptor_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'ihm_chemical_component_descriptor_structure_id_fkey']],
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
