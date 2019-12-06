import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'struct'

schema_name = 'PDB'

column_annotations = {
    'pdbx_details': {},
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'title': {},
    'entry_id': {},
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
    'pdbx_CASP_flag': {},
    'Owner': {},
    'pdbx_descriptor': {},
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    },
    'pdbx_model_type_details': {},
    'pdbx_model_details': {}
}

column_comment = {
    'pdbx_details': 'type:text\nAdditional remarks related to this structure deposition that have not\nbeen included in details data items elsewhere.\nexamples:Hydrogen bonds between peptide chains follow the Rich and Crick\nmodel II for collagen.',
    'pdbx_CASP_flag': 'type:text\nThe item indicates whether the entry is a CASP target, a CASD-NMR target,\n or similar target participating in methods development experiments.\nexamples:Y',
    'Owner': 'Group that can update the record.',
    'pdbx_model_type_details': 'type:text\nA description of the type of structure model.\nexamples:MINIMIZED AVERAGE',
    'pdbx_descriptor': "type:text\nAn automatically generated descriptor for an NDB structure or\n the unstructured content of the PDB COMPND record.\nexamples:5'-D(*CP*GP*CP*(HYD)AP*AP*AP*TP*TP*TP*GP*CP*G)-3'",
    'entry_id': 'A reference to table entry.id.',
    'structure_id': 'A reference to table entry.id.',
    'title': "type:text\nA title for the data block. The author should attempt to convey\n the essence of the structure archived in the CIF in the title,\n and to distinguish this structural result from others.\nexamples:T4 lysozyme mutant - S32A,5'-D(*(I)CP*CP*GP*G)-3,T4 lysozyme mutant - S32A,hen egg white lysozyme at -30 degrees C,quail egg white lysozyme at 2 atmospheres",
    'pdbx_model_details': 'type:text\nText description of the methodology which produced this\n model structure.\nexamples:This model was produced from a 10 nanosecond Amber/MD simulation\nstarting from PDB structure ID 1ABC.'
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
        'entry_id', em.builtin_types['text'], nullok=False, comment=column_comment['entry_id'],
    ),
    em.Column.define(
        'pdbx_CASP_flag', em.builtin_types['text'], comment=column_comment['pdbx_CASP_flag'],
    ),
    em.Column.define(
        'pdbx_descriptor', em.builtin_types['text'], comment=column_comment['pdbx_descriptor'],
    ),
    em.Column.define(
        'pdbx_details', em.builtin_types['text'], comment=column_comment['pdbx_details'],
    ),
    em.Column.define(
        'pdbx_model_details',
        em.builtin_types['text'],
        comment=column_comment['pdbx_model_details'],
    ),
    em.Column.define(
        'pdbx_model_type_details',
        em.builtin_types['text'],
        comment=column_comment['pdbx_model_type_details'],
    ),
    em.Column.define('title', em.builtin_types['text'], comment=column_comment['title'],
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
                'outbound': ['PDB', 'struct_structure_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'entry id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'struct_entry_id_fkey']
            }, 'RID']
        }, ['PDB', 'struct_pdbx_CASP_flag_term_fkey'], 'pdbx_descriptor', 'pdbx_details',
        'pdbx_model_details', 'pdbx_model_type_details', 'title'
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'struct_structure_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'entry id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'struct_entry_id_fkey']
            }, 'RID']
        }, ['PDB', 'struct_pdbx_CASP_flag_term_fkey'], 'pdbx_descriptor', 'pdbx_details',
        'pdbx_model_details', 'pdbx_model_type_details', 'title', ['PDB', 'struct_RCB_fkey'],
        ['PDB', 'struct_RMB_fkey'], 'RCT', 'RMT', ['PDB', 'struct_Owner_fkey']
    ]
}

table_annotations = {chaise_tags.visible_columns: visible_columns, }

table_comment = None

table_acls = {}

table_acl_bindings = {}

key_defs = [
    em.Key.define(['RID'], constraint_names=[['PDB', 'struct_RIDkey1']],
                  ),
    em.Key.define(['entry_id', 'structure_id'], constraint_names=[['PDB', 'struct_primary_key']],
                  ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['pdbx_CASP_flag'],
        'Vocab',
        'struct_pdbx_CASP_flag_term', ['ID'],
        constraint_names=[['PDB', 'struct_pdbx_CASP_flag_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'struct_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'struct_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'struct_structure_id_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'entry_id'],
        'PDB',
        'entry', ['structure_id', 'id'],
        constraint_names=[['PDB', 'struct_entry_id_fkey']],
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
