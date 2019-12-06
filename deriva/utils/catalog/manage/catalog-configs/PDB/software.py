import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'software'

schema_name = 'PDB'

column_annotations = {
    'name': {},
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
    'location': {},
    'structure_id': {},
    'citation_id': {},
    'version': {},
    'Owner': {},
    'description': {},
    'pdbx_ordinal': {},
    'classification': {},
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    },
    'type': {}
}

column_comment = {
    'citation_id': 'A reference to table citation.id.',
    'name': 'type:text\nThe name of the software.\nexamples:Merlot,O,Xengen,X-plor',
    'Owner': 'Group that can update the record.',
    'classification': 'type:text\nThe classification of the program according to its\n major function.\nexamples:data collection,data reduction,phasing,model building,refinement,validation,other',
    'description': 'type:text\nDescription of the software.\nexamples:Uses method of restrained least squares',
    'pdbx_ordinal': 'type:int4\nAn ordinal index for this category\nexamples:1,2',
    'version': 'type:text\nThe version of the software.\nexamples:v1.0,beta,3.1-2,unknown',
    'type': 'type:text\nThe classification of the software according to the most\n common types.',
    'structure_id': 'A reference to table entry.id.',
    'location': 'type:text\nThe URL for an Internet address at which\n details of the software can be found.\nexamples:http://rosebud.sdsc.edu/projects/pb/IUCr/software.html,ftp://ftp.sdsc.edu/pub/sdsc/biology/'
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
        'citation_id', em.builtin_types['text'], comment=column_comment['citation_id'],
    ),
    em.Column.define(
        'classification',
        em.builtin_types['text'],
        nullok=False,
        comment=column_comment['classification'],
    ),
    em.Column.define(
        'description', em.builtin_types['text'], comment=column_comment['description'],
    ),
    em.Column.define('location', em.builtin_types['text'], comment=column_comment['location'],
                     ),
    em.Column.define(
        'name', em.builtin_types['text'], nullok=False, comment=column_comment['name'],
    ),
    em.Column.define(
        'pdbx_ordinal',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['pdbx_ordinal'],
    ),
    em.Column.define('type', em.builtin_types['text'], comment=column_comment['type'],
                     ),
    em.Column.define('version', em.builtin_types['text'], comment=column_comment['version'],
                     ),
    em.Column.define('Owner', em.builtin_types['text'], comment=column_comment['Owner'],
                     ),
]

table_display = {'row_name': {'row_markdown_pattern': '{{{pdbx_ordinal}}}'}}

visible_foreign_keys = {
    'detailed': [
        ['PDB', 'ihm_starting_computational_models_software_id_fkey'],
        ['PDB', 'ihm_epr_restraint_fitting_software_id_fkey'],
        ['PDB', 'ihm_hydroxyl_radical_fp_restraint_software_id_fkey'],
        ['PDB', 'ihm_predicted_contact_restraint_software_id_fkey'],
        ['PDB', 'ihm_modeling_protocol_details_software_id_fkey'],
        ['PDB', 'ihm_modeling_post_process_software_id_fkey']
    ],
    'filter': 'detailed'
}

visible_columns = {
    'entry': [
        {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'software_structure_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'citation id',
            'comment': 'A reference to table citation.id.',
            'source': [{
                'outbound': ['PDB', 'software_citation_id_fkey']
            }, 'RID']
        }, 'classification', 'description', 'location', 'name', 'pdbx_ordinal',
        ['PDB', 'software_type_term_fkey'], 'version'
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'software_structure_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'citation id',
            'comment': 'A reference to table citation.id.',
            'source': [{
                'outbound': ['PDB', 'software_citation_id_fkey']
            }, 'RID']
        }, 'classification', 'description', 'location', 'name', 'pdbx_ordinal',
        ['PDB', 'software_type_term_fkey'], 'version', ['PDB', 'software_RCB_fkey'],
        ['PDB', 'software_RMB_fkey'], 'RCT', 'RMT', ['PDB', 'software_Owner_fkey']
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
    em.Key.define(['RID'], constraint_names=[['PDB', 'software_RIDkey1']],
                  ),
    em.Key.define(
        ['structure_id', 'pdbx_ordinal'], constraint_names=[['PDB', 'software_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['type'],
        'Vocab',
        'software_type_term', ['ID'],
        constraint_names=[['PDB', 'software_type_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'software_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'software_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'software_structure_id_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'citation_id'],
        'PDB',
        'citation', ['structure_id', 'id'],
        constraint_names=[['PDB', 'software_citation_id_fkey']],
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
