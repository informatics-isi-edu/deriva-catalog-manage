import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'audit_author'

schema_name = 'PDB'

column_annotations = {
    'name': {},
    'Owner': {},
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'pdbx_ordinal': {},
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
    'structure_id': {}
}

column_comment = {
    'name': "type:text\nThe name of an author of this data block. If there are multiple\n authors, _audit_author.name is looped with _audit_author.address.\n The family name(s), followed by a comma and including any\n dynastic components, precedes the first name(s) or initial(s).\nexamples:Jones, T.J.,Bleary, Percival R.,O'Neil, F.K.,Van den Bossche, G.,Yang, D.-L.,Simonov, Yu.A",
    'Owner': 'Group that can update the record.',
    'structure_id': 'A reference to table entry.id.',
    'pdbx_ordinal': "type:int4\nThis data item defines the order of the author's name in the\n list of audit authors.\nexamples:1,2,3"
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
        'name', em.builtin_types['text'], nullok=False, comment=column_comment['name'],
    ),
    em.Column.define(
        'pdbx_ordinal',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['pdbx_ordinal'],
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
                'outbound': ['PDB', 'audit_author_structure_id_fkey']
            }, 'RID']
        }, 'name', 'pdbx_ordinal'
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'audit_author_structure_id_fkey']
            }, 'RID']
        }, 'name', 'pdbx_ordinal', ['PDB', 'audit_author_RCB_fkey'],
        ['PDB', 'audit_author_RMB_fkey'], 'RCT', 'RMT', ['PDB', 'audit_author_Owner_fkey']
    ]
}

table_annotations = {chaise_tags.visible_columns: visible_columns, }

table_comment = None

table_acls = {}

table_acl_bindings = {}

key_defs = [
    em.Key.define(['RID'], constraint_names=[['PDB', 'audit_author_RIDkey1']],
                  ),
    em.Key.define(
        ['pdbx_ordinal', 'structure_id'], constraint_names=[['PDB', 'audit_author_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'audit_author_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'audit_author_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'audit_author_structure_id_fkey']],
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
