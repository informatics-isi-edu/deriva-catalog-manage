import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'predicted_contact_restraint_model_granularity_term'

schema_name = 'Vocab'

column_annotations = {
    'Name': {},
    'Owner': {},
    'Description': {},
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'Synonyms': {},
    'URI': {},
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
    'ID': {}
}

column_comment = {
    'Name': 'The preferred human-readable name for this term.',
    'Owner': 'Group that can update the record.',
    'Description': 'A longer human-readable description of this term.',
    'Synonyms': 'Alternate human-readable names for this term.',
    'URI': 'The preferred URI for this term.',
    'ID': 'The preferred Compact URI (CURIE) for this term.'
}

column_acls = {}

column_acl_bindings = {}

column_defs = [
    em.Column.define(
        'ID',
        em.builtin_types['ermrest_curie'],
        nullok=False,
        default='PDB:{RID}',
        comment=column_comment['ID'],
    ),
    em.Column.define(
        'URI',
        em.builtin_types['ermrest_uri'],
        nullok=False,
        default='/id/{RID}',
        comment=column_comment['URI'],
    ),
    em.Column.define(
        'Name', em.builtin_types['text'], nullok=False, comment=column_comment['Name'],
    ),
    em.Column.define(
        'Description',
        em.builtin_types['markdown'],
        nullok=False,
        comment=column_comment['Description'],
    ),
    em.Column.define('Synonyms', em.builtin_types['text[]'], comment=column_comment['Synonyms'],
                     ),
    em.Column.define('Owner', em.builtin_types['text'], comment=column_comment['Owner'],
                     ),
]

table_display = {'row_name': {'row_markdown_pattern': '{{{Name}}}'}}

visible_columns = {
    '*': [
        'RID', 'Name', 'Description', 'ID', 'URI',
        ['Vocab', 'predicted_contact_restraint_model_granularity_term_RCB_fkey'],
        ['Vocab', 'predicted_contact_restraint_model_granularity_term_RMB_fkey'], 'RCT', 'RMT',
        ['Vocab', 'predicted_contact_restraint_model_granularity_term_Owner_fkey']
    ]
}

table_annotations = {
    chaise_tags.visible_columns: visible_columns,
    chaise_tags.table_display: table_display,
}

table_comment = 'A set of controlled vocabular terms.'

table_acls = {}

table_acl_bindings = {}

key_defs = [
    em.Key.define(
        ['RID'],
        constraint_names=[['Vocab', 'predicted_contact_restraint_model_granularity_term_RIDkey1']],
    ),
    em.Key.define(
        ['ID'],
        constraint_names=[['Vocab', 'predicted_contact_restraint_model_granularity_term_IDkey1']],
    ),
    em.Key.define(
        ['URI'],
        constraint_names=[['Vocab', 'predicted_contact_restraint_model_granularity_term_URIkey1']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['Vocab', 'predicted_contact_restraint_model_granularity_term_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['Vocab', 'predicted_contact_restraint_model_granularity_term_RMB_fkey']],
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
