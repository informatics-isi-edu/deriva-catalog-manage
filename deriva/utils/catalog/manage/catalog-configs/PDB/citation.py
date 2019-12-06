import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'citation'

schema_name = 'PDB'

column_annotations = {
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'page_last': {},
    'journal_id_CSD': {},
    'pdbx_database_id_DOI': {},
    'journal_volume': {},
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
    'title': {},
    'structure_id': {},
    'Owner': {},
    'page_first': {},
    'country': {},
    'journal_id_ISSN': {},
    'journal_issue': {},
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    },
    'journal_abbrev': {},
    'pdbx_database_id_PubMed': {},
    'journal_id_ASTM': {},
    'id': {},
    'year': {}
}

column_comment = {
    'page_last': 'type:text\nThe last page of the citation; relevant for journal\n articles, books and book chapters.',
    'journal_id_CSD': 'type:text\nThe Cambridge Structural Database (CSD) code assigned to the\n journal cited; relevant for journal articles. This is also the\n system used at the Protein Data Bank (PDB).\nexamples:0070',
    'pdbx_database_id_DOI': 'type:text\nDocument Object Identifier used by doi.org to uniquely\n specify bibliographic entry.\nexamples:10.2345/S1384107697000225',
    'structure_id': 'A reference to table entry.id.',
    'journal_issue': 'type:text\nIssue number of the journal cited; relevant for journal\n articles.\nexamples:2',
    'title': 'type:text\nThe title of the citation; relevant for journal articles, books\n and book chapters.\nexamples:Structure of diferric duck ovotransferrin\n                                  at 2.35 Angstroms resolution.',
    'journal_volume': 'type:text\nVolume number of the journal cited; relevant for journal\n articles.\nexamples:174',
    'Owner': 'Group that can update the record.',
    'page_first': 'type:text\nThe first page of the citation; relevant for journal\n articles, books and book chapters.',
    'country': 'type:text\nThe country/region of publication; relevant for books\n and book chapters.',
    'journal_id_ISSN': 'type:text\nThe International Standard Serial Number (ISSN) code assigned to\n the journal cited; relevant for journal articles.',
    'journal_abbrev': 'type:text\nAbbreviated name of the cited journal as given in the\n Chemical Abstracts Service Source Index.\nexamples:J.Mol.Biol.,J. Mol. Biol.',
    'pdbx_database_id_PubMed': 'type:int4\nAscession number used by PubMed to categorize a specific\n bibliographic entry.\nexamples:12627512',
    'journal_id_ASTM': 'type:text\nThe American Society for Testing and Materials (ASTM) code\n assigned to the journal cited (also referred to as the CODEN\n designator of the Chemical Abstracts Service); relevant for\n journal articles.',
    'id': "type:text\nThe value of _citation.id must uniquely identify a record in the\n CITATION list.\n\n The _citation.id 'primary' should be used to indicate the\n citation that the author(s) consider to be the most pertinent to\n the contents of the data block.\n\n Note that this item need not be a number; it can be any unique\n identifier.\nexamples:primary,1,2",
    'year': 'type:int4\nThe year of the citation; relevant for journal articles, books\n and book chapters.\nexamples:1984'
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
    em.Column.define('country', em.builtin_types['text'], comment=column_comment['country'],
                     ),
    em.Column.define('id', em.builtin_types['text'], nullok=False, comment=column_comment['id'],
                     ),
    em.Column.define(
        'journal_abbrev', em.builtin_types['text'], comment=column_comment['journal_abbrev'],
    ),
    em.Column.define(
        'journal_id_ASTM', em.builtin_types['text'], comment=column_comment['journal_id_ASTM'],
    ),
    em.Column.define(
        'journal_id_CSD', em.builtin_types['text'], comment=column_comment['journal_id_CSD'],
    ),
    em.Column.define(
        'journal_id_ISSN', em.builtin_types['text'], comment=column_comment['journal_id_ISSN'],
    ),
    em.Column.define(
        'journal_issue', em.builtin_types['text'], comment=column_comment['journal_issue'],
    ),
    em.Column.define(
        'journal_volume', em.builtin_types['text'], comment=column_comment['journal_volume'],
    ),
    em.Column.define(
        'page_first', em.builtin_types['text'], comment=column_comment['page_first'],
    ),
    em.Column.define('page_last', em.builtin_types['text'], comment=column_comment['page_last'],
                     ),
    em.Column.define(
        'pdbx_database_id_DOI',
        em.builtin_types['text'],
        comment=column_comment['pdbx_database_id_DOI'],
    ),
    em.Column.define(
        'pdbx_database_id_PubMed',
        em.builtin_types['int4'],
        comment=column_comment['pdbx_database_id_PubMed'],
    ),
    em.Column.define('title', em.builtin_types['text'], comment=column_comment['title'],
                     ),
    em.Column.define('year', em.builtin_types['int4'], comment=column_comment['year'],
                     ),
    em.Column.define('Owner', em.builtin_types['text'], comment=column_comment['Owner'],
                     ),
]

table_display = {'row_name': {'row_markdown_pattern': '{{{id}}}'}}

visible_foreign_keys = {
    'detailed': [
        ['PDB', 'citation_author_citation_id_fkey'], ['PDB', 'software_citation_id_fkey'],
        ['PDB', 'ihm_3dem_restraint_fitting_method_citation_id_fkey'],
        ['PDB', 'ihm_epr_restraint_fitting_method_citation_id_fkey']
    ],
    'filter': 'detailed'
}

visible_columns = {
    'entry': [
        {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'citation_structure_id_fkey']
            }, 'RID']
        }, 'country', 'id', 'journal_abbrev', 'journal_id_ASTM', 'journal_id_CSD',
        'journal_id_ISSN', 'journal_issue', 'journal_volume', 'page_first', 'page_last',
        'pdbx_database_id_DOI', 'pdbx_database_id_PubMed', 'title', 'year'
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'citation_structure_id_fkey']
            }, 'RID']
        }, 'country', 'id', 'journal_abbrev', 'journal_id_ASTM', 'journal_id_CSD',
        'journal_id_ISSN', 'journal_issue', 'journal_volume', 'page_first', 'page_last',
        'pdbx_database_id_DOI', 'pdbx_database_id_PubMed', 'title', 'year',
        ['PDB', 'citation_RCB_fkey'], ['PDB', 'citation_RMB_fkey'], 'RCT', 'RMT',
        ['PDB', 'citation_Owner_fkey']
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
    em.Key.define(['RID'], constraint_names=[['PDB', 'citation_RIDkey1']],
                  ),
    em.Key.define(['id', 'structure_id'], constraint_names=[['PDB', 'citation_primary_key']],
                  ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'citation_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'citation_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'citation_structure_id_fkey']],
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
