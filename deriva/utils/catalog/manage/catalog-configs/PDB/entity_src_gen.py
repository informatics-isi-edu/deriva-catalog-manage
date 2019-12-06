import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {}

table_name = 'entity_src_gen'

schema_name = 'PDB'

column_annotations = {
    'pdbx_gene_src_scientific_name': {},
    'RMT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Last Modified Time'
        },
        chaise_tags.immutable: None
    },
    'pdbx_alt_source_flag': {},
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
    'pdbx_src_id': {},
    'Owner': {},
    'entity_id': {},
    'gene_src_common_name': {},
    'gene_src_genus': {},
    'RCT': {
        chaise_tags.generated: None,
        chaise_tags.display: {
            'name': 'Creation Time'
        },
        chaise_tags.immutable: None
    }
}

column_comment = {
    'pdbx_src_id': 'type:int4\nThis data item is an ordinal identifier for entity_src_gen data records.',
    'Owner': 'Group that can update the record.',
    'pdbx_gene_src_scientific_name': 'type:text\nScientific name of the organism.\nexamples:Homo sapiens,ESCHERICHIA COLI\nHOMO SAPIENS\nSACCHAROMYCES CEREVISIAE',
    'gene_src_genus': 'type:text\nThe genus of the natural organism from which the gene was\n obtained.\nexamples:Homo,Saccharomyces,Escherichia',
    'pdbx_alt_source_flag': 'type:text\nThis data item identifies cases in which an alternative source\n modeled.',
    'entity_id': 'A reference to table entity.id.',
    'gene_src_common_name': 'type:text\nThe common name of the natural organism from which the gene was\n obtained.\nexamples:man,yeast,bacteria',
    'structure_id': 'A reference to table entry.id.'
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
        'entity_id', em.builtin_types['text'], nullok=False, comment=column_comment['entity_id'],
    ),
    em.Column.define(
        'gene_src_common_name',
        em.builtin_types['text'],
        comment=column_comment['gene_src_common_name'],
    ),
    em.Column.define(
        'gene_src_genus', em.builtin_types['text'], comment=column_comment['gene_src_genus'],
    ),
    em.Column.define(
        'pdbx_alt_source_flag',
        em.builtin_types['text'],
        comment=column_comment['pdbx_alt_source_flag'],
    ),
    em.Column.define(
        'pdbx_gene_src_scientific_name',
        em.builtin_types['text'],
        comment=column_comment['pdbx_gene_src_scientific_name'],
    ),
    em.Column.define(
        'pdbx_src_id',
        em.builtin_types['int4'],
        nullok=False,
        comment=column_comment['pdbx_src_id'],
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
                'outbound': ['PDB', 'entity_src_gen_structure_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'entity id',
            'comment': 'A reference to table entity.id.',
            'source': [{
                'outbound': ['PDB', 'entity_src_gen_entity_id_fkey']
            }, 'RID']
        }, 'gene_src_common_name', 'gene_src_genus',
        ['PDB', 'entity_src_gen_pdbx_alt_source_flag_term_fkey'], 'pdbx_gene_src_scientific_name',
        'pdbx_src_id'
    ],
    '*': [
        'RID', {
            'markdown_name': 'structure id',
            'comment': 'A reference to table entry.id.',
            'source': [{
                'outbound': ['PDB', 'entity_src_gen_structure_id_fkey']
            }, 'RID']
        }, {
            'markdown_name': 'entity id',
            'comment': 'A reference to table entity.id.',
            'source': [{
                'outbound': ['PDB', 'entity_src_gen_entity_id_fkey']
            }, 'RID']
        }, 'gene_src_common_name', 'gene_src_genus',
        ['PDB', 'entity_src_gen_pdbx_alt_source_flag_term_fkey'],
        'pdbx_gene_src_scientific_name', 'pdbx_src_id', ['PDB', 'entity_src_gen_RCB_fkey'],
        ['PDB', 'entity_src_gen_RMB_fkey'], 'RCT', 'RMT', ['PDB', 'entity_src_gen_Owner_fkey']
    ]
}

table_annotations = {chaise_tags.visible_columns: visible_columns, }

table_comment = None

table_acls = {}

table_acl_bindings = {}

key_defs = [
    em.Key.define(['RID'], constraint_names=[['PDB', 'entity_src_gen_RIDkey1']],
                  ),
    em.Key.define(
        ['pdbx_src_id', 'structure_id', 'entity_id'],
        constraint_names=[['PDB', 'entity_src_gen_primary_key']],
    ),
]

fkey_defs = [
    em.ForeignKey.define(
        ['pdbx_alt_source_flag'],
        'Vocab',
        'entity_src_gen_pdbx_alt_source_flag_term', ['ID'],
        constraint_names=[['PDB', 'entity_src_gen_pdbx_alt_source_flag_term_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['RCB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'entity_src_gen_RCB_fkey']],
    ),
    em.ForeignKey.define(
        ['RMB'],
        'public',
        'ERMrest_Client', ['ID'],
        constraint_names=[['PDB', 'entity_src_gen_RMB_fkey']],
    ),
    em.ForeignKey.define(
        ['structure_id'],
        'PDB',
        'entry', ['id'],
        constraint_names=[['PDB', 'entity_src_gen_structure_id_fkey']],
        on_update='CASCADE',
        on_delete='SET NULL',
    ),
    em.ForeignKey.define(
        ['structure_id', 'entity_id'],
        'PDB',
        'entity', ['structure_id', 'id'],
        constraint_names=[['PDB', 'entity_src_gen_entity_id_fkey']],
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
