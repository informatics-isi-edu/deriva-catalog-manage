import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential, DerivaPathError
from deriva.utils.catalog.components.deriva_model import DerivaCatalog
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args
from deriva.core.ermrest_config import tag as chaise_tags
import deriva.core.ermrest_model as em

groups = {}

catalog_config = {
    'name': 'pdb',
    'groups': {
        'admin': 'https://auth.globus.org/0b98092c-3c41-11e9-a8c8-0ee7d80087ee',
        'reader': 'https://auth.globus.org/8875a770-3c40-11e9-a8c8-0ee7d80087ee',
        'curator': 'https://auth.globus.org/eef3e02a-3c40-11e9-9276-0edc9bdd56a6',
        'writer': 'https://auth.globus.org/c94a1e5c-3c40-11e9-a5d1-0aacc65bfe9a'
    }
}

bulk_upload = {
    'asset_mappings': [
        {
            'asset_type': 'table',
            'default_columns': ['RID', 'RCB', 'RMB', 'RCT', 'RMT'],
            'target_table': ['WWW', 'Page'],
            'ext_pattern': '^.*[.](?P<file_ext>json|csv)$',
            'file_pattern': '^((?!/assets/).)*/records/(?P<schema>WWW?)/(?P<table>Page)[.]'
        }, {
            'record_query_template': '/entity/{schema}:{table}_Asset/{table}={table_rid}/MD5={md5}/URL={URI_urlencoded}',
            'hatrac_options': {
                'versioned_uris': True
            },
            'metadata_query_templates': [
                '/attribute/D:={schema}:{table}/RID={key_column}/table_rid:=D:RID'
            ],
            'hatrac_templates': {
                'hatrac_uri': '/hatrac/{schema}/{table}/{md5}.{file_name}'
            },
            'target_table': ['WWW', 'Page_Asset'],
            'file_pattern': '.*',
            'checksum_types': ['md5'],
            'dir_pattern': '^.*/(?P<schema>WWW)/(?P<table>Page)/(?P<key_column>.*)/',
            'column_map': {
                'MD5': '{md5}',
                'Length': '{file_size}',
                'Filename': '{file_name}',
                'Page': '{table_rid}',
                'URL': '{URI}'
            },
            'ext_pattern': '^.*[.](?P<file_ext>.*)$'
        }
    ],
    'version_compatibility': [['>=0.4.3', '<1.0.0']],
    'version_update_url': 'https://github.com/informatics-isi-edu/deriva-qt/releases'
}

annotations = {chaise_tags.catalog_config: catalog_config, chaise_tags.bulk_upload: bulk_upload, }

acls = {}


def main(catalog, mode, replace=False):
    updater = CatalogUpdater(catalog)
    updater.update_catalog(mode, annotations, acls, replace=replace)


if __name__ == "__main__":
    host = 'pdb.isrd.isi.edu'
    catalog_id = 9
    mode, replace, host, catalog_id = parse_args(host, catalog_id, is_catalog=True)
    catalog = DerivaCatalog(host, catalog_id=catalog_id, validate=False)
    main(catalog, mode, replace)
