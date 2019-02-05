import argparse
from attrdict import AttrDict
from deriva.core import ErmrestCatalog, get_credential, DerivaPathError
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args
from deriva.core.ermrest_config import tag as chaise_tags
import deriva.core.ermrest_model as em

groups = {
    'isrd-systems': 'https://auth.globus.org/3938e0d0-ed35-11e5-8641-22000ab4b42b',
    'test-reader': 'https://auth.globus.org/4966c7fe-16f6-11e9-8bb8-0ee7d80087ee',
    'test-writer': 'https://auth.globus.org/646933ac-16f6-11e9-b9af-0edc9bdd56a6',
    'test-curator': 'https://auth.globus.org/86cd6ee0-16f6-11e9-b9af-0edc9bdd56a6'
}

bulk_upload = {
    'asset_mappings': [
        {
            'asset_type': 'table',
            'ext_pattern': '^.*[.](?P<file_ext>json|csv)$',
            'file_pattern': '^((?!/assets/).)*/records/(?P<schema>.+?)/(?P<table>.+?)[.]',
            'default_columns': ['RID', 'RCB', 'RMB', 'RCT', 'RMT']
        },
        {
            'column_map': {
                'MD5': '{MD5}',
                'URL': '{URI}',
                'Length': '{file_size}',
                'Foo_RID': '{table_rid}',
                'Filename': '{Filename}'
            },
            'dir_pattern': '^.*/(?P<schema>.*)/(?P<table>.*)/(?P<key_column>[0-9A-Z-]+/))',
            'ext_pattern': '^.*[.](?P<file_ext>.*)$',
            'file_pattern': '.*',
            'checksum_types': ['md5'],
            'hatrac_options': {
                'versioned_uris': True
            },
            'hatrac_templates': {
                'hatrac_uri': '/hatrac/{schema}/{table}/{md5}.{file_name}'
            },
            'record_query_template': '/entity/{schema}:{table}_Asset/{table}_RID={table_rid}/MD5={md5}/URL={URI_urlencoded}',
            'metadata_query_templates': [
                '/attribute/D:={target_table}/Id={key_column}/table_rid:=D:RID'
            ]
        }
    ],
    'version_update_url': 'https://github.com/informatics-isi-edu/deriva-qt/releases',
    'version_compatibility': [['>=0.4.3', '<1.0.0']]
}

annotations = {
    chaise_tags.bulk_upload: bulk_upload,
    'tag:isrd.isi.edu,2019:catalog-config': {
        'name': 'test',
        'groups': {
            'admin': 'https://auth.globus.org/3938e0d0-ed35-11e5-8641-22000ab4b42b',
            'reader': 'https://auth.globus.org/4966c7fe-16f6-11e9-8bb8-0ee7d80087ee',
            'writer': 'https://auth.globus.org/646933ac-16f6-11e9-b9af-0edc9bdd56a6',
            'curator': 'https://auth.globus.org/86cd6ee0-16f6-11e9-b9af-0edc9bdd56a6'
        }
    },
}

acls = {
    'owner': [groups['isrd-systems']],
    'select': [groups['test-writer'], groups['test-reader']],
    'insert': [groups['test-curator'], groups['test-writer']],
    'enumerate': ['*'],
    'delete': [groups['test-curator']],
    'write': [],
    'update': [groups['test-curator']],
    'create': []
}


def main(catalog, mode, replace=False):
    updater = CatalogUpdater(catalog)
    updater.update_catalog(mode, annotations, acls, replace=replace)


if __name__ == "__main__":
    server = 'dev.isrd.isi.edu'
    catalog_id = 55507
    mode, replace, server, catalog_id = parse_args(server, catalog_id, is_catalog=True)
    credential = get_credential(server)
    catalog = ErmrestCatalog('https', server, catalog_id, credentials=credential)
    main(catalog, mode, replace)

