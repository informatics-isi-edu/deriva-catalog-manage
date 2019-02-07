import argparse
from attrdict import AttrDict
from deriva.core import ErmrestCatalog, get_credential, DerivaPathError
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args
from deriva.core.ermrest_config import tag as chaise_tags
import deriva.core.ermrest_model as em

groups = {
    'isrd-systems': 'https://auth.globus.org/3938e0d0-ed35-11e5-8641-22000ab4b42b',
    'test-writer': 'https://auth.globus.org/646933ac-16f6-11e9-b9af-0edc9bdd56a6',
    'test-curator': 'https://auth.globus.org/86cd6ee0-16f6-11e9-b9af-0edc9bdd56a6',
    'test-reader': 'https://auth.globus.org/4966c7fe-16f6-11e9-8bb8-0ee7d80087ee'
}

bulk_upload = {
    'asset_mappings': [
        {
            'asset_type': 'table',
            'ext_pattern': '^.*[.](?P<file_ext>json|csv)$',
            'file_pattern': '^((?!/assets/).)*/records/(?P<schema>WWW?)/(?P<table>Page)[.]',
            'target_table': ['WWW', 'Page'],
            'default_columns': ['RID', 'RCB', 'RMB', 'RCT', 'RMT']
        },
        {
            'column_map': {
                'MD5': '{md5}',
                'URL': '{URI}',
                'Length': '{file_size}',
                'Filename': '{file_name}',
                'Page_RID': '{table_rid}'
            },
            'dir_pattern': '^.*/(?P<schema>WWW)/(?P<table>Page)/(?P<key_column>.*)/',
            'ext_pattern': '^.*[.](?P<file_ext>.*)$',
            'file_pattern': '.*',
            'target_table': ['WWW', 'Page_Asset'],
            'checksum_types': ['md5'],
            'hatrac_options': {
                'versioned_uris': True
            },
            'hatrac_templates': {
                'hatrac_uri': '/hatrac/{schema}/{table}/{md5}.{file_name}'
            },
            'record_query_template': '/entity/{schema}:{table}_Asset/{table}_RID={table_rid}/MD5={md5}/URL={URI_urlencoded}',
            'metadata_query_templates': [
                '/attribute/D:={schema}:{table}/RID={key_column}/table_rid:=D:RID'
            ]
        },
        {
            'asset_type': 'table',
            'ext_pattern': '^.*[.](?P<file_ext>json|csv)$',
            'file_pattern': '^((?!/assets/).)*/records/(?P<schema>TestSchema?)/(?P<table>Foo)[.]',
            'target_table': ['TestSchema', 'Foo'],
            'default_columns': ['RID', 'RCB', 'RMB', 'RCT', 'RMT']
        },
        {
            'column_map': {
                'MD5': '{md5}',
                'URL': '{URI}',
                'Length': '{file_size}',
                'Foo_RID': '{table_rid}',
                'Filename': '{file_name}'
            },
            'dir_pattern': '^.*/(?P<schema>TestSchema)/(?P<table>Foo)/(?P<key_column>.*)/',
            'ext_pattern': '^.*[.](?P<file_ext>.*)$',
            'file_pattern': '.*',
            'target_table': ['TestSchema', 'Foo_Asset'],
            'checksum_types': ['md5'],
            'hatrac_options': {
                'versioned_uris': True
            },
            'hatrac_templates': {
                'hatrac_uri': '/hatrac/{schema}/{table}/{md5}.{file_name}'
            },
            'record_query_template': '/entity/{schema}:{table}_Asset/{table}_RID={table_rid}/MD5={md5}/URL={URI_urlencoded}',
            'metadata_query_templates': [
                '/attribute/D:={schema}:{table}/ID={key_column}/table_rid:=D:RID'
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
    'create': [],
    'update': [groups['test-curator']],
    'write': [],
    'insert': [groups['test-curator'], groups['test-writer']],
    'owner': [groups['isrd-systems']],
    'delete': [groups['test-curator']],
    'select': [groups['test-writer'], groups['test-reader']],
    'enumerate': ['*']
}


def main(catalog, mode, replace=False):
    updater = CatalogUpdater(catalog)
    updater.update_catalog(mode, annotations, acls, replace=replace)


if __name__ == "__main__":
    server = 'dev.isrd.isi.edu'
    catalog_id = 55674
    mode, replace, server, catalog_id = parse_args(server, catalog_id, is_catalog=True)
    credential = get_credential(server)
    catalog = ErmrestCatalog('https', server, catalog_id, credentials=credential)
    main(catalog, mode, replace)

