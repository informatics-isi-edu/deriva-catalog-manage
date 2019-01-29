import argparse
import sys

import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.core import ErmrestCatalog, get_credential
from deriva.utils.catalog.manage.configure_catalog import configure_table_defaults

from requests import exceptions

IS_PY2 = (sys.version_info[0] == 2)
IS_PY3 = (sys.version_info[0] == 3)

if IS_PY3:
    from urllib.parse import urlparse
else:
    from urlparse import urlparse

CATALOG_CONFIG__TAG = 'tag:isrd.isi.edu,2019:catalog-config'


class DerivaConfigError(Exception):
    def __init__(self, msg):
        self.msg = msg

def asset_map(schema_name, table_name, key_column):
    """
    Create an asset map.  Assume that there is a column in the metadata entry that correlates with the directory
     name that the asset is located.

    :param schema_name:
    :param table_name:
    :param key_column: Column used to correlate asset to metadata
    :return:
    """
    asset_table_name = '{}_Asset'.format(table_name)
    asset_mappings = [
        {
            'default_columns': ['RID', 'RCB', 'RMB', 'RCT', 'RMT'],
            'ext_pattern': '^.*[.](?P<file_ext>json|csv)$',
            'asset_type': 'table',
            'file_pattern': '^((?!/assets/).)*/records/(?P<schema>.+?)/(?P<table>.+?)[.]'
        },
        {
            'checksum_types': ['md5'],
            'column_map': {
                'URL': '{URI}',
                'Length': '{file_size}',
                table_name + '_RID': '{table_rid}',
                'Filename': '{file_name}',
                'MD5': '{md5}',
                key_column: '{key_column}'
            },
            'create_record_before_upload': 'False',
            'dir_pattern': '^.*/(?P<schema_name>.*)/(?P<table_name>.*)/(?P<key_column>[0-9A-Z-]+)/)',
            'ext_pattern': '.*$',
            'file_pattern': '.*',
            'hatrac_templates': {'hatrac_uri': '/hatrac/{schema_name}/{table_name}_Asset/{table_rid}/{file_name}'},
            # Look for rows in the metadata table with matching key column values.
            'metadata_query_templates': [
                '/attribute/D:={schema_name}:{table_name}/%s={key_column}/table_rid:=D:RID' % key_column],
            'record_query_template':
                '/entity/{target_table}/{table_name}_RID={table_rid}/MD5={md5}/URL={URI_urlencoded}',
            'target_table': [schema_name, asset_table_name],
            'hatrac_options': {'versioned_uris': 'True'},
        }
    ]
    return asset_mappings


def create_asset_table(catalog, table, key_column, set_policy=True):
    """
    Create a basic asset table and configure upload script to load the table along with a table of associated
    metadata.
    :param catalog:
    :param table: Table to contain the asset metadata.  Asset will have a foreign key to this table.
    :param key_column: The column in the metadata table to be used to correlate assets with entries. Assets will be
    named using the key column.
    :param set_policy: If true, add ACLs for self serve policy to the asset table
    :return:
    """
    table_name = table.name
    schema_name = table.sname
    model = catalog.getCatalogModel()
    asset_table_name = '{}_Asset'.format(table_name)

    if CATALOG_CONFIG__TAG not in model.annotations:
        raise DerivaConfigError(msg='Attempting to configure table before catalog is configured')

    if key_column not in [i.name for i in table.column_definitions]:
        raise DerivaConfigError(msg='Key column not found in target table')

    column_annotations = {
        'URL': {
            chaise_tags.asset: {
                'filename_column': 'Filename',
                'byte_count_column': 'Length',
                'url_pattern': '/hatrac/%s/%s/{{{%s_RID}}}/{{{_URL.filename}}}' %
                               (schema_name, asset_table_name, table_name),
                'md5': 'MD5'
            },
            chaise_tags.column_display: {'*': {'markdown_pattern': '[**{{Filename}}**]({{{URL}}})'}}
        },
        'Filename': {
            chaise_tags.column_display: {'*': {'markdown_pattern': '[**{{Filename}}**]({{{URL}}})'}}
        }
    }

    column_defs = [
        em.Column.define('{}_RID'.format(table_name),
                         em.builtin_types['text'],
                         nullok=False,
                         comment="The {} entry to which this asset is attached".format(table_name)),
        em.Column.define('URL', em.builtin_types['text'],
                         nullok=False,
                         annotations=column_annotations['URL']),
        em.Column.define('Filename', em.builtin_types['text'], annotations=column_annotations['Filename']),
        em.Column.define('Content_Type', em.builtin_types['text'], nullok=True, comment='Content type of the asset'),
        em.Column.define('Description', em.builtin_types['markdown']),
        em.Column.define('Length', em.builtin_types['int8'], nullok=False, comment='Asset length (bytes)'),
        em.Column.define('MD5', em.builtin_types['text'], nullok=False)
    ]

    key_defs = [em.Key.define(['Filename'],
                              constraint_names=[(schema_name, '{}_Filename_key'.format(asset_table_name))],
                              comment='Key constraint to ensure file names in the table are unique')]

    fkey_acls, fkey_acl_bindings = {}, {}
    if set_policy:
        groups = get_core_groups(catalog, model)
        fkey_acls = {
            "insert": [groups['curator']],
            "update": [groups['curator']],
        }
        fkey_acl_bindings = {
            "self_linkage_creator": {
                "types": ["insert", "update"],
                "projection": ["RCB"],
                "projection_type": "acl",
            },
            "self_linkage_owner": {
                "types": ["insert", "update"],
                "projection": ["Owner"],
                "projection_type": "acl",
            }
        }

    fkey_defs = [
        em.ForeignKey.define(['{}_RID'.format(table_name)],
                             schema_name, table_name, ['RID'],
                             acls=fkey_acls, acl_bindings=fkey_acl_bindings,
                             constraint_names=[(schema_name, '{}_{}_fkey'.format(asset_table_name, table_name))],
                             )
    ]
    table_annotations = {chaise_tags['table_display']: {'row_name': {'row_markdown_pattern': '{{{Filename}}}'}}}

    table_def = em.Table.define(asset_table_name, column_defs,
                                key_defs=key_defs, fkey_defs=fkey_defs,
                                annotations=table_annotations,
                                comment='Asset table for {}'.format(table_name))

    asset_table = model.schemas[schema_name].create_table(catalog, table_def)
    configure_table_defaults(catalog, asset_table)

    # The last thing we should do is update the upload spec to accomidate this new asset table.
    if chaise_tags.bulk_upload not in model.annotations:
        model.annotations.update({
            chaise_tags.bulk_upload: {
                'asset_mappings': [],
                'version_update_url': 'https://github.com/informatics-isi-edu/deriva-qt/releases',
                'version_compatibility': [['>=0.4.3', '<1.0.0']]
            }
        }
        )
    model.annotations[chaise_tags.bulk_upload]['asset_mappings'].extend(asset_map(schema_name, table_name, key_column))

    model.apply(catalog)
    return asset_table

def link_tables(s1,t1,s2,t2):
    """
    Create a pure binary association table that connects rows in table one to rows in table 2.  Assume that RIDs are
    used for linking
    :param s1:
    :param t1:
    :param s2:
    :param t2:
    :return:
    """
    fkey_defs = [
        em.ForeignKey.define(['{}_RID'.format(table1_name)],
                             schema1_name, table1_name, ['RID'],
                             acls=fkey_acls, acl_bindings=fkey_acl_bindings,
                             constraint_names=[(schema_name, '()_{}_fkey'.format(asset_table_name, table_name))],
                             )
        em.ForeignKey.define(['{}_RID'.format(table_name)],
                             schema2_name, table2_name, ['RID'],
                             acls=fkey_acls, acl_bindings=fkey_acl_bindings,
                             cons
    ]
    table_def = em.Table.define(table_name, column_defs,
                                key_defs=key_defs, fkey_defs=fkey_defs,
                                annotations=table_annotations,
                                comment='Asset table for {}'.format(table_name))



def rename_column(catalog, table, from_column, to_column):

    # Check to make sure that this column is not part of a FK definition.

    # Create a new column_spec from the existing spec.
    # Copy over the old values
    # Udate annotations where the old spec was being used
    # Delete old column
    # Patch up
    return


def main():
    parser = argparse.ArgumentParser(description="Configure an Ermrest Catalog")
    parser.add_argument('server', help='Catalog server name')
    parser.add_argument('--catalog_id', default=1, help="ID number of desired catalog (Default:1)")

    parser.add_argument('--table', default=None, metavar='SCHEMA_NAME:TABLE_NAME',
                        help='Name of table to be configured')
    parser.add_argument('--asset_table', default=None, metavar='KEY_COLUMN',
                        help='Create an asset table linked to table on key_column')
    parser.add_argument('--config', default=None, help='python script to set up configuration variables)')

    args = parser.parse_args()

    credentials = get_credential(args.server)
    catalog = ErmrestCatalog('https', args.server, args.catalog_id, credentials=credentials)

    if args.table:
        [schema_name, table_name] = args.table.split(':')
        table = catalog.getCatalogModel().schemas[schema_name].tables[table_name]
    if args.asset_table:
        if not args.table:
            print('Creating asset table requires specification of a table')
            exit(1)
        create_asset_table(catalog, table, args.asset_table)


if __name__ == "__main__":
    main()
