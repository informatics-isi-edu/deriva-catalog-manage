from attrdict import AttrDict
import argparse

import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.core import ErmrestCatalog, get_credential

from config import groups

groups = AttrDict(groups)

self_service_policy = {
    "self_service_creator": {
        "types": ["update", "delete"],
        "projection": ["RCB"],
        "projection_type": "acl"
    },
    'self_service_owner': {
        "types": ["update", "delete"],
        "projection": ["Owner"],
        "projection_type": "acl"
    }
}

fkey_self_service_policy = {
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

class DerivaConfigError(Exception):
    def __init__(self, msg):
        self.msg = msg

def configure_ermrest_client(catalog, model, ):
    """
    Set up ermrest_client table so that it has readable names and uses the display name of the user as the row name.
    :param catalog: Ermrest catalog
    :return:
    """

    ermrest_client = model.schemas['public'].tables['ermrest_client']

    column_annotations = {
        'RCT': {chaise_tags.display: {'name': 'Creation Time'}},
        'RMT': {chaise_tags.display: {'name': 'Last Modified Time'}},
        'RCB': {chaise_tags.display: {'name': 'Created By'}},
        'RMB': {chaise_tags.display: {'name': 'Modified By'}},
        'id': {chaise_tags.display: {'name': 'ID'}},
        'full_name': {chaise_tags.display: {'name': 'Full Name'}},
        'display_name': {chaise_tags.display: {'name': 'Display Name'}},
        'email': {chaise_tags.display: {'name': 'Email'}}
    }

    # Set table and row name.
    ermrest_client.annotations.update({
        chaise_tags.display: {'name': 'Users'},
        chaise_tags.visible_columns: {'compact': ['id', 'full_name', 'email']},
        chaise_tags.table_display: {'row_name': {'row_markdown_pattern': '{{{display_name}}}'}}
    })

    for k, v in column_annotations.items():
        ermrest_client.column_definitions[k].annotations.update(v)

    ermrest_client.apply(catalog)
    return


def configure_baseline_catalog(catalog, set_policy=True):
    """
    Put catalog into standard configuration which includes:
    1) Setting default display mode to be to turn underscores to spaces.
    2) Set access control assuming admin, curator, writer, and reader groups.
    3) Configure ermrest_client to have readable names.

    :param catalog: Ermrest catalog
    :param set_policy: Set policy for catalog to support reader/writer/curator/admin groups.
    :return:
    """

    model = catalog.getCatalogModel()

    # Set up default name style for all schemas.
    for s in model.schemas.values():
        s.annotations[chaise_tags.display] = {'name_style': {'underline_space': True}}

    # modify local representation of catalog ACL config
    if set_policy:
        model.acls.update({
            "owner": [groups.admin],
            "insert": [groups.curator, groups.writer],
            "update": [groups.curator],
            "delete": [groups.curator],
            "select": [groups.writer, groups.reader],
            "enumerate": ["*"],
        })

    configure_ermrest_client(catalog, model)
    model.apply(catalog)

    return


def configure_selfserve_policy(catalog, table):
    """
    Set up a table so it has a self service policy.  Add an owner column if one is not present, and set the acl binding
    so that it follows the self service policy.

    :param catalog:
    :param table: An ermrest model table object on which the policy is to be set.
    :return:
    """

    if 'Owner' not in [i.name for i in table.column_definitions]:
        print('Adding owner column...')
        col_def = em.Column.define('Owner', em.builtin_types['text'], comment='Current owner of the record.')
        table.create_column(catalog, col_def)

    # Make table policy be self service, creators and owners can update.
    table.acl_bindings.update(self_service_policy)
    table.apply(catalog)


def configure_table_defaults(catalog, table, self_serve_policy=True):
    """
    This function adds the following basic configuration details to an existing table:
    1) Creates a self service modification policy in which creators can update update any row they create.  Optionally,
       an Owner column can be provided, which allows the creater of a row to delegate row ownership to a specific
       individual.
    2) Adds display annotations and foreign key declarations so that system columns RCB, RMB display in a user friendly
       way.
    :param catalog: ERMRest catalog
    :param table: ERMRest table object which is to be configured.
    :param self_serv_policy: If true, then configure the table to have a self service policy
    :return:
    """

    table_name = table.name
    schema_name = table.sname
    schema = catalog.getCatalogModel().schemas[schema_name]

    if self_serve_policy:
        configure_selfserve_policy(catalog, table)

    if chaise_tags.display not in schema.annotations:
        schema.annotations[chaise_tags.display]  = {}
    if 'name_style' not in schema.annotations[chaise_tags.display]:
        schema.annotations[chaise_tags.display].update({'name_style': {'underline_space': True}})

    # Set up foreign key to ermrest_client on RCB, RMB and Owner. If ermrest_client is configured, the
    # full name of the user will be used for the FK value.
    for col, display in [('RCB', 'Created By'), ('RMB', 'Modified By'), ('Owner', 'Owner')]:
        fk_name = '{}_{}_fkey'.format(table_name, col)
        try:
            # Delete old fkey if there is one laying around....
            f = table.foreign_keys[(schema_name, fk_name)]
            f.delete(catalog, table)
        except KeyError:
            pass
        fk = em.ForeignKey.define([col],
                                  'public', 'ermrest_client', ['id'],
                                  constraint_names=[(schema_name, fk_name)],
                                  )
        table.create_fkey(catalog, fk)

        # Add a display annotation so that we have sensible name for RCB and RMB.
        table.column_definitions[col].annotations.update({chaise_tags.display: {'name': display}})

    table.apply(catalog)
    return


def asset_map(schema_name, table_name, key_column):
    """
    Create an asset map.  Assume that there is a column in the metadata entry that correlates with the directory
     name that the asset is located.

    :param schema_name:
    :param table_name:
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
            'hatrac_templates': {'hatrac_uri':
                                     '/hatrac/{schema_name}/{table_name}_Asset/{table_rid}/{file_name}'
                                 },
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
    :return:
    """
    table_name = table.name
    schema_name = table.sname
    model = catalog.getCatalogModel()
    asset_table_name = '{}_Asset'.format(table_name)

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
    if set_policy:
        fkey_acls = {
            "insert": [groups.curator],
            "update": [groups.curator],
        }
        fkey_acl_bindings = fkey_self_service_policy
    else:
        fkey_acls, fkey_acl_bindings = {}, {}

    fkey_defs = [
        em.ForeignKey.define(['{}_RID'.format(table_name)],
                             schema_name, table_name, ['RID'],
                             acls=fkey_acls, acl_bindings=fkey_acl_bindings,
                             constraint_names=[(schema_name, '()_{}_fkey'.format(asset_table_name, table_name))],
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


def main():
    parser = argparse.ArgumentParser(description="Configure an Ermrest Catalog")
    parser.add_argument('server', help='Catalog server name')
    parser.add_argument('--catalog_id', default=1, help="ID number of desired catalog (Default:1)")
    parser.add_argument("--catalog", action='store_true', help='Configure a catalog')
    parser.add_argument('--table', default=None, metavar='SCHEMA_NAME:TABLE_NAME',
    help='Name of table to be configured (Default:tabledata filename)')
    parser.add_argument('--asset_table', default=None, metavar='KEY_COLUMN',
                        help='Create an asset table linked to table on key_column')
    parser.add_argument('--config', default=None, help='python script to set up configuration variables)')

    args = parser.parse_args()

    credentials = get_credential(args.server)
    catalog = ErmrestCatalog('https', args.server, args.catalog_id, credentials=credentials)

    if args.catalog:
        configure_baseline_catalog(catalog)
    if args.table:
        [schema_name, table_name] = args.table.split(':')
        table = catalog.getCatalogModel().schemas[schema_name].tables[table_name]
        configure_table_defaults(catalog, table)
    if args.asset_table:
        if not args.table:
            print('Creating asset table requires specfication of a table')
            exit(1)
        create_asset_table(catalog, table, args.asset_table)


if __name__ == "__main__":
    main()
