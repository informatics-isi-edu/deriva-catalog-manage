import argparse
import sys

import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.core import ErmrestCatalog, get_credential

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


def configure_ermrest_client(catalog, model, groups, anonymous=False):
    """
    Set up ermrest_client table so that it has readable names and uses the display name of the user as the row name.
    :param catalog: Ermrest catalog
    :param model:
    :param groups:
    :return:
    """

    ermrest_client = model.schemas['public'].tables['ERMrest_Client']

    # Make ermrest_client table visible to members of the reader group. By default, this is not
    ermrest_client.acls['select'] = [groups['reader'], groups['writer'], groups['curator'], groups['admin']] \
        if not anonymous else ['*']

    # Set table and row name.
    ermrest_client.annotations.update({
        chaise_tags.display: {'name': 'Users'},
        chaise_tags.visible_columns: {'compact': ['ID', 'Full_Name', 'Email']},
        chaise_tags.table_display: {'row_name': {'row_markdown_pattern': '{{{Display_Name}}}'}}
    })

    column_annotations = {
        'RCT': {chaise_tags.display: {'name': 'Creation Time'}},
        'RMT': {chaise_tags.display: {'name': 'Last Modified Time'}},
        'RCB': {chaise_tags.display: {'name': 'Created By'}},
        'RMB': {chaise_tags.display: {'name': 'Modified By'}}
    }
    for k, v in column_annotations.items():
        ermrest_client.column_definitions[k].annotations.update(v)

    ermrest_client.apply(catalog)
    return


def update_group_table(catalog):
    pb = catalog.getPathBuilder()
    # Attempt to add URL.  This can go away once we have URL entered by ERMrest.
    pb.public.ERMrest_Group.update(
        [{'RID': i['RID'], 'URL': group_urls(i['ID'])[0]} for i in pb.public.ERMrest_Group.entities()]
    )


def get_core_groups(catalog, model, catalog_name=None, admin=None, curator=None, writer=None, reader=None,
                    replace=False):
    """
    Look in the catalog to get the group IDs for the four core groups used in the baseline configuration.
    :param catalog:
    :param model:
    :param catalog_name: Name of the catalog to use as a prefix in looking up default name of the group. Default
           group names are formed by combining the catalog_name with the standard group name: e.g. foo-admin
           foo-writer, and foo-reader
    :param admin: Group name to use in place of default
    :param curator: Group name to use in place of default
    :param writer: Group name to use in lace of default
    :param reader: Either '*' for anonymous read access, or the group name to use in place of default
    :param replace: Ignore existing catalog config and use provided arguements.
    :return: dictionary with the four group ids.
    """
    groups = {}
    # Get previous catalog configuration values if they exist
    if CATALOG_CONFIG__TAG in model.annotations and not replace:
        groups.update({
            'admin': model.annotations[CATALOG_CONFIG__TAG]['groups']['admin'],
            'curator': model.annotations[CATALOG_CONFIG__TAG]['groups']['curator'],
            'writer': model.annotations[CATALOG_CONFIG__TAG]['groups']['writer'],
            'reader': model.annotations[CATALOG_CONFIG__TAG]['groups']['reader']
        })
    else:
        if admin == '*' or curator == '*' or writer == '*':
            raise DerivaConfigError(msg='Only reader may be anonymous when setting core catalog groups')
        if not catalog_name and (admin is None or curator is None or writer is None or reader is None):
            raise DerivaConfigError(msg='Catalog name required to look up group')

        if admin is None:
            admin = catalog_name + '-admin'
        if curator is None:
            curator = catalog_name + '-curator'
        if writer is None:
            writer = catalog_name + '-writer'
        if reader is None:
            reader = catalog_name + '-reader'

        pb = catalog.getPathBuilder()
        catalog_groups = {i['Display_Name']: i for i in pb.public.ERMrest_Group.entities()}
        groups = {}
        try:
            groups['admin'] = catalog_groups[admin]['ID']
            groups['curator'] = catalog_groups[curator]['ID']
            groups['writer'] = catalog_groups[writer]['ID']
            groups['reader'] = catalog_groups[reader]['ID'] if reader is not '*' else '*'
        except KeyError as e:
            raise DerivaConfigError(msg='Group {} not defined'.format(e.args[0]))
    return groups


def group_urls(group):
    guid = group.split('/')[-1]
    link = 'https://app.globus.org/groups/' + guid
    uri = 'https://auth.globus.org/' + guid
    return link, uri


def configure_group_table(catalog, model, groups, anonymous=False):
    """
    Create a table in the public schema for tracking mapping of group names.
    :param catalog:
    :param model:
    :param groups:
    :param anonymous: Set to true if anonymous read access is to be allowed.
    :return:
    """

    ermrest_group = model.schemas['public'].tables['ERMrest_Group']

    # Make ERMrest_Group table visible to writers, curators, and admins.
    ermrest_group.acls['select'] = [groups['writer'], groups['curator'], groups['admin']]

    configure_table_defaults(catalog, ermrest_group, self_serve_policy=False)

    # Set table and row name.
    ermrest_group.annotations.update({
        chaise_tags.display: {'name': 'Groups'},
        chaise_tags.visible_columns: {'*': ['Display_Name', 'ID', 'URL', 'Description']},
        chaise_tags.table_display: {'row_name': {'row_markdown_pattern': '{{{Display_Name}}}'}}
    })

    # Set compound key so that we can link up with Visible_Group table.
    ermrest_group.create_key(
        catalog,
        em.Key.define(['ID', 'URL', 'Display_Name', 'Description'],
                      constraint_names=['Group_Compound_key'],
                      comment='Compound key to ensure that columns sync up into Visible_Groups on update.'

                      )
    )
    ermrest_group.apply(catalog)

    # Create a visible groups table
    column_defs = [
        em.Column.define('ID', em.builtin_types['text'], nullok=False),
        em.Column.define('URL', em.builtin_types['text'],
                         annotations={
                             chaise_tags.column_display: {
                                 '*': {'markdown_pattern': '[**{{Display_Name}}**]({{{URL}}})'}},
                             chaise_tags.display: {'name': 'Group Management Page'}
                         }
                         ),
        em.Column.define('Display_Name', em.builtin_types['text']),
        em.Column.define('Description', em.builtin_types['text'])
    ]

    # Set up a foreign key to the group table so that the creator of a record can only select
    # groups of which they are members of for values of the Owners column.
    fkey_group_policy = {
        # FKey to group can be created only if you are a member of the group you are referencing
        'set_owner': {"types": ["update", "insert"],
                      "projection": ["ID"],
                      "projection_type": "acl"}
    }

    # Allow curators to also update the foreign key.
    fkey_group_acls = {"insert": [groups['curator']], "update": [groups['curator']]}

    # Create a foreign key to the group table. Set update policy to keep group entry in sync.
    fkey_defs = [
        em.ForeignKey.define(['ID', 'URL', 'Display_Name', 'Description'],
                             'public', 'ERMrest_Group', ['ID', 'URL', 'Display_Name', 'Description'],
                             on_update='CASCADE',
                             acls=fkey_group_acls,
                             acl_bindings=fkey_group_policy,
                             )
    ]
    # Create the visible groups table. Set ACLs so that writers or curators can add entries or edit.  Allow writers
    # to be able to create new entries.  No one is allowed to update, as this is only done via the CASCADE.
    visible_groups = em.Table.define(
        'Visible_Groups',
        column_defs=column_defs,
        fkey_defs=fkey_defs,
        acls={
            # Make ERMrest_Group table visible to members of the group members, curators, and admins.
            'select': [groups['reader']],
            'insert': [groups['writer'], groups['curator']]
        },
    )
    model.schemas['public'].create_table(catalog, visible_groups)


def configure_self_serve_policy(catalog, table, groups):
    """
    Set up a table so it has a self service policy.  Add an owner column if one is not present, and set the acl binding
    so that it follows the self service policy.

    :param catalog:
    :param table: An ermrest model table object on which the policy is to be set.
    :param groups: dictionary of core catalog groups

    :return:
    """
    table_name = table.name
    schema_name = table.sname

    # Configure table so that access can be assigned to a group.  This requires that we create a column and establish
    # a foreign key to an entry in the group table.  We will set the access control on the foreign key so that you
    # are only able to delagate access to a the creator of the entity belongs to.
    if 'Owner' not in [i.name for i in table.column_definitions]:
        print('Adding owner column...')
        col_def = em.Column.define('Owner', em.builtin_types['text'], comment='Group that can update the record.')
        table.create_column(catalog, col_def)

    # Now configure the policy on the table...
    self_service_policy = {
        # Set up a policy for the table that allows the creator of the record to update and delete the record.
        "self_service_creator": {
            "types": ["update", "delete"],
            "projection": ["RCB"],
            "projection_type": "acl"
        },
        # Set up a policy for the table that allows members of the group referenced by the Owner column to update
        # and delete the record.
        'self_service_group': {
            "types": ["update", "delete"],
            "projection": ["Owner"],
            "projection_type": "acl"
        }
    }

    # Make table policy be self service, creators and owners can update.
    table.acl_bindings.update(self_service_policy)

    # Set up a foreign key to the group table on the owners column so that the creator of a record can only select
    # groups of which they are members of for values of the Owners column.
    fkey_group_policy = {
        # FKey to group can be created only if you are a member of the group you are referencing
        'set_owner': {"types": ["update", "insert"],
                      "projection": ["ID"],
                      "projection_type": "acl"}
    }

    # Allow curators to also update the foreign key.
    fkey_group_acls = {"insert": [groups['curator']], "update": [groups['curator']]}

    owner_fkey_name = '{}_Visible_Group_fkey'.format(table_name)
    fk = em.ForeignKey.define(['Owner'],
                              'public', 'Visible_Group', ['ID'],

                              acls=fkey_group_acls, acl_bindings=fkey_group_policy,
                              constraint_names=[(schema_name, owner_fkey_name)],
                              )
    try:
        # Delete old fkey if there is one laying around....
        f = table.foreign_keys[(schema_name, owner_fkey_name)]
        f.delete(catalog, table)
    except KeyError:
        pass

    # Now create the foreign key to the group table.
    table.create_fkey(catalog, fk)

    table.apply(catalog)


def configure_baseline_catalog(catalog, catalog_name=None,
                               admin=None, curator=None, writer=None, reader=None,
                               set_policy=True, anonymous=False):
    """
    Put catalog into standard configuration which includes:
    1) Setting default display mode to be to turn underscores to spaces.
    2) Set access control assuming admin, curator, writer, and reader groups.
    3) Configure ermrest_client to have readable names.

    :param catalog: Ermrest catalog
    :param catalog_name:
    :param admin: Name of the admin group.  Defaults to catalog-admin
    :param curator: Name of the curator group. Defaults to catalog-curator
    :param writer: Name of the writer group. Defaults to catalog-writer
    :param reader: Name of the reader group. Defaults to catalog-reader
    :param set_policy: Set policy for catalog to support reader/writer/curator/admin groups.
    :param anonymous: Set to true if anonymous read access should be allowed.
    :return:
    """

    model = catalog.getCatalogModel()
    if not catalog_name:
        # If catalog name is not provided, default to the host name of the server.
        catalog_name = urlparse(catalog.get_server_uri()).hostname.split('.')[0]
    groups = get_core_groups(catalog, model, catalog_name=catalog_name,
                             admin=admin, curator=curator, writer=writer, reader=reader)

    # Record configuration of catalog so we can retrieve when we configure tables later on.
    model.annotations[CATALOG_CONFIG__TAG] = {'name': catalog_name, 'groups': groups}
    model.apply(catalog)

    # Set up default name style for all schemas.
    for s in model.schemas.values():
        s.annotations[chaise_tags.display] = {'name_style': {'underline_space': True}}

    # modify catalog ACL config to support basic admin/curator/writer/reader access.
    if set_policy:
        model.acls.update({
            "owner": [groups['admin']],
            "insert": [groups['curator'], groups['writer']],
            "update": [groups['curator']],
            "delete": [groups['curator']],
            "select": [groups['writer'], groups['reader']] if not anonymous else ['*'],
            "enumerate": ["*"],
        })

    configure_ermrest_client(catalog, model, groups)
    configure_group_table(catalog, model, groups)

    model.apply(catalog)

    return


def configure_table_defaults(catalog, table, self_serve_policy=True):
    """
    This function adds the following basic configuration details to an existing table:
    1) Creates a self service modification policy in which creators can update update any row they create.  Optionally,
       an Owner column can be provided, which allows the creater of a row to delegate row ownership to a specific
       group.
    2) Adds display annotations and foreign key declarations so that system columns RCB, RMB display in a user friendly
       way.
    :param catalog: ERMRest catalog
    :param table: ERMRest table object which is to be configured.
    :param self_serve_policy: If true, then configure the table to have a self service policy
    :return:
    """
    model_root = catalog.getCatalogModel()
    if CATALOG_CONFIG__TAG not in model_root.annotations:
        raise DerivaConfigError(msg='Attempting to configure table before catalog is configured')

    # Hack to update description and URL until we get these passed through ermrest....
    update_group_table(catalog)

    table_name = table.name
    schema_name = table.sname
    schema = model_root.schemas[schema_name]

    if self_serve_policy:
        configure_self_serve_policy(catalog, table, get_core_groups(catalog, model_root))

    # Configure schema if not already done so.
    if chaise_tags.display not in schema.annotations:
        schema.annotations[chaise_tags.display] = {}
    if 'name_style' not in schema.annotations[chaise_tags.display]:
        schema.annotations[chaise_tags.display].update({'name_style': {'underline_space': True}})

    # Set up foreign key to ermrest_client on RCB, RMB and Owner. If ermrest_client is configured, the
    # full name of the user will be used for the FK value.
    for col, display in [('RCB', 'Created By'), ('RMB', 'Modified By')]:
        fk_name = '{}_{}_fkey'.format(table_name, col)
        try:
            # Delete old fkey if there is one laying around....
            f = table.foreign_keys[(schema_name, fk_name)]
            f.delete(catalog, table)
        except KeyError:
            pass
        fk = em.ForeignKey.define([col],
                                  'public', 'ERMrest_Client', ['ID'],
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
    parser.add_argument('--catalog_name', default=None, help="Name of catalog (Default:hostname)")
    parser.add_argument("--catalog", action='store_true', help='Configure a catalog')
    parser.add_argument('--table', default=None, metavar='SCHEMA_NAME:TABLE_NAME',
                        help='Name of table to be configured')
    parser.add_argument('--asset_table', default=None, metavar='KEY_COLUMN',
                        help='Create an asset table linked to table on key_column')
    parser.add_argument('--config', default=None, help='python script to set up configuration variables)')

    args = parser.parse_args()

    credentials = get_credential(args.server)
    catalog = ErmrestCatalog('https', args.server, args.catalog_id, credentials=credentials)

    if args.catalog:
        configure_baseline_catalog(catalog, catalog_name=args.catalog_name)
    if args.table:
        [schema_name, table_name] = args.table.split(':')
        table = catalog.getCatalogModel().schemas[schema_name].tables[table_name]
        configure_table_defaults(catalog, table)
    if args.asset_table:
        if not args.table:
            print('Creating asset table requires specification of a table')
            exit(1)
        create_asset_table(catalog, table, args.asset_table)


if __name__ == "__main__":
    main()
