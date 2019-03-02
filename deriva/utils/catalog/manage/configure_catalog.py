import argparse
import sys
import warnings
import logging
from requests import exceptions
import traceback
import requests
from requests.exceptions import HTTPError, ConnectionError

import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.core import ErmrestCatalog, get_credential, format_exception
from deriva.core.utils import eprint
from deriva.core.base_cli import BaseCLI
from deriva.utils.catalog.version import __version__ as VERSION

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

IS_PY2 = (sys.version_info[0] == 2)
IS_PY3 = (sys.version_info[0] == 3)

if IS_PY3:
    from urllib.parse import urlparse
else:
    from urlparse import urlparse

chaise_tags.catalog_config = 'tag:isrd.isi.edu,2019:catalog-config'

logger = logging.getLogger(__name__)

class DerivaConfigError(Exception):
    def __init__(self, msg):
        self.msg = msg


class DerivaCatalogConfigure:
    def __init__(self, catalog):
        self.catalog = catalog
        self.model = catalog.getCatalogModel()

    def apply(self):
        self.model.apply(self.catalog)
        return self

    def configure_ermrest_client(self, groups):
        """
        Set up ermrest_client table so that it has readable names and uses the display name of the user as the row name.
        :param groups:
        :return:
        """

        ermrest_client = self.model.schemas['public'].tables['ERMrest_Client']

        # Make ermrest_client table visible.  If the GUID or member name is considered sensitivie, then this needs to be
        # changed.
        ermrest_client.acls['select'] = ['*']

        # Set table and row name.
        ermrest_client.annotations.update({
            chaise_tags.display: {'name': 'Users'},
            chaise_tags.visible_columns: {'compact': ['ID', 'Full_Name', 'Display_Name', 'Email']},
            chaise_tags.table_display: {'row_name': {'row_markdown_pattern': '{{{Full_Name}}}'}}
        })

        column_annotations = {
            'RCT': {chaise_tags.display: {'name': 'Creation Time'}},
            'RMT': {chaise_tags.display: {'name': 'Modified Time'}},
            'RCB': {chaise_tags.display: {'name': 'Created By'}},
            'RMB': {chaise_tags.display: {'name': 'Modified By'}}
        }
        for k, v in column_annotations.items():
            ermrest_client.column_definitions[k].annotations.update(v)
        return

    def _configure_www_schema(self):
        """
        Set up a new schema and tables to hold web-page like content.  The tables include a page table, and a asset
        table that can have images that are referred to by the web page.  Pages are written using markdown.
        :return:
        """
        logging.info('Configuring WWW schema')
        # Create a WWW schema if one doesn't already exist.
        try:
            www_schema_def = em.Schema.define('WWW', comment='Schema for tables that will be displayed as web content')
            www_schema = self.model.create_schema(self.catalog, www_schema_def)
        except ValueError as e:
            if 'already exists' not in e.args[0]:
                raise
            else:
                www_schema = self.model.schemas['WWW']

        # Create the page table
        page_table_def = em.Table.define(
            'Page',
            column_defs=[
                em.Column.define('Title', em.builtin_types['text'], nullok=False, comment='Unique title for the page'),
                em.Column.define('Content', em.builtin_types['markdown'], comment='Content of the page in markdown')
            ],
            key_defs=[em.Key.define(['Title'], [['WWW', 'Page_Title_key']])],
            annotations={
                chaise_tags.table_display: { 'detailed': {'hide_column_headers': True, 'collapse_toc_panel': True}
            },
                chaise_tags.visible_foreign_keys: {'detailed': {}},
                chaise_tags.visible_columns: {'detailed': ['Content']}}
        )
        try:
            www_schema.create_table(self.catalog, page_table_def)
        except ValueError as e:
            if 'already exists' not in e.args[0]:
                raise
        table = DerivaTableConfigure(self.catalog, 'WWW', 'Page', model=self.model)
        table.configure_table_defaults()

        # Now set up the asset table
        try:
            table = DerivaTableConfigure(self.catalog, 'WWW', 'Page', model=self.model)
            table.create_asset_table('RID')
        except ValueError as e:
            if 'already exists' not in e.args[0]:
                raise

        return self

    def set_core_groups(self, catalog_name=None, admin=None, curator=None, writer=None, reader=None,
                        replace=False):
        """
        Look in the catalog to get the group IDs for the four core groups used in the baseline configuration. There are
        three options:  1) core group name can be provided explicitly, 2) group name can be formed from a catalog
        name and a default group name, 3) group name can be formed from the host name and a default group name.
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
        if chaise_tags.catalog_config in self.model.annotations and not replace:
            groups.update({
                'admin': self.model.annotations[chaise_tags.catalog_config]['groups']['admin'],
                'curator': self.model.annotations[chaise_tags.catalog_config]['groups']['curator'],
                'writer': self.model.annotations[chaise_tags.catalog_config]['groups']['writer'],
                'reader': self.model.annotations[chaise_tags.catalog_config]['groups']['reader']
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

            pb = self.catalog.getPathBuilder()
            catalog_groups = {i['Display_Name']: i for i in pb.public.ERMrest_Group.entities()}
            groups = {}
            try:
                groups.update({
                    'admin': catalog_groups[admin]['ID'],
                    'curator': catalog_groups[curator]['ID'],
                    'writer': catalog_groups[writer]['ID'],
                    'reader': catalog_groups[reader]['ID'] if reader is not '*' else '*'
                })
            except KeyError as e:
                raise DerivaConfigError(msg='Group {} not defined'.format(e.args[0]))

        return groups

    def configure_group_table(self, groups):
        """
        Create a table in the public schema for tracking mapping of group names.
        :param groups:
        :return:
        """

        logging.info('Configuring groups')
        ermrest_group = self.model.schemas['public'].tables['ERMrest_Group']

        # Make ERMrest_Group table visible to writers, curators, and admins.
        ermrest_group.acls['select'] = [groups['writer'], groups['curator'], groups['admin']]

        # Set table and row name.
        ermrest_group.annotations.update({
            chaise_tags.display: {'name': 'Globus Group'},
            chaise_tags.visible_columns: {'*': ['Display_Name', 'Description', 'URL', 'ID']},
            chaise_tags.table_display: {'row_name': {'row_markdown_pattern': '{{{Display_Name}}}'}}
        })

        # Set compound key so that we can link up with Visible_Group table.
        try:
            ermrest_group.create_key(
                self.catalog,
                em.Key.define(['ID', 'URL', 'Display_Name', 'Description'],
                              constraint_names=[('public', 'Group_Compound_key')],
                              comment='Compound key to ensure that columns sync up into Visible_Groups on update.'

                              )
            )
            ermrest_group.create_key(
                self.catalog,
                em.Key.define(['ID'],
                              constraint_names=[('public', 'Group_ID_key')],
                              comment='Group ID is unique.'

                              )
            )
        except exceptions.HTTPError as e:
            if 'already exists' not in e.args[0]:
                raise

        # Create a catalog groups table
        column_defs = [
            em.Column.define('Display_Name', em.builtin_types['text']),
            em.Column.define('URL', em.builtin_types['text'],
                             annotations={
                                 chaise_tags.column_display: {
                                     '*': {'markdown_pattern': '[**{{Display_Name}}**]({{{URL}}})'}},
                                 chaise_tags.display: {'name': 'Group Management Page'}
                             }
                             ),
            em.Column.define('Description', em.builtin_types['text']),
            em.Column.define('ID', em.builtin_types['text'], nullok=False)
        ]

        key_defs = [
            em.Key.define(['ID'],
                          constraint_names=[('public', 'Group_ID_key')],
                          comment='Compound key to ensure that columns sync up into Catalog_Groups on update.'

                          )
        ]

        # Set up a foreign key to the group table so that the creator of a record can only select
        # groups of which they are members of for values of the Owners column.
        fkey_group_policy = {
            # FKey to group can be created only if you are a member of the group you are referencing
            'set_owner': {"types": ["insert"],
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
        catalog_group = em.Table.define(
            'Catalog_Group',
            annotations={chaise_tags.table_display: {'row_name': {'row_markdown_pattern': '{{{Display_Name}}}'}}},
            column_defs=column_defs,
            fkey_defs=fkey_defs, key_defs=key_defs,
            acls={
                # Make ERMrest_Group table visible to members of the group members, curators, and admins.
                'select': [groups['reader']],
                'insert': [groups['writer'], groups['curator']]
            },
        )

        public_schema = self.model.schemas['public']

        # Get or create Catalog_Group table....
        try:
            public_schema.create_table(self.catalog, catalog_group)
        except ValueError as e:
            if 'already exists' not in e.args[0]:
                raise
        table = DerivaTableConfigure(self.catalog, 'public', 'Catalog_Group', model=self.model)
        table.configure_table_defaults(set_policy=False)
        return

    def configure_baseline_catalog(self,
                                   catalog_name=None,
                                   admin=None, curator=None, writer=None, reader=None,
                                   set_policy=True,
                                   public=False,
                                   apply=True):
        """
        Put catalog into standard configuration which includes:
        1) Setting default display mode to be to turn underscores to spaces.
        2) Set access control assuming admin, curator, writer, and reader groups.
        3) Configure ermrest_client to have readable names.

        :param catalog_name:
        :param admin: Name of the admin group.  Defaults to catalog-admin
        :param curator: Name of the curator group. Defaults to catalog-curator
        :param writer: Name of the writer group. Defaults to catalog-writer
        :param reader: Name of the reader group. Defaults to catalog-reader
        :param set_policy: Set policy for catalog to support reader/writer/curator/admin groups.
        :param public: Set to true if anonymous read access should be allowed.
        :param apply: If true then applyt the changes from the model to the catalog
        :return:
        """

        if not catalog_name:
            # If catalog name is not provided, default to the host name of the host.
            catalog_name = urlparse(self.catalog.get_server_uri()).hostname.split('.')[0]
        groups = self.set_core_groups(catalog_name=catalog_name,
                                      admin=admin, curator=curator, writer=writer, reader=reader)

        # Record configuration of catalog so we can retrieve when we configure tables later on.
        self.model.annotations[chaise_tags.catalog_config] = {'name': catalog_name, 'groups': groups}

        # Set up default name style for all schemas.
        for s in self.model.schemas.values():
            s.annotations[chaise_tags.display] = {'name_style': {'underline_space': True}}

        # modify catalog ACL config to support basic admin/curator/writer/reader access.
        if set_policy:
            self.model.acls.update({
                "owner": [groups['admin']],
                "insert": [groups['curator'], groups['writer']],
                "update": [groups['curator']],
                "delete": [groups['curator']],
                "select": [groups['writer'], groups['reader']] if not public else ['*'],
                "enumerate": ["*"],
            })

        self.configure_ermrest_client(groups)
        self.configure_group_table(groups)
        self._configure_www_schema()

        if apply:
            self.apply()

        return


def update_group_table(catalog):
    def group_urls(group):
        guid = group.split('/')[-1]
        link = 'https://app.globus.org/groups/' + guid
        uri = 'https://auth.globus.org/' + guid
        return link, uri

    pb = catalog.getPathBuilder()
    # Attempt to add URL.  This can go away once we have URL entered by ERMrest.
    pb.public.ERMrest_Group.update(
        [{'RID': i['RID'], 'URL': group_urls(i['ID'])[0]} for i in pb.public.ERMrest_Group.entities()]
    )


class DerivaTableConfigure:
    def __init__(self, catalog, schema_name, table_name, model=None):
        self.catalog = catalog
        self.schema_name = schema_name
        self.table_name = table_name
        self.model = catalog.getCatalogModel() if model is None else model
        self.table = self.model.schemas[schema_name].tables[table_name]
        return

    def apply(self):
        self.model.apply(self.catalog)

    def get_groups(self):
        if chaise_tags.catalog_config in self.model.annotations:
            return self.model.annotations[chaise_tags.catalog_config]['groups']
        else:
            raise DerivaConfigError(msg='Attempting to configure table before catalog is configured')

    def create_asset_table(self, key_column,
                           extensions=[],
                           file_pattern='.*',
                           column_defs=[], key_defs=[], fkey_defs=[],
                           comment=None, acls={},
                           acl_bindings={},
                           annotations={},
                           set_policy=True,
                           apply=True):
        """
        Create a basic asset table and configures the bulk upload annotation to load the table along with a table of
        associated metadata. This routine assumes that the metadata table has already been defined, and there is a key
        associated metadata. This routine assumes that the metadata table has already been defined, and there is a key
        column the metadata table that can be used to associate the asset with a row in the table. The default
        configuration assumes that the assets are in a directory named with the table name for the metadata and that
        they either are in a subdirectory named by the key value, or that they are in a file whose name starts with the
        key value.

        :param key_column: The column in the metadata table to be used to correlate assets with entries. Assets will be
                           named using the key column.
        :param extensions: List file extensions to be matched. Default is to match any extension.
        :param file_pattern: Regex that identified the files to be considered for upload
        :param column_defs: a list of Column.define() results for extra or overridden column definitions
        :param key_defs: a list of Key.define() results for extra or overridden key constraint definitions
        :param fkey_defs: a list of ForeignKey.define() results for foreign key definitions
        :param comment: a comment string for the asset table
        :param acls: a dictionary of ACLs for specific access modes
        :param acl_bindings: a dictionary of dynamic ACL bindings
        :param annotations: a dictionary of annotations
        :param set_policy: If true, add ACLs for self serve policy to the asset table
        :return:
        """

        def create_asset_upload_spec():
            extension_pattern = '^.*[.](?P<file_ext>{})$'.format('|'.join(extensions if extensions else ['.*']))

            return [
                # Any metadata is in a file named /records/schema_name/tablename.[csv|json]
                {
                    'default_columns': ['RID', 'RCB', 'RMB', 'RCT', 'RMT'],
                    'ext_pattern': '^.*[.](?P<file_ext>json|csv)$',
                    'asset_type': 'table',
                    'file_pattern': '^((?!/assets/).)*/records/(?P<schema>%s?)/(?P<table>%s)[.]' %
                                    (self.schema_name, self.table_name),
                    'target_table': [self.schema_name, self.table_name],
                },
                # Assets are in format assets/schema_name/table_name/correlation_key/file.ext
                {
                    'checksum_types': ['md5'],
                    'column_map': {
                        'URL': '{URI}',
                        'Length': '{file_size}',
                        self.table_name + '_RID': '{table_rid}',
                        'Filename': '{file_name}',
                        'MD5': '{md5}',
                    },
                    'dir_pattern': '^.*/(?P<schema>%s)/(?P<table>%s)/(?P<key_column>.*)/' %
                                   (self.schema_name, self.table_name),
                    'ext_pattern': extension_pattern,
                    'file_pattern': file_pattern,
                    'hatrac_templates': {'hatrac_uri': '/hatrac/{schema}/{table}/{md5}.{file_name}'},
                    'target_table': [self.schema_name, asset_table_name],
                    # Look for rows in the metadata table with matching key column values.
                    'metadata_query_templates': [
                        '/attribute/D:={schema}:{table}/%s={key_column}/table_rid:=D:RID' % key_column],
                    # Rows in the asset table should have a FK reference to the RID for the matching metadata row
                    'record_query_template':
                        '/entity/{schema}:{table}_Asset/{table}_RID={table_rid}/MD5={md5}/URL={URI_urlencoded}',
                    'hatrac_options': {'versioned_uris': True},
                }
            ]

        asset_table_name = '{}_Asset'.format(self.table_name)

        if set_policy and chaise_tags.catalog_config not in self.model.annotations:
            raise DerivaConfigError(msg='Attempting to configure table before catalog is configured')

        if key_column not in [i.name for i in self.table.column_definitions]:
            raise DerivaConfigError(msg='Key column not found in target table')

        column_defs = [
                          em.Column.define('{}_RID'.format(self.table_name),
                                           em.builtin_types['text'],
                                           nullok=False,
                                           comment="The {} entry to which this asset is attached".format(
                                               self.table_name)),
                      ] + column_defs

        # Set up policy so that you can only add an asset to a record that you own.
        fkey_acls, fkey_acl_bindings = {}, {}
        if set_policy:
            groups = self.get_groups()

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

        # Link asset table to metadata table with additional information about assets.
        asset_fkey_defs = [
                              em.ForeignKey.define(['{}_RID'.format(self.table_name)],
                                                   self.schema_name, self.table_name, ['RID'],
                                                   acls=fkey_acls, acl_bindings=fkey_acl_bindings,
                                                   constraint_names=[
                                                       (self.schema_name,
                                                        '{}_{}_fkey'.format(asset_table_name, self.table_name))],
                                                   )
                          ] + fkey_defs
        comment = comment if comment else 'Asset table for {}'.format(self.table_name)

        if chaise_tags.table_display not in annotations:
            annotations[chaise_tags.table_display] = {'row_name': {'row_markdown_pattern': '{{{Filename}}}'}}

        table_def = em.Table.define_asset(self.schema_name, asset_table_name, fkey_defs=asset_fkey_defs,
                                          column_defs=column_defs, key_defs=key_defs, annotations=annotations,
                                          acls=acls, acl_bindings=acl_bindings,
                                          comment=comment)

        for i in table_def['column_definitions']:
            if i['name'] == 'URL':
                i[chaise_tags.column_display] = {'*': {'markdown_pattern': '[**{{URL}}**]({{{URL}}})'}}
            if i['name'] == 'Filename':
                i[chaise_tags.column_display] = {'*': {'markdown_pattern': '[**{{Filename}}**]({{{URL}}})'}}

        asset_table = self.model.schemas[self.schema_name].create_table(self.catalog, table_def)
        DerivaTableConfigure(self.catalog, self.schema_name, asset_table_name, model=self.model).configure_table_defaults()

        # The last thing we should do is update the upload spec to accomidate this new asset table.
        if chaise_tags.bulk_upload not in self.model.annotations:
            self.model.annotations.update({
                chaise_tags.bulk_upload: {
                    'asset_mappings': [],
                    'version_update_url': 'https://github.com/informatics-isi-edu/deriva-qt/releases',
                    'version_compatibility': [['>=0.4.3', '<1.0.0']]
                }
            })

        # Clean out any old upload specs if there are any and add the new specs.
        upload_annotations = self.model.annotations[chaise_tags.bulk_upload]
        upload_annotations['asset_mappings'] = \
            [i for i in upload_annotations['asset_mappings'] if
             not (
                     i.get('target_table', []) == [self.schema_name, asset_table_name]
                     or
                     (
                             i.get('target_table', []) == [self.schema_name, self.table_name]
                             and
                             i.get('asset_type', '') == 'table'
                     )
             )
             ] + create_asset_upload_spec()
        if apply:
            self.apply()

        return asset_table

    def configure_self_serve_policy(self, groups):
        """
        Set up a table so it has a self service policy.  Add an owner column if one is not present, and set the acl
        binding so that it follows the self service policy.

        :param groups: dictionary of core catalog groups
        :return:
        """

        # Configure table so that access can be assigned to a group.  This requires that we create a column and
        # establish a foreign key to an entry in the group table.  We will set the access control on the foreign key
        # so that you are only able to delagate access to a the creator of the entity belongs to.
        if 'Owner' not in [i.name for i in self.table.column_definitions]:
            col_def = em.Column.define('Owner', em.builtin_types['text'], comment='Group that can update the record.')
            self.table.create_column(self.catalog, col_def)

        # Now configure the policy on the table...
        self_service_policy = {
            # Set up a policy for the table that allows the creator of the record to update and delete the record.
            "self_service_creator": {
                "types": ["update", 'delete'],
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
        self.table.acl_bindings.update(self_service_policy)

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

        owner_fkey_name = '{}_Catalog_Group_fkey'.format(self.table_name)
        fk = em.ForeignKey.define(['Owner'],
                                  'public', 'Catalog_Group', ['ID'],

                                  acls=fkey_group_acls, acl_bindings=fkey_group_policy,
                                  constraint_names=[(self.schema_name, owner_fkey_name)],
                                  )
        try:
            # Delete old fkey if there is one laying around....
            f = self.table.foreign_keys[(self.schema_name, owner_fkey_name)]
            f.delete(self.catalog, self.table)
        except KeyError:
            pass

        # Now create the foreign key to the group table.
        self.table.create_fkey(self.catalog, fk)
        return

    def create_default_visible_columns(self, really=False, apply=True):
        if chaise_tags.visible_columns not in self.table.annotations:
            self.table.annotations[chaise_tags.visible_columns] = {'*': self.default_visible_column_list()}
        elif '*' not in self.table.annotations[chaise_tags.visible_columns] or really:
            self.table.annotations[chaise_tags.visible_columns].update({'*': self.default_visible_column_list()})
        else:
            raise DerivaConfigError(msg='Existing visible column annotation in {}'.format(self.table_name))
        if apply:
            self.apply()
        return self

    def default_visible_column_list(self):
        """
        Create a general visible columns annotation spec that would be consistant with what chaise does by default.
        This spec can then be added to a table and editied for user preference.
        :return:
        """
        fkeys = {i.foreign_key_columns[0]['column_name']: [i.names[0], i.referenced_columns[0]['column_name']]
                 for i in self.table.foreign_keys}
        columns = [i for i in self.table.column_definitions]
        column_names = [i.name for i in columns]

        # Move Owner column to be right after RMB if they both exist.
        if 'Owner' in column_names and 'RMB' in column_names:
            columns.insert(column_names.index('RMB') + 1, columns.pop(column_names.index('Owner')))
        return [
            {'source': [{'outbound': fkeys[i.name][0]}, fkeys[i.name][1]] if i.name in fkeys else i.name}
            for i in columns
        ]

    def configure_table_defaults(self, set_policy=True, public=False, apply=True):
        """
        This function adds the following basic configuration details to an existing table:
        1) Creates a self service modification policy in which creators can update update any row they create.
           Optionally, an Owner column can be provided, which allows the creater of a row to delegate row ownership to
           a specific group.
        2) Adds display annotations and foreign key declarations so that system columns RCB, RMB display in a user
           friendly way.
        :param set_policy: If true, then configure the table to have a self service policy
        :param public: Make table acessible without logging in.
        :return:
        """

        if chaise_tags.catalog_config not in self.model.annotations:
            raise DerivaConfigError(msg='Attempting to configure table before catalog is configured')

        # Hack to update description and URL until we get these passed through ermrest....
        update_group_table(self.catalog)

        schema = self.model.schemas[self.schema_name]

        if public:
            # First copy dver any inherited ACLS.
            if schema.acls:
                self.table.acls.update(schema.acls)
            elif self.model.acls:
                self.table.acls.update(self.model.acls)
            self.table.acls.pop("create", None)
            # Now add permision for anyone to read.
            self.table.acls['select'] = ['*']

        if set_policy:
            self.configure_self_serve_policy(self.get_groups())

        # Configure schema if not already done so.
        if chaise_tags.display not in schema.annotations:
            schema.annotations[chaise_tags.display] = {}
        if 'name_style' not in schema.annotations[chaise_tags.display]:
            schema.annotations[chaise_tags.display].update({'name_style': {'underline_space': True}})

        # Set up foreign key to ermrest_client on RCB, RMB and Owner. If ermrest_client is configured, the
        # full name of the user will be used for the FK value.
        for col, display in [('RCB', 'Created By'), ('RMB', 'Modified By')]:
            fk_name = '{}_{}_fkey'.format(self.table_name, col)
            try:
                # Delete old fkey if there is one laying around....
                f = self.table.foreign_keys[(self.schema_name, fk_name)]
                f.delete(self.catalog, self.table)
            except KeyError:
                pass
            fk = em.ForeignKey.define([col],
                                      'public', 'ERMrest_Client', ['ID'],
                                      constraint_names=[(self.schema_name, fk_name)],
                                      )
            self.table.create_fkey(self.catalog, fk)

            # Add a display annotation so that we have sensible name for RCB and RMB.
            self.table.column_definitions[col].annotations.update({chaise_tags.display: {'name': display}})

        self.table.column_definitions['RCT'].annotations.update({chaise_tags.display: {'name': 'Creation Time'}})
        self.table.column_definitions['RMT'].annotations.update({chaise_tags.display: {'name': 'Modified Time'}})

        self.create_default_visible_columns(apply=False)
        if apply:
            self.apply()
        return self


class DerivaConfigureCatalogCLI(BaseCLI):

    def __init__(self, description, epilog):
        super(DerivaConfigureCatalogCLI, self).__init__(description, epilog, VERSION, hostname_required=True)

        # parent arg parser
        parser = self.parser
        parser.add_argument('configure', choices=['catalog', 'table'],
                            help='Choose between configuring a catalog or a specific table')
        parser.add_argument('--catalog-name', default=None, help="Name of catalog (Default:hostname)")
        parser.add_argument('--catalog', default=1, help="ID number of desired catalog (Default:1)")
        parser.add_argument('--table', default=None, metavar='SCHEMA_NAME:TABLE_NAME',
                            help='Name of table to be configured')
        parser.add_argument('--set-policy', default='True', choices=[True, False],
                            help='Access control policy to be applied to catalog or table')
        parser.add_argument('--reader-group', dest='reader', default=None,
                            help='Group name to use for readers. For a catalog named "foo" defaults for foo-reader')
        parser.add_argument('--writer-group', dest='writer', default=None,
                            help='Group name to use for writers. For a catalog named "foo" defaults for foo-writer')
        parser.add_argument('--curator-group', dest='curator', default=None,
                            help='Group name to use for readers. For a catalog named "foo" defaults for foo-curator')
        parser.add_argument('--admin-group', dest='admin', default=None,
                            help='Group name to use for readers. For a catalog named "foo" defaults for foo-admin')
        parser.add_argument('--publish', default=False, action='store_true',
                            help='Make the catalog or table accessible for reading without logging in')

    @staticmethod
    def _get_credential(host_name, token=None):
        if token:
            return {"cookie": "webauthn={t}".format(t=token)}
        else:
            return get_credential(host_name)

    def main(self):

        args = self.parse_cli()
        credentials = self._get_credential(args.host)
        catalog = ErmrestCatalog('https', args.host, args.catalog, credentials=credentials)

        try:
            if args.configure == 'catalog':
                logging.info('Configuring catalog {}:{}'.format(args.host, args.catalog))
                cfg = DerivaCatalogConfigure(catalog)
                cfg.configure_baseline_catalog(catalog_name=args.catalog_name,
                                               reader=args.reader, writer=args.writer, curator=args.curator,
                                               admin=args.admin,
                                               set_policy=args.set_policy, public=args.publish)
                cfg.apply()
            if args.table:
                [schema_name, table_name] = args.table.split(':')
                table = DerivaTableConfigure(catalog, schema_name, table_name)
                table.configure_table_defaults(set_policy=args.set_policy, public=args.publish)
                table.apply()
        except DerivaConfigError as e:
            print(e.msg)
        except HTTPError as e:
            if e.response.status_code == requests.codes.unauthorized:
                msg = 'Authentication required for {}'.format(args.host)
            elif e.response.status_code == requests.codes.forbidden:
                msg = 'Permission denied'
            else:
                msg = e
            logging.debug(format_exception(e))
            eprint(msg)
        except RuntimeError as e:
            sys.stderr.write(str(e))
            return 1
        except:
            traceback.print_exc()
            return 1
        finally:
            sys.stderr.write("\n\n")
        return


def main():
    DESC = "DERIVA Configure Catalog Command-Line Interface"
    INFO = "For more information see: https://github.com/informatics-isi-edu/deriva-catalog-manage"
    return DerivaConfigureCatalogCLI(DESC, INFO).main()


if __name__ == '__main__':
    sys.exit(main())
