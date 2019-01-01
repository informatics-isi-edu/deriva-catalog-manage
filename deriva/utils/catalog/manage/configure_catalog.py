from attrdict import AttrDict

import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags

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


def configure_ermrest_client(catalog):
    """
    Set up ermrest_client table so that it has readable names and uses the full name of the user as the row name.
    :param catalog: Ermrest catalog
    :return:
    """
    ermrest_client = catalog.getCatalogModel().schemas['public'].tables['ermrest_client']

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
        chaise_tags.table_display: {'row_name': {'row_markdown_pattern': '{{{full_name}}}'}}
    })

    for k, v in column_annotations.items():
        ermrest_client.column_definitions[k].annotations.update(v)

    ermrest_client.apply(catalog)
    return


def configure_baseline_catalog(catalog, set_policy=True):
    """
    Set catalog into standard configuration which includes:
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

    configure_ermrest_client(catalog)
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
    :param catalog:
    :param table:
    :return:
    """

    table_name = table.name
    schema_name = table.sname

    if self_serve_policy:
        configure_selfserve_policy(catalog, table)

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
    :param hatrac_url:
    :return:
    """
    asset_mappings = [
        {
            'default_columns': ['RID', 'RCB', 'RMB', 'RCT', 'RMT'],
            'ext_pattern': '^.*[.](?P<file_ext>json|csv)$',
            'asset_type': 'table',
            'file_pattern': '^((?!/assets/).)*/records/(?P<schema>.+?)/(?P<table>.+?)[.]'
        },
        {
            'checksum_types': ['md5'],
            'record_query_template':
                '/entity/{target_table}/Local_Id={local_id}/MD5={md5}/URL={URI_urlencoded}',
            'hatrac_templates': {'hatrac_uri': '/hatrac/commons/data/{asset_type}/{file_name}'},
            'create_record_before_upload': 'False',
            'ext_pattern': '.*$',
            'file_pattern': '.*',
            'target_table': [schema_name, table_name],
            'hatrac_options': {'versioned_uris': 'True'},
            'metadata_query_templates': [
                '/attribute/D:={%s}:{%s}/rid:=D:RID, local_id:=D:{%s}' % (schema_name, table_name, key_column),
            ],
            'dir_pattern': '^.*/(?P<local_id>[0-9A-Z-]+)/)',
            'column_map': {
                'url': '{URI}',
                'byte_count': '{file_size}',
                table_name: '{table_rid}',
                'filename': '{file_name}',
                'md5': '{md5}'
                #             }
            }
        }
    ]
    return asset_mappings


def create_asset_table(catalog, table):
    """
    Create a basic asset table and configure upload script to load the table along with a table of associated
    metadata.
    :param catalog:
    :param table: Table to contain the asset metadata.  Asset will have a foreign key to this table.
    :return:
    """
    table_name = table.name
    schema_name = table.sname
    model = catalog.getCatalogModel()
    asset_table_name = '{}_Assets'.format(table_name)

    column_annotations = {
        'URL': {
            chaise_tags.asset: {
                'filename_column': 'Filename',
                'byte_count_column': 'Length',
                'url_pattern':
                    '/hatrac/commons/data/{{{%s}}}/{{#encode}}{{{Filename}}}{{/encode}}' % table_name,
                'md5': 'MD5'
            },
            chaise_tags.column_display: {'*': {'markdown_pattern': '[**{{Filename}}**]({{{URL}}})'}}
        },
        'Filename': {
            chaise_tags.column_display: {'*': {'markdown_pattern': '[**{{filename}}**]({{{URL}}})'}}
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

    fkey_defs = [
        em.ForeignKey.define(['{}_RID'.format(table_name)],
                             schema_name, table_name, ['RID'],
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
    if chaise_tags.builk_uploads not in model.annotations:
        model.annotations.update({
            'asset_mappings': [],
            'version_update_url': 'https://github.com/informatics-isi-edu/deriva-qt/releases',
            'version_compatibility': [['>=0.4.3', '<1.0.0']]
        } )
    model.annotations[chaise_tags.bulk_upload]['asset_mappings'].append(asset_mappings)
    return asset_table
