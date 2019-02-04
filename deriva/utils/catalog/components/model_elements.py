import argparse
import sys
import logging
import traceback
import requests
from requests.exceptions import HTTPError, ConnectionError

import deriva.core.ermrest_model as em
from deriva.core.base_cli import BaseCLI
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.core import ErmrestCatalog, get_credential, urlquote, format_exception
from deriva.core.utils import eprint
from deriva.utils.catalog.manage.configure_catalog import create_asset_table, configure_table_defaults

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

from requests import exceptions

VERSION = '0.1'

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


def link_tables(catalog, t1, t2, model=None, schema=None):
    """
    Create a pure binary association table that connects rows in table one to rows in table 2.  Assume that RIDs are
    used for linking
    :param catalog:
    :param model:
    :param t1: table spec
    :param t2:
    :return:
    """

    if model is None:
        model = catalog.getCatalogModel()
    (left_schema, left_table) = t1
    (right_schema, right_table) = t2

    if schema is None:
        schema = left_schema

    association_table_name = '{}_{}'.format(left_table, right_table)

    columun_defs = [
        em.Column.define('{}_RID'.format(left_table), em.builtin_types['text'], nullok=False),
        em.Column.define('{}_RID'.format(right_table), em.builtin_types['text'], nullok=False)
    ]

    key_defs = [
        em.Key.define(['%s_RID' % left_table, '%s_RID' % right_table],
                      constraint_names=[
                          (schema, '{}_{}_{}_key'.format(association_table_name, left_table, right_table))],
                      )
    ]

    fkey_defs = [
        em.ForeignKey.define(['{}_RID'.format(left_table)],
                             left_schema, left_table, ['RID'],
                             constraint_names=[(schema, '{}_{}_fkey'.format(association_table_name, left_table))],
                             ),
        em.ForeignKey.define(['{}_RID'.format(right_table)],
                             right_schema, right_table, ['RID'],
                             constraint_names=[(schema, '{}_{}_fkey'.format(association_table_name, right_table))])
    ]
    table_def = em.Table.define(association_table_name, column_defs=columun_defs,
                                key_defs=key_defs, fkey_defs=fkey_defs,
                                comment='Association table for {}'.format(association_table_name))
    print(table_def)
    association_table = model.schemas[schema].create_table(catalog, table_def)
    return association_table


def rename_column(catalog, table, from_column, to_column, delete=False):
    table_name = table.name
    schema_name = table.sname

    # Check to make sure that this column is not part of a FK.
    column_ref = {'schema_name': schema_name, 'table_name': table_name, 'column_name': from_column}
    for fk in table.referenced_by:
        if column_ref in fk.referenced_columns:
            raise DerivaConfigError(msg='Cannot rename column that is used in foreign key')

    # Create a new column_spec from the existing spec.
    to_def = table.column_definitions[from_column].prejson()
    to_def['name'] = to_column
    table.create_column(catalog, to_def)

    # Copy over the old values
    table_path = catalog.getPathBuilder().schemas[schema_name].tables[table_name]
    table_path.update(table_path.entities(table_path.RID, **{to_column: table_path.column_definitions[from_column]}))

    # Update annotations where the old spec was being used
    for k, v in table.annotations[chaise_tags.visible_columns]:
        table.annotations[chaise_tags.visible_columns][k] = [to_column if i == from_column else i for i in v]

    # Delete old column
    if delete:
        table.column_definitions[from_column].delete(catalog, table)

    return


def create_default_visible_columns(catalog, table_spec, model=None, really=False):
    if not model:
        model=catalog.getCatalogModel()

    (schema_name,table_name) = table_spec
    table = model.schemas[schema_name].tables[table_name]

    if chaise_tags.visible_columns not in table.annotations:
        table.annotations[chaise_tags.visible_columns] = {'*' : default_visible_column_list(table)}
    elif '*' not in table.annotations[chaise_tags.visible_columns] or really:
        table.annotations[chaise_tags.visible_columns].update({'*': default_visible_column_list(table)})
    else:
        raise DerivaConfigError(msg='Existing visible column annotation in {}'.format(table_name))

    table.apply(catalog)
    return


def default_visible_column_list(table):
    """
    Create a general visible columns annotation spec that would be consistant with what chaise does by default.
    This spec can then be added to a table and editied for user preference.
    :param table:
    :return:
    """
    fkeys = {i.foreign_key_columns[0]['column_name']: [i.names[0], i.referenced_columns[0]['column_name']]
             for i in table.foreign_keys}
    return [
        {'source':
             [{'outbound': fkeys[i.name][0]}, fkeys[i.name][1]] if i.name in fkeys else i.name
         }
        for i in table.column_definitions
    ]


class DerivaModelElementsCLI(BaseCLI):

    def __init__(self, description, epilog):
        """Initializes the CLI.
        """
        super(DerivaModelElementsCLI, self).__init__(description, epilog, VERSION)

        # initialized after argument parsing
        self.args = None
        self.host = None

        # parent arg parser
        parser = self.parser
        parser.add_argument('server', help='Catalog server name')
        parser.add_argument('table', default=None, metavar='SCHEMA_NAME:TABLE_NAME',
                            help='Name of table to be configured')
        parser.add_argument('--catalog', default=1, help="ID number of desired catalog (Default:1)")
        parser.add_argument('--asset-table', default=None, metavar='KEY_COLUMN',
                            help='Create an asset table linked to table on key_column')
        parser.add_argument('--visible-columns', action='store_true',
                        help='Create a default visible columns annotation')
        parser.add_argument('--replace', action='store_true', help='Overwrite existing value')

    @staticmethod
    def _get_credential(host_name, token=None):
        if token:
            return {"cookie": "webauthn={t}".format(t=token)}
        else:
            return get_credential(host_name)

    def main(self):
        """Main routine of the CLI.
        """
        args = self.parse_cli()

        try:
            catalog = ErmrestCatalog('https', args.host, args.catalog, credentials=self._get_credential(args.host))
            [schema_name, table_name] = args.table.split(':')
            table = catalog.getCatalogModel().schemas[schema_name].tables[table_name]
            if args.asset_table:
                create_asset_table(catalog, table, args.asset_table)
            if args.visible_columns:
                create_default_visible_columns(catalog, (schema_name, table_name), really=args.replace)

        except HTTPError as e:
            if e.response.status_code == requests.codes.unauthorized:
                msg = 'Authentication required'
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
        return 0


def main():
    DESC = "DERIVA Model Elements Command-Line Interface"
    INFO = "For more information see: https://github.com/informatics-isi-edu/deriva-catalog-manage"
    return DerivaModelElementsCLI(DESC, INFO).main()


if __name__ == '__main__':
    sys.exit(main())
