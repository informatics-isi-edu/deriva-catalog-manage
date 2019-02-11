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
from deriva.utils.catalog.manage.configure_catalog import DerivaTableConfigure
from deriva.utils.catalog.version import __version__ as VERSION

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

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


class DerivaTable(DerivaTableConfigure):
    def __init__(self, catalog, schema_name, table_name, model=None):
        super(DerivaTable, self).__init__(catalog, schema_name, table_name, model=model)

    def link_tables(self, column_name, target_schema, target_table, target_column='RID'):
        self.table.create_fkey(self.catalog,
                               em.ForeignKey.define([column_name],
                                                    target_schema, target_table, [target_column],
                                                    constraint_names=[(self.schema_name,
                                                                       '{}_{}_fkey'.format(self.table_name,
                                                                                           target_table))],
                                                    )
                               )

    def link_vocabulary(self, column_name, term_schema, term_table):
        self.link_tables(column_name, term_schema, term_table, target_column='ID')

    def associate_tables(self, target_schema, target_table, table_column='RID', target_column='RID'):
        """
        Create a pure binary association table that connects rows in table one to rows in table 2.  Assume that RIDs are
        used for linking
        :param target_schema:
        :param target_table:
        :param table_column
        :param target_column
        :return:
        """

        association_table_name = '{}_{}'.format(self.table_name, target_table)

        column_defs = [
            em.Column.define('{}'.format(self.table_name), em.builtin_types['text'], nullok=False),
            em.Column.define('{}'.format(target_table), em.builtin_types['text'], nullok=False)
        ]

        key_defs = [
            em.Key.define([self.table_name, target_table],
                          constraint_names=[
                              (self.schema_name,
                               '{}_{}_{}_key'.format(association_table_name, self.table_name, target_table))],
                          )
        ]

        fkey_defs = [
            em.ForeignKey.define([self.table_name],
                                 self.schema_name, self.table_name, [table_column],
                                 constraint_names=[
                                     (self.schema_name, '{}_{}_fkey'.format(association_table_name, self.table_name))],
                                 ),
            em.ForeignKey.define([target_table],
                                 target_schema, target_table, [target_column],
                                 constraint_names=[
                                     (self.schema_name, '{}_{}_fkey'.format(association_table_name, target_table))])
        ]
        table_def = em.Table.define(association_table_name, column_defs=column_defs,
                                    key_defs=key_defs, fkey_defs=fkey_defs,
                                    comment='Association table for {}'.format(association_table_name))
        association_table = self.model.schemas[self.schema_name].create_table(self.catalog, table_def)
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
            table = DerivaTable(catalog, schema_name, table_name)
            if args.asset_table:
                table.create_asset_table(args.asset_table)
            if args.visible_columns:
                table.create_default_visible_columns(really=args.replace)

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
