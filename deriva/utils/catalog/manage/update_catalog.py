import argparse
import logging
from requests.exceptions import HTTPError

from deriva.core import get_credential, AttrDict, ErmrestCatalog

logger = logging.getLogger(__name__)


def parse_args(server, catalog_id, is_table=False, is_catalog=False):
    parser = argparse.ArgumentParser(description='Update catalog configuration')
    parser.add_argument('--host', default=server, help='Catalog host name')
    parser.add_argument('--catalog', default=catalog_id, help='ID of desired catalog')
    parser.add_argument('--replace', action='store_true',
                        help='Replace existing values with new ones.  Otherwise, attempt to merge in values provided.')

    if is_table:
        modes = ['table', 'annotations', 'acls', 'comment', 'keys', 'fkeys', 'columns']
    elif is_catalog:
        modes = ['annotations', 'acls']
        parser.add_argument('--recurse', action='store_true',
                            help='Update all schema and tables in the catalog.')
    else:
        modes = ['schema', 'annotations', 'acls', 'comment']
        parser.add_argument('--recurse', action='store_true',
                            help='Update all tables in the schema.')

    parser.add_argument('mode', choices=modes,
                        help='Model element to be updated.')

    args = parser.parse_args()
    return args.mode, args.replace, args.host, args.catalog


class CatalogUpdaterException(Exception):
    def __init__(self, msg='Catalog Update Exception'):
        self.msg = msg


class CatalogUpdater:
    def __init__(self, catalog):
        self._catalog = catalog
        self._model = self._catalog.getCatalogModel()

    @staticmethod
    def update_annotations(o, annotations, merge=False):
        logger.debug('%s %s %s', o, annotations, merge)
        if not merge:
            o.annotations.clear()
        o.annotations.update(annotations)

    @staticmethod
    def update_acls(o, acls, merge=False):
        if not merge:
            o.acls.clear()
        o.acls.update(acls)

    @staticmethod
    def update_acl_bindings(o, acl_bindings, merge=False):
        if not merge:
            o.acl_bindings.clear()
        o.acl_bindings.update(acl_bindings)

    def update_catalog(self, mode, annotations, acls, replace=False, merge=False):
        if mode not in ['annotations', 'acls']:
            raise CatalogUpdaterException(msg="Unknown mode {}".format(mode))

        if mode == 'annotations':
            self.update_annotations(self._model, annotations, merge=merge)
        elif mode == 'acls':
            self.update_acls(self._model, acls, merge=merge)
        self._model.apply()

    def update_schema(self, mode, schema_def, replace=False, merge=False, really=False):
        schema_name = schema_def['schema_name']
        annotations = schema_def['annotations']
        acls = schema_def['acls']
        comment = schema_def.get('comment', None)

        if mode not in ['schema', 'annotations', 'comment', 'acls']:
            raise CatalogUpdaterException(msg="Unknown mode {}".format(mode))

        if mode == 'schema':
            if replace:
                schema = self._model.schema_exists(schema_name)
                logger.info('Deleting schema %s', schema.name)
                ok = 'YES' if really else input('Type YES to confirm:')
                if ok == 'YES':
                    schema.drop()
            schema = self._model.create_schema(schema_def)
        else:
            schema = self._model.schemas[schema_name]
            if mode == 'annotations':
                self.update_annotations(schema, annotations, merge=merge)
            elif mode == 'acls':
                self.update_acls(schema, acls, merge=merge)
            elif mode == 'comment':
                schema.comment = comment
        self._model.apply()
        return schema

    def update_table(self, mode, schema_name, table_def, replace=False, merge=False, really=False):
        schema = self._model.schemas[schema_name]

        table_name = table_def['table_name']
        column_defs = table_def['column_definitions']
        table_acls = table_def['acls']
        table_acl_bindings = table_def['acl_bindings']
        table_annotations = table_def['annotations']
        # TODO: changed this to get attribute to work around some test failures
        column_annotations = table_def.get('column_annotations')
        table_comment = table_def.get('comment', None)
        column_comment = table_def.get('column_comment', None)
        key_defs = table_def['keys']
        fkey_defs = table_def['foreign_keys']

        logger.info('Updating {}:{}'.format(schema_name, table_name))
        if mode not in ['table', 'columns', 'fkeys', 'keys', 'annotations', 'comment', 'acls']:
            raise CatalogUpdaterException(msg="Unknown mode {}".format(mode))

        skip_fkeys = False

        if mode == 'table':
            if replace:
                table = schema.tables[table_name]
                logger.info('Deleting table %s', table.name)
                ok = 'YES' if really else input('Type YES to confirm:')
                if ok == 'YES':
                    table.drop()
                schema = self._model.schemas[schema_name]
            if skip_fkeys:
                table_def.fkey_defs = []
            logger.info('Creating table...%s', table_name)
            table = schema.create_table(table_def)
            return table

        table = self._model.schemas[schema_name].tables[table_name]

        if mode == 'columns':
            if replace:
                table = schema.tables[table_name]
                logger.info('Deleting columns ', table.name)
                ok = 'YES' if really else input('Type YES to confirm:')
                if ok == 'YES':
                    for k in [c for c in table.column_definitions]:
                        if k.name in ['RID', 'RMB', 'RCB', 'RCT', 'RMT']:
                            continue
                        k.drop()
            # Go through the column definitions and add a new column if it doesn't already exist.
            for i in column_defs:
                try:
                    logger.info('Creating column {}'.format(i['name']))
                    table.create_column(i)
                except HTTPError as e:
                    if 'already exists' in e.args:
                        print("Skipping existing column {}".format(i['names']))
                    else:
                        print("Skipping: column key {} {}: \n{}".format(i['names'], i, e.args))
        if mode == 'fkeys':
            if replace:
                logger.info('deleting foreign_keys')
                for k in [fk for fk in table.foreign_keys]:
                    print('dropping', k.name)
                    k.drop()
            for i in fkey_defs:
                try:
                    table.create_fkey(i)
                    print('Created foreign key {} {}'.format(i['names'], i))
                except HTTPError as e:
                    if 'already exists' in e.args:
                        print("Skipping existing foreign key {}".format(i['names']))
                    else:
                        print("Skipping: foreign key {} {}: \n{}".format(i['names'], i, e.args))

        if mode == 'keys':
            if replace:
                logger.info('Deleting keys')
                for k in table.keys:
                    k.drop()
            for i in key_defs:
                try:
                    table.create_key(i)
                    print('Created key {}'.format(i['names']))
                except HTTPError as err:
                    if 'already exists' in err.response.text:
                        print("Skipping: key {} already exists".format(i['names']))
                    else:
                        print(err.response.text)
        if mode == 'annotations':
            self.update_annotations(table, table_annotations, merge=merge)

            for c in table.column_definitions:
                if c.name in column_annotations:

                    self.update_annotations(c, column_annotations[c.name], merge=merge)

        if mode == 'comment':
            table.comment = table_comment

            for c in table.column_definitions:
                if c.name in column_comment:
                    c.comment = column_comment[c.name]

        if mode == 'acls':
            self.update_acls(table, table_acls)
            self.update_acl_bindings(table, table_acl_bindings, merge=merge)

            column_acls = {i['name']: i['acls'] for i in column_defs if 'acls' in i}
            column_acl_bindings = {i['name']: i['acl_bindings'] for i in column_defs if 'acl_bindings in i'}
            for c in table.column_definitions:
                if c.name in column_acls:
                    self.update_acls(c, column_acls[c.name], merge=merge)
                if c.name in column_acl_bindings:
                    self.update_acl_bindings(c, column_acl_bindings[c.name], merge=merge)
        self._model.apply()
