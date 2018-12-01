import argparse
from requests.exceptions import HTTPError
from deriva.core import ErmrestCatalog, get_credential


def parse_args(server, catalog_id, is_table=False, is_catalog=False):
    parser = argparse.ArgumentParser(description='Update catalog configuration')
    parser.add_argument('--server', default=server, help='Catalog server name')
    parser.add_argument('--catalog', default=catalog_id, help='ID of desired catalog')
    parser.add_argument('--replace', action='store_true',
                        help='Replace existing values with new ones.  Otherwise, attempt to merge in values provided.')

    if is_table:
        modes = ['table', 'annotations', 'acls', 'comments', 'keys', 'fkeys', 'columns']
    elif is_catalog:
        modes = ['annotations', 'acls']
    else:
        modes = ['schema', 'annotations', 'acls', 'comments']

    parser.add_argument('mode', choices=modes,
                        help='Model element to be updated.')

    args = parser.parse_args()
    return args.mode, args.replace, args.server, args.catalog


class CatalogUpdater:
    def __init__(self, mode, replace, server, catalog_id):
        self.mode = mode
        self.replace = replace
        self.server = server
        self.catalog_id = catalog_id

        credential = get_credential(self.server)
        self._catalog = ErmrestCatalog('https', self.server, self.catalog_id, credentials=credential)
        self.model = self._catalog.getCatalogModel()

    def update_annotations(self, o, annotations):
        if self.replace:
            o.annotations.clear()
        o.annotations.update(annotations)

    def update_acls(self, o, acls):
        if self.replace:
            o.acls.clear()
        o.acls.update(acls)

    def update_acl_bindings(self, o, acl_bindings):
        if self.replace:
            o.acls_binding.clear()
        o.acl_bindings.update(acl_bindings)

    def update_catalog(self, annotations, acls):
        if self.mode == 'annotations':
            self.update_annotations(self.model, annotations)
        elif self.mode == 'acls':
            self.update_acls(self.model, acls)
        self.model.apply(self._catalog)

    def update_schema(self, schema_name, schema_def, annotations, acls, comment):

        credential = get_credential(self.server)
        catalog = ErmrestCatalog('https', self.server, self.catalog_id, credentials=credential)
        model_root = catalog.getCatalogModel()

        if self.mode == 'schema':
            if self.replace:
                schema = model_root.schemas[schema_name]
                print('Deleting schema ', schema.name)
                ok = input('Type YES to confirm:')
                if ok == 'YES':
                    schema.delete(catalog)
                model_root = catalog.getCatalogModel()
            schema = model_root.create_schema(catalog, schema_def)
        else:
            schema = model_root.schemas[schema_name]
            if self.mode == 'annotations':
                self.update_annotations(schema, annotations)
            elif self.mode == 'acls':
                self.update_acls(schema, acls)
            elif self.mode == 'comment':
                schema.comment = comment
        model_root.apply(catalog)
        return schema

    def update_table(self, schema_name, table_def):

        schema = self.model.schemas[schema_name]
        table_name = table_def['table_name']
        column_defs = table_def['column_definitions']
        table_acls = table_def['acls']
        table_acl_bindings = table_def['acl_bindings']
        table_annotations = table_def['annotations']
        table_comment = table_def['table_comment']
        key_defs = table_def['keys']
        fkey_defs = table_def['foreign_keys']

        print('Updating {}:{}'.format(schema_name, table_name))

        skip_fkeys = False

        if self.mode == 'table':
            if self.replace:
                table = schema.tables[table_name]
                print('Deleting table ', table.name)
                ok = input('Type YES to confirm:')
                if ok == 'YES':
                    table.delete(self._catalog, schema)
                schema = self.model.schemas[schema_name]
            if skip_fkeys:
                table_def.fkey_defs = []
            print('Creating table...')
            table = schema.create_table(self._catalog, table_def)
            return table

        table = schema.tables[table_name]
        if self.mode == 'columns':
            if self.replace:
                print('deleting columns')
                for k in table.column_definitions:
                    k.delete(self._catalog, table)
            # Go through the column definitions and add a new column if it doesn't already exist.
            for i in column_defs:
                try:
                    print('Creating column {}'.format(i['name']))
                    table.create_column(self._catalog, i)
                except HTTPError as e:
                    print("Skipping: column key {} {}: \n{}".format(i['names'], i, e.args))
        if self.mode == 'fkeys':
            if self.replace:
                print('deleting foreign_keys')
                for k in table.foreign_keys:
                    k.delete(self._catalog, table)
            for i in fkey_defs:
                try:
                    table.create_fkey(self._catalog, i)
                    print('Created foreign key {} {}'.format(i['names'], i))
                except HTTPError as e:
                    print("Skipping: foreign key {} {}: \n{}".format(i['names'], i, e.args))
        if self.mode == 'keys':
            if self.replace:
                print('deleting keys')
                for k in table.keys:
                    k.delete(self._catalog, table)
            for i in key_defs:
                try:
                    table.create_key(self._catalog, i)
                    print('Created key {}'.format(i['names']))
                except HTTPError as err:
                    if 'already exists' in err.response.text:
                        print("Skipping: key {} already exists".format(i['names']))
                    else:
                        print(err.response.text)
        if self.mode == 'annotations':
            self.update_annotations(table, table_annotations)

            column_annotations = {i.name:i.annotations for i in column_defs}
            for c in table.column_definitions:
                if self.replace:
                    c.annotations.clear()
                if c.name in [column_annotations]:
                    self.update_annotations(c, column_annotations[c.name])

        if self.mode == 'comment':
            table.comment = table_comment
            column_comment = {i.name:i.annotations for i in column_defs}
            for c in table.column_definitions:
                if c.name in column_comment:
                    c.comment = column_comment[c.name]

        if self.mode == 'acls':
            self.update_acls(table, table_acls)
            self.update_acl_bindings(table, table_acl_bindings)

            column_acls = {i.name:i.acls for i in column_defs}
            column_acl_bindings = {i.name:i.acl_bindings for i in column_defs}
            for c in table.column_definitions:
                if c.name in column_acls:
                    self.update_acls(c, column_acls[c.name])
                if c.name in column_acl_bindings:
                    self.update_acl_bindings(c, column_acl_bindings[c.name])

        table.apply(self._catalog)
