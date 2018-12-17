from __future__ import print_function

import os
from graphviz import Digraph


class DerivaCatalogToGraph:
    def __init__(self, catalog):
        self.graph = Digraph(
            engine='neato',
            format='pdf',
            edge_attr=None,
            strict=True)
        self.graph.attr('graph', rankdir='LR')
        self.graph.attr('graph', overlap='false', splines='true')

        self.model = catalog.getCatalogModel()

    def is_pure_binary(self, table, follow_naming_convention=True):
        """
        Check to see if the table has the propoerties of a pure binary association.
          1. It only has two foreign keys,
          2. There is a uniqueness constraint on the two keys.
          3. NULL values are not allowed in the foreign keys.
        :param table:
        :return:
        """

        # table has only two foreign_key constraints.
        # Each constraint is over only one column.
        if not (len(table.foreign_keys) == 2 and
                len(table.foreign_keys[0].foreign_key_columns) == 1 and
                len(table.foreign_keys[1].foreign_key_columns) == 1):
            return False

        fk0 = table.foreign_keys[0].foreign_key_columns[0]['column_name']
        fk1 = table.foreign_keys[1].foreign_key_columns[0]['column_name']

        # There is a uniqeness constraint on the pair of fkey columns.
        f = filter(lambda x: len(x.unique_columns) == 2 and fk0 in x.unique_columns and fk1 in x.unique_columns,
                   table.keys)

        if len(list(f)) != 1:
            return False

        # Null is not allowed on the column.
        if table.column_definitions[fk0].nullok or table.column_definitions[fk1].nullok:
            return False

        if follow_naming_convention and not (fk0 in table.name and fk1 in table.name):
            return False

        return True

    def linked_tables(self, table):
        fk0 = (table.foreign_keys[0].referenced_columns[0]['schema_name'],
               table.foreign_keys[0].referenced_columns[0]['table_name'])
        fk1 = (table.foreign_keys[1].referenced_columns[0]['schema_name'],
               table.foreign_keys[1].referenced_columns[0]['table_name'])
        return [fk0, fk1]

    def is_term_table(self, table):
        try:
            result = table.column_definitions['id'] and \
                     table.column_definitions['uri'] and \
                     table.column_definitions['name']
        except KeyError:
            result = False
        return result

    def catalog_to_graph(self, skip_terms=False, skip_assocation_tables=False):
        for schema in self.model.schemas:
            if schema == '_acl_admin':
                continue
            if schema == 'public':
                continue
            self.schema_to_graph(schema, skip_terms=skip_terms, skip_assocation_tables=skip_assocation_tables)

    def schema_to_graph(self, schema_name, skip_terms=False, skip_assocation_tables=False):
        schema = self.model.schemas[schema_name]
        with self.graph.subgraph(name='non_cluster_' + schema_name, node_attr={'shape': 'box'}) as schema_graph:
            for table_name in schema.tables:
                node_name = '{}_{}'.format(schema_name, table_name)

                if self.is_term_table(schema.tables[table_name]):
                    if not skip_terms:
                        schema_graph.node(node_name, label='{}:{}'.format(schema_name, table_name), shape='ellipse')
                else:
                    # Skip over providing node if this is a association table and option is set.
                    if not (self.is_pure_binary(schema.tables[table_name]) and skip_assocation_tables):
                        schema_graph.node(node_name, label='{}:{}'.format(schema_name, table_name), shape='box')
                    else:
                        print('Skipping node', node_name)
            for table_name in schema.tables:
                self.foreign_key_defs_to_graph(schema.tables[table_name],
                                               skip_terms=skip_terms, skip_association_tables=skip_assocation_tables)
        return

    def foreign_key_defs_to_graph(self, table, skip_terms=False, skip_association_tables=False):
        if self.is_pure_binary(table) and skip_association_tables:
            [t1, t2] = self.linked_tables(table)
            t1_name = '{}_{}'.format(*t1)
            t2_name = '{}_{}'.format(*t2)
            self.graph.edge(t1_name, t2_name, dir='both', color='gray')
        else:
            for fkey in table.foreign_keys:
                target_schema = fkey.referenced_columns[0]['schema_name']
                target_table = fkey.referenced_columns[0]['table_name']
                table_name = '{}_{}'.format(target_schema, target_table)
                if (table_name == 'public_ermrest_client') or ('_acl_admin' in table_name):
                    continue
                if self.is_term_table(self.model.schemas[target_schema].tables[target_table]) and skip_terms:
                    continue
                self.graph.edge('{}_{}'.format(table.sname, table.name), table_name)
        return

    def save(self, filename=None, format='pdf', view=False):
        (dir, file) = os.path.split(os.path.abspath(filename))
        if 'gv' in format:
            self.graph.save(filename=file, directory=dir)
        else:
            print('dumping graph in file', file, format)
            self.graph.render(filename=file, directory=dir, view=view, cleanup=True, format=format)

    def view(self):
        self.graph.view()
