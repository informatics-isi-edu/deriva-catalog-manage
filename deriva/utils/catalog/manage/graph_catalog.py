from __future__ import print_function

import os
from urllib.parse import urlparse

from graphviz import Digraph

class DerivaCatalogToGraph:
    def __init__(self, catalog, engine='dot'):
        self.graph = Digraph(
            engine=engine,
            format='pdf',
            edge_attr=None,
            strict=True,
        )

        self.catalog = catalog
        self._model = catalog.getCatalogModel()
        self._chaise_base = "https://{}/chaise/recordset/#{}/".format(
            urlparse(catalog.get_server_uri()).netloc, self.catalog.catalog_id)

        self.graph.attr('graph', rankdir='LR')
        self.graph.attr('graph', overlap='false', splines='true')
        #self.graph.attr('graph', concentrate=True)

    def clear(self):
        self.graph.clear()

    def view(self):
        self.graph.view()

    def catalog_to_graph(self, schemas=None, skip_terms=False, skip_association_tables=False):
        """
        Convert a catalog to a DOT based graph.
        :param schemas:  List of schemas that should be included.  Use whole catalog if None.
        :param skip_terms: Do not include term tables in the graph
        :param skip_association_tables: Collapse association tables so that only edges between endpoints are used
        :return:
        """

        schemas = [s.name for s in self._model.schemas.values() if s.name not in ['_acl_admin', 'public', 'WWW']] \
            if schemas is None else schemas

        for schema in schemas:
            self.schema_to_graph(schema, skip_terms=skip_terms, schemas=schemas,
                                 skip_association_tables=skip_association_tables)

    def schema_to_graph(self, schema_name, schemas=[], skip_terms=False, skip_association_tables=False):
        """
        Create a graph for the specified schema.
        :param schema_name: Name of the schema in the model to be used.
        :param schemas: List of additional schemas to include in the graph.
        :param skip_terms:
        :param skip_association_tables:
        :return:
        """

        schema = self._model.schemas[schema_name]

        # Put nodes for each schema in a seperate subgraph.
        with self.graph.subgraph(name='cluster_' + schema_name, node_attr={'shape': 'box'}) as schema_graph:
            schema_graph.attr(style='invis')
            for table in schema.tables.values():
                node_name = '{}_{}'.format(schema_name, table.name)
                if DerivaCatalogToGraph._is_vocabulary_table(table):
                    if not skip_terms:
                        schema_graph.node(node_name, label='{}:{}'.format(schema_name, table.name),
                                          shape='ellipse',
                                          URL=self._chaise_uri(table))
                else:
                    # Skip over current table if it is a association table and option is set.
                    if not (table.is_association() and skip_association_tables):
                        schema_graph.node(node_name, label='{}:{}'.format(schema_name, table.name),
                                          shape='box',
                                          URL=self._chaise_uri(table))
                    else:
                        print('Skipping node', node_name)

        # We have all the nodes out now, so run over and add edges.
        for table in schema.tables.values():
            self.foreign_key_defs_to_graph(table,
                                           skip_terms=skip_terms,
                                           schemas=schemas,
                                           skip_association_tables=skip_association_tables)
        return

    def foreign_key_defs_to_graph(self, table, skip_terms=False, skip_association_tables=False, schemas=[]):
        """
        Add edges for each foreign key relationship in the specified table.
        :param table:
        :param skip_terms:
        :param skip_association_tables:
        :param skip_schemas:
        :return:
        """

        # If table is an association table, put in a edge between the two endpoints in the relation.
        if table.is_association() == 2 and skip_association_tables:
            t1 = table.foreign_keys[0].referenced_columns[0].table
            t2 = table.foreign_keys[1].referenced_columns[0].table
            t1_name = '{}_{}'.format(t1.schema.name, t1.name)
            t2_name = '{}_{}'.format(t2.schema.name, t2.name)
            self.graph.edge(t1_name, t2_name, dir='both', color='gray')
        else:
            for fkey in table.foreign_keys:
                referenced_table = list(fkey.column_map.values())[0].table
                table_name = '{}_{}'.format(referenced_table.schema.name, referenced_table.name)

                # If the target is a schema we are skipping, do not add an edge.
                if (referenced_table.schema.name not in schemas or table.schema.name not in schemas):
                    continue
                # If the target is a term table, and we are not including terms, do not add an edge.
                if DerivaCatalogToGraph._is_vocabulary_table(referenced_table) and skip_terms:
                    continue

                # Add an edge from the current node to the target table.
                self.graph.edge('{}_{}'.format(table.schema.name, table.name), table_name)

        return

    def save(self, filename=None, format='pdf', view=False):
        (dir, file) = os.path.split(os.path.abspath(filename))
        if 'gv' in format:
            self.graph.save(filename=file, directory=dir)
        else:
            print('dumping graph in file', file, format)
            self.graph.render(filename=file, directory=dir, view=view, cleanup=True, format=format)

    def _repr_svg_(self):
        return self.graph._repr_svg_()

    @staticmethod
    def _is_vocabulary_table(t):
        if t.schema.name.lower() in 'vocabulary':
            return True
        try:
            return t.columns['ID'] and t.columns['Name'] and t.columns['URI'] and t.columns['Synonyms']
        except KeyError:
            return False

    def _chaise_uri(self, table):
        return self._chaise_base + "{}:{}".format(table.schema.name, table.name)