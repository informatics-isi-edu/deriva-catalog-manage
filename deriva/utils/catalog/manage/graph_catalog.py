from __future__ import print_function

import argparse
import importlib
import os
import re
import sys
from graphviz import Digraph

#overlap=false;
#splines=true;

from attrdict import AttrDict
from deriva.core import ErmrestCatalog, get_credential

from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.deriva_file_templates import table_file_template, schema_file_template, \
    catalog_file_template

class DerivaCatalogToGraph:
    def __init__(self, catalog):
        print('hello')
        self.graph = Digraph(
            engine='neato',
            format='svg',
            edge_attr=None,
            strict=True)
        #self.attr('graph', rankdir='LR')
        #self.attr('graph', overlap='false', splines='true')
        self.model = catalog.getCatalogModel()

    def is_term_table(self, table):
        try:
            result = table.column_definitions['id'] and \
                     table.column_definitions['uri'] and \
                     table.column_definitions['name']
        except KeyError:
            result = False
        return result

    def catalog_to_graph(self):
        for schema in self.model.schemas:
            self.schema_to_graph(schema.name)

    def schema_to_graph(self, schema_name):
        schema = self.model.schemas[schema_name]

        with self.graph.subgraph(name='cluster_' + schema_name, node_attr={'shape': 'box'}) as schema_graph:
            for table_name in schema.tables:
                node_name = '{}_{}'.format(schema_name, table_name)
                if self.is_term_table(schema.tables[table_name]):
                    self.node(node_name, label='{}:{}'.format(schema_name, table_name), shape='ellipse')
                else:
                    self.node(node_name, label='{}:{}'.format(schema_name, table_name), shape='box')
                    self.foreign_key_defs_to_graph(schema.tables[table_name])
        return

    def foreign_key_defs_to_graph(self, table):
        for fkey in table.foreign_keys:
            target_table = '{}_{}'.format(fkey.referenced_columns[0]['schema_name'],
                                          fkey.referenced_columns[0]['table_name'])
            if target_table == 'public_ermrest_client':
                continue
            self.graph.edge('{}_{}'.format(table.sname, table.name),target_table)
        return

def main():
    parser = argparse.ArgumentParser(description='Dump definition for catalog {}:{}')
    parser.add_argument('server', help='Catalog server name')
    parser.add_argument('--catalog', default=1, help='ID number of desired catalog')
    parser.add_argument('--dir', default="catalog-configs", help='output directory name)')
    parser.add_argument('--config', default=None, help='python script to set up configuration variables)')
    parser.add_argument('--table', default=None, help='Only dump out the spec for the specified table.  Format is '
                                                      'schema_name:table_name')
    args = parser.parse_args()

    dumpdir = args.dir
    server = args.server
    catalog_id = args.catalog
    configfile = args.config
    table = args.table

    try:
        os.makedirs(dumpdir, exist_ok=True)
    except OSError:
        print("Creation of the directory %s failed" % dumpdir)
        sys.exit(1)

    credential = get_credential(server)
    catalog = ErmrestCatalog('https', server, catalog_id, credentials=credential)
    model_root = catalog.getCatalogModel()

    graph = DerivaCatalogToGraph(catalog)

    if table is not None:
        if ':' not in table:
            print('Table name must be in form of schema:table')
            exit(1)
        print("Dumping out table def....")
        [schema_name, table_name] = table.split(":")
        table_string = stringer.table_to_str(schema_name, table_name)
        with open(table_name + '.py', 'w') as f:
            print(table_string, file=f)
    else:
        print("Dumping catalog def....")
        catalog_string = stringer.catalog_to_str()

        with open('{}/{}_{}.py'.format(dumpdir, server, catalog_id), 'w') as f:
            print(catalog_string, file=f)

        for schema_name in model_root.schemas:
            print("Dumping schema def for {}....".format(schema_name))
            schema_string = stringer.schema_to_str(schema_name)

            with open('{}/{}.schema.py'.format(dumpdir, schema_name), 'w') as f:
                print(schema_string, file=f)

        for schema_name, schema in model_root.schemas.items():
            for i in schema.tables:
                print('Graphing {}:{}'.format(schema_name, i))
                table_string = stringer.table_to_str(schema_name, i)
                filename = '{}/{}/{}.py'.format(dumpdir, schema_name, i)
                os.makedirs(os.path.dirname(filename), exist_ok=True)
                with open(filename, 'w') as f:
                    print(table_string, file=f)


if __name__ == "__main__":
    main()
