from unittest import TestCase

import os
import sys
import tempfile
import deriva_csv
import dump_catalog
from deriva.core import get_credential
import deriva.core.ermrest_model as em
from utils import LoopbackCatalog, TempErmrestCatalog

if sys.version_info >= (3, 0):
    from urllib.parse import urlparse
if sys.version_info < (3, 0) and sys.version_info >= (2, 5):
    from urlparse import urlparse


class TestDerivaCSV(TestCase):
    def setUp(self):
        self.server = 'dev.isrd.isi.edu'
        self.credentials = get_credential(self.server)
        self.catalog_id = None
        self.schema_name = 'TestSchema'

    def test_validate(self):
        pass

    def test_table_schema_from_catalog(self):
        tablefile = os.path.dirname(os.path.realpath(__file__)) + '/test1.csv'
        configfile = os.path.dirname(os.path.realpath(__file__)) + '/config.py'

        with TempErmrestCatalog('https', self.server, self.credentials) as catalog:
            model = catalog.getCatalogModel()

            # Set up a table with the right schema.
            model.create_schema(catalog, em.Schema.define(self.schema_name))
            src_table = deriva_csv.DerivaCSV(tablefile, self.schema_name, column_map=False)
            src_table.create_validate_upload_csv(catalog, convert=True, validate=True, create=True)

            table = deriva_csv.DerivaCSV(tablefile, self.schema_name, column_map=False)
            tableschema = table.table_schema_from_catalog(catalog)

            self.assertEqual([i['name'] for i in src_table.schema.descriptor['fields']],
                             [i['name'] for i in tableschema.descriptor['fields']])
            self.assertEqual([i['type'] for i in src_table.schema.descriptor['fields']],
                             [i['type'] for i in tableschema.descriptor['fields']])

    def test_upload_to_deriva(self):
        pass

    def test_convert_to_deriva(self):
        tablefile = os.path.dirname(os.path.realpath(__file__)) + '/test1.csv'
        configfile = os.path.dirname(os.path.realpath(__file__)) + '/config.py'
        with TempErmrestCatalog('https', self.server, self.credentials) as catalog:
            model = catalog.getCatalogModel()
            model.create_schema(catalog, em.Schema.define(self.schema_name))
            column_map = True
            table = deriva_csv.DerivaCSV(tablefile, self.schema_name, config=configfile, column_map=column_map)
            self.assertEqual(table.headers, ['a', 'b', 'c'])

            with tempfile.TemporaryDirectory() as tmpdir:
                pythonfile = '{}/{}.py'.format(tmpdir, 'test1')
                jsonfile = '{}/{}.py'.format(tmpdir, 'test1')
                results = table.convert_to_deriva(outfile=pythonfile, schemafile=jsonfile)
                self.assertEqual(results, ({'a': 'A', 'b': 'B', 'c': 'C'}, {'text': ['a', 'c'], 'float8': ['b']}))

                with TempErmrestCatalog('https', self.server, credentials=self.credentials) as test_catalog:
                    test_catalog.getCatalogModel().create_schema(test_catalog, em.Schema.define(self.schema_name))
                    server = urlparse(test_catalog.get_server_uri()).hostname
                    catalog_id = test_catalog.get_server_uri().split('/')[-1]
                    m = dump_catalog.load_module_from_path(pythonfile)
                    m.main(test_catalog, 'table')

    def test_create_validate_upload_csv(self):
        tablefile = os.path.dirname(os.path.realpath(__file__)) + '/test1.csv'
        configfile = os.path.dirname(os.path.realpath(__file__)) + '/config.py'
        with TempErmrestCatalog('https', self.server, self.credentials) as catalog:
            model = catalog.getCatalogModel()
            model.create_schema(catalog, em.Schema.define(self.schema_name))

            table = deriva_csv.DerivaCSV(tablefile, self.schema_name)
            row_cnt, chunk_size, chunk_cnt = table.create_validate_upload_csv(catalog, convert=True, validate=True,
                                                                              create=True, upload=True,
                                                                              chunk_size=1000, starting_chunk=1)
            self.assertEqual(row_cnt, 3)

    def test_map_name(self):
        path = os.path.dirname(os.path.realpath(__file__))

        column_map = None
        table = deriva_csv.DerivaCSV(path + '/test1.csv', self.schema_name, column_map=column_map)
        self.assertEqual(table.map_name('foo bar'), 'foo bar')

        column_map = False
        table = deriva_csv.DerivaCSV(path + '/test1.csv', self.schema_name, column_map=column_map)
        self.assertEqual(table.map_name('foo bar'), 'foo bar')

        column_map = True
        table = deriva_csv.DerivaCSV(path + '/test1.csv', self.schema_name, column_map=column_map)
        self.assertEqual(table.map_name('foo bar'), 'Foo_Bar')

        column_map = ['DNA', 'RNA']
        table = deriva_csv.DerivaCSV(path + '/test1.csv', self.schema_name, column_map=column_map)
        self.assertEqual(table.map_name('foo bar'), 'Foo_Bar')

        column_map = {'(%)': '(Percent)', 'RnA': 'RNA', 'dna': 'DNA',
                      'the hun': 'Attila_The_Hun', 'the_clown': 'Bozo_The_Clown'}
        table = deriva_csv.DerivaCSV(path + '/test1.csv', self.schema_name, column_map=column_map)
        self.assertEqual(table.map_name('Change in value (%)'), 'Change_In_Value_(Percent)')
        self.assertEqual(table.map_name('amountDna'), 'Amount_DNA')
        self.assertEqual(table.map_name('the hun'), 'Attila_The_Hun')
        self.assertEqual(table.map_name('the clown'), 'Bozo_The_Clown')
