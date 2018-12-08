from unittest import TestCase

import os
import csv
import sys
import tempfile
from deriva.utils.catalog.manage.deriva_csv import DerivaCSV
import deriva.utils.catalog.manage.dump_catalog as dump_catalog
from deriva.core import get_credential
import deriva.core.ermrest_model as em
from deriva.utils.catalog.manage.utils import generate_test_csv, TempErmrestCatalog

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
        self.table_name = 'TestTable'

        self.configfile = os.path.dirname(os.path.realpath(__file__)) + '/config.py'
        self.catalog = TempErmrestCatalog('https', self.server, credentials=self.credentials)
        model = self.catalog.getCatalogModel()
        model.create_schema(self.catalog, em.Schema.define(self.schema_name))

        self.table_size = 100
        self.column_count = 20
        self.test_dir = tempfile.mkdtemp()

        (row, self.headers) = generate_test_csv(self.column_count)

        self.tablefile = '{}/{}.csv'.format(self.test_dir, self.table_name)
        with open(self.tablefile,'w') as f:
            tablewriter = csv.writer(f)
            for i, j in zip(range(self.table_size), row):
                tablewriter.writerow(j)

        self.table = DerivaCSV(self.tablefile, self.schema_name, key_columns='id', column_map=True)

    def _create_test_table(self):
        pyfile = '{}/{}.py'.format(self.test_dir, self.table_name)
        try:
            self.table.convert_to_deriva(outfile=pyfile)
            tablescript = dump_catalog.load_module_from_path(pyfile)
            tablescript.main(self.catalog, 'table')
        except ValueError as e:
            print(e)

    def test_map_name(self):
        path = os.path.dirname(os.path.realpath(__file__))

        column_map = None
        table = DerivaCSV(path + '/test1.csv', self.schema_name, column_map=column_map)
        self.assertEqual(table.map_name('foo bar'), 'foo bar')

        column_map = False
        table = DerivaCSV(path + '/test1.csv', self.schema_name, column_map=column_map)
        self.assertEqual(table.map_name('foo bar'), 'foo bar')

        column_map = True
        table = DerivaCSV(path + '/test1.csv', self.schema_name, column_map=column_map)
        self.assertEqual(table.map_name('foo bar'), 'Foo_Bar')

        column_map = ['DNA', 'RNA']
        table = DerivaCSV(path + '/test1.csv', self.schema_name, column_map=column_map)
        self.assertEqual(table.map_name('foo bar'), 'Foo_Bar')

        column_map = {'(%)': '(Percent)', 'RnA': 'RNA', 'dna': 'DNA',
                      'the hun': 'Attila_The_Hun', 'the_clown': 'Bozo_The_Clown'}
        table = DerivaCSV(path + '/test1.csv', self.schema_name, column_map=column_map)
        self.assertEqual(table.map_name('Change in value (%)'), 'Change_In_Value_(Percent)')
        self.assertEqual(table.map_name('amountDna'), 'Amount_DNA')
        self.assertEqual(table.map_name('the hun'), 'Attila_The_Hun')
        self.assertEqual(table.map_name('the clown'), 'Bozo_The_Clown')

    def test_convert_to_deriva(self):
        self._create_test_table()
        tname = self.table.map_name(self.table_name)

        self.assertEqual(tname, self.catalog.getCatalogModel().schemas[self.schema_name].tables[tname].name)

    def test_table_schema_from_catalog(self):

            self._create_test_table()

            tableschema = self.table.table_schema_from_catalog(self.catalog)

            self.assertEqual([i['name'] for i in self.table.schema.descriptor['fields']],
                             [i['name'] for i in tableschema.descriptor['fields']])
            self.assertEqual([i['type'] for i in self.table.schema.descriptor['fields']],
                             [i['type'] for i in tableschema.descriptor['fields']])

    def test_validate(self):
        self._create_test_table()
        self.table.validate(self.catalog)

    def test_upload_to_deriva(self):
        self._create_test_table()
        row_count = self.table.upload_to_deriva(self.catalog, chunk_size=10)
        self.assertEqual(row_count, self.table_size)

    def test_create_validate_upload_csv(self):
        pass
