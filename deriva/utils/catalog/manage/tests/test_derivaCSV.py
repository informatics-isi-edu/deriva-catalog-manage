from unittest import TestCase

import os
import tempfile
import deriva_csv
import dump_catalog
import deriva.core.ermrest_model as em
from loopback_catalog import LoopbackCatalog

class TestDerivaCSV(TestCase):
    def setUp(self):
        self.server = 'host.local'
        self.catalog_id = 1
        self.schema = 'TestSchema'
        self._catalog = LoopbackCatalog()
        model = self._catalog.getCatalogModel()
        model.create_schema(self._catalog, em.Schema.define('TestSchema'))

    def test_validate(self):
        pass

    def test_table_schema_from_catalog(self):
        pass

    def test_upload_to_deriva(self):
        pass

    def test_convert_to_deriva(self):

        tablefile = os.path.dirname(os.path.realpath(__file__)) + '/test1.csv'
        configfile = os.path.dirname(os.path.realpath(__file__)) + '/config.py'
        with tempfile.TemporaryDirectory() as dir:
            column_map = True
            fname = dir + '/test1.py'
            table = deriva_csv.DerivaCSV(tablefile, self.server, self.catalog_id, self.schema,
                                         config=configfile, column_map=column_map)
            self.assertEqual(table.headers, ['a','b','c'])

            pythonfile = '{}/{}.py'.format(dir,'test1')
            jsonfile = '{}/{}.py'.format(dir, 'test1')
            results = table.convert_to_deriva(outfile=pythonfile, schemafile=jsonfile)
            self.assertEqual(results, ({'a': 'A', 'b': 'B', 'c': 'C'}, {'text': ['a', 'c'], 'float8': ['b']}))

            m = dump_catalog.load_module_from_path(pythonfile)

    def test_create_validate_upload_csv(self):
        path = os.path.dirname(os.path.realpath(__file__)) + '/test1.csv'
        with tempfile.TemporaryDirectory() as dir:
            table = deriva_csv.DerivaCSV(path, self.server, self.catalog_id, self.schema, catalog=self._catalog)


        results = table.create_validate_upload_csv(convert=True, validate=True, create=True, upload=False,
                                   chunk_size=1000, starting_chunk=1)
        pass

    def test_map_name(self):
        path = os.path.dirname(os.path.realpath(__file__))

        column_map = None
        table = deriva_csv.DerivaCSV(path + '/test1.csv', self.server, self.catalog_id, self.schema, column_map=column_map)
        self.assertEqual(table.map_name('foo bar'), 'foo bar')

        column_map = False
        table = deriva_csv.DerivaCSV(path +'/test1.csv', self.server, self.catalog_id, self.schema, column_map=column_map)
        self.assertEqual(table.map_name('foo bar'), 'foo bar')

        column_map = True
        table = deriva_csv.DerivaCSV(path +'/test1.csv', self.server, self.catalog_id, self.schema, column_map=column_map)
        self.assertEqual(table.map_name('foo bar'), 'Foo_Bar')

        column_map = ['DNA', 'RNA']
        table = deriva_csv.DerivaCSV(path + '/test1.csv', self.server, self.catalog_id, self.schema, column_map=column_map)
        self.assertEqual(table.map_name('foo bar'), 'Foo_Bar')

        column_map = {'(%)': '(Percent)', 'RnA': 'RNA', 'dna': 'DNA',
                      'the hun':'Attila_The_Hun', 'the_clown': 'Bozo_The_Clown'}
        table = deriva_csv.DerivaCSV(path + '/test1.csv', self.server, self.catalog_id, self.schema,
                                     column_map=column_map)
        self.assertEqual(table.map_name('Change in value (%)'), 'Change_In_Value_(Percent)')
        self.assertEqual(table.map_name('amountDna'), 'Amount_DNA')
        self.assertEqual(table.map_name('the hun'),'Attila_The_Hun')
        self.assertEqual(table.map_name('the clown'), 'Bozo_The_Clown')
