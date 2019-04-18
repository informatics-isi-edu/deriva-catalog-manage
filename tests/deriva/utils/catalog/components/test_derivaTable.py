from unittest import TestCase

import random
import datetime
import string
import os
import csv
import logging
import warnings

from deriva.core import get_credential, DerivaServer
import deriva.core.ermrest_model as em
import deriva.utils.catalog.components.deriva_model as dm
from deriva.utils.catalog.manage.deriva_csv import DerivaCSV
from deriva.utils.catalog.components.configure_catalog import DerivaCatalogConfigure
from deriva.utils.catalog.components.deriva_model import DerivaTable, DerivaCatalogError, \
     DerivaKey, DerivaForeignKey, DerivaVisibleSources, DerivaContext, DerivaColumn, DerivaModel

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

warnings.filterwarnings("ignore", category=DeprecationWarning,
                                   module=em.__name__)

class TestDerivaTable(TestCase):
    server = 'dev.isrd.isi.edu'
    catalog_id = None
    schema_name = 'TestSchema'
    table_name = 'TestTable'
    catalog = None

    @classmethod
    def setUpClass(cls):
        credentials = get_credential(cls.server)
        catalog = DerivaServer('https', TestDerivaTable.server, credentials=credentials).create_ermrest_catalog()
        cls.catalog_id = catalog._catalog_id
        logger.info('Catalog_id is {}'.format(cls.catalog_id))

        cls.catalog = DerivaCatalogConfigure(cls.server, catalog_id=cls.catalog_id)
        with DerivaModel(cls.catalog) as m:
            model = m.catalog_model()
            model.create_schema(cls.catalog.ermrest_catalog, em.Schema.define(cls.schema_name))
            model.schemas[cls.schema_name].create_table(cls.catalog.ermrest_catalog, em.Table.define(cls.table_name, []))

    @classmethod
    def tearDownClass(self):
        self.catalog.ermrest_catalog.delete_ermrest_catalog(really=True)

    def test_visible_columns(self):
        table = self.catalog[self.schema_name][self.table_name]

    def test_column(self):
        table = self.catalog['public']['ERMrest_Client']
        self.assertEqual(table['RID'].name, 'RID')
        self.assertEqual(table.column('RID').name, 'RID')
        self.assertEqual(table.columns['RID'].name, 'RID')
        self.assertTrue( {'RID','RCB','RMB','RCT','RMT'} < {i.name for i in table.columns})
        table['RID'].dump()
        self.assertIsInstance(table['RID'].definition(), em.Column)

    def test_derivacolumn_create_delete(self):
        table = self.catalog[self.schema_name][self.table_name]
        col = DerivaColumn(table, 'Foo','text')
        col.create()
        self.assertEqual(table['Foo'].name, 'Foo')
        col.delete()
        with self.assertRaises(DerivaCatalogError):
            table['Foo']

    def test_table_column_funcs(self):
        table = self.catalog[self.schema_name][self.table_name]
        table.visible_columns.insert_context('*')
        table.create_columns(DerivaColumn(table, 'Foo', 'text'))
        assert (table['Foo'].name == 'Foo')
        print('Column added')
        table.visible_columns.dump()
        print('visible columns.')
        table.column('Foo').delete()

    def test_keys(self):
        table = self.catalog[self.schema_name][self.table_name]
        table.visible_columns.insert_context('*')
        table.create_columns([DerivaColumn(table, 'Foo1', 'text'),
                              DerivaColumn(table, 'Foo2', 'text'),
                              DerivaColumn(table, 'Foo3', 'text'))
        table.

    def test_columns(self):
        pass

    def test_create_columns(self):
        pass

    def test_rename_column(self):
        pass

    def test_rename_columns(self):
        pass
