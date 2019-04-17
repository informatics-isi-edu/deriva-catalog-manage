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
import deriva.utils.catalog.components.model_elements as dm
from deriva.utils.catalog.manage.deriva_csv import DerivaCSV
from deriva.utils.catalog.components.configure_catalog import DerivaCatalogConfigure
from deriva.utils.catalog.components.model_elements import DerivaTable, DerivaCatalogError, \
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
        catalog_id = catalog._catalog_id
        logger.info('Catalog_id is {}'.format(catalog_id))

        cls.catalog = DerivaCatalogConfigure(cls.server, catalog_id=catalog_id)

    @classmethod
    def tearDownClass(self):
        self.catalog.ermrest_catalog.delete_ermrest_catalog(really=True)

    def setUp(self):
        with DerivaModel(self.catalog) as m:
            model = m.catalog_model()
            model.catalog_model(self.catalog).create_schema(self.catalog, em.Schema.define(self.schema_name))
            model.schemas[self.schema_name].create_table(em.Table.Define(self.table_name,[] ))

    def tearDown(self):
        with DerivaModel(self.catalog) as m:
            model = m.catalog_model()
            model.schemas(self.schema_name).delete()

    def test_visible_columns(self):
        table = self.catalog[self.schema_name][self.table_name]

    def test_column(self):
        table = self.catalog['public']['ERMrest_Client']
        self.assertEqual(table['RID'].name == 'RID')
        self.assertEqual(table.column('RID') == 'RID')
        self.assertEqual(table.columns['RID'] == 'RID')
        self.assertTrue( {'RID','RCB','RMB','RCT','RMT'} < {i.name for i in table.columns})
        table['RID'].dump()
        self.assertIsInstance(table['RID'].definition(), em.Column)

    def test_column_create_delete(self):
        table = self.catalog[self.schema_name][self.table_name]
        col = DerivaColumn(table, 'Foo','text')
        self.assertEqual(table['Foo'].name, 'Foo')
        col.delete()
        with self.assertRaises(DerivaCatalogError):
            table['Foo']

    def test_columns(self):
        self.fail()

    def test_create_columns(self):
        self.fail()

    def test_rename_column(self):
        self.fail()

    def test_rename_columns(self):
        self.fail()
