from unittest import TestCase
import datetime
import os
import csv
import sys
import string
import tempfile
import random
import warnings
import logging

from tableschema import exceptions
from deriva.utils.catalog.manage.deriva_csv import DerivaCSV
import deriva.utils.catalog.manage.configure_catalog as configure_catalog
import deriva.utils.catalog.manage.dump_catalog as dump_catalog
from deriva.core import get_credential
import deriva.core.ermrest_model as em
from deriva.utils.catalog.manage.utils import TempErmrestCatalog
from deriva.utils.catalog.manage.tests.test_derivaCSV import generate_test_csv
from deriva.utils.catalog.components import create_asset_upload_spec, create_asset_table

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

warnings.filterwarnings("ignore", category=DeprecationWarning)

if sys.version_info >= (3, 0):
    from urllib.parse import urlparse
if sys.version_info < (3, 0) and sys.version_info >= (2, 5):
    from urlparse import urlparse


class TestCreate_model_elements(TestCase):
    def setUp(self):
        self.server = 'dev.isrd.isi.edu'
        self.credentials = get_credential(self.server)
        self.catalog_id = None
        self.schema_name = 'TestSchema'
        self.table_name = 'TestTable'

        self.table_size = 10
        self.column_count = 5
        self.test_dir = tempfile.mkdtemp()

        (row, self.headers) = generate_test_csv(self.column_count)
        self.tablefile = '{}/{}.csv'.format(self.test_dir, self.table_name)

        with open(self.tablefile, 'w', newline='') as f:
            tablewriter = csv.writer(f)
            for i, j in zip(range(self.table_size + 1), row):
                tablewriter.writerow(j)

        self.configfile = os.path.dirname(os.path.realpath(__file__)) + '/config.py'
        self.catalog = TempErmrestCatalog('https', self.server, credentials=self.credentials)

        model = self.catalog.getCatalogModel()
        model.create_schema(self.catalog, em.Schema.define(self.schema_name))

        self.table = DerivaCSV(self.tablefile, self.schema_name, column_map=True, key_columns='id')
      #  self._create_test_table()
        self.table.create_validate_upload_csv(self.catalog, create=True, upload=True)
        configure_catalog.configure_baseline_catalog(self.catalog, catalog_name='test', admin='isrd-systems')

        configure_catalog.configure_table_defaults(self.catalog, model.schemas[self.schema_name].tables[self.table_name],
                                                   public=True)

        logger.debug('Setup done....')
        # Make upload directory:
        # mkdir schema_name/table/
        #    schema/file/id/file1, file2, ....for

    def tearDown(self):
        self.catalog.delete_ermrest_catalog(really=True)
        logger.debug('teardown...')


    def test_create_asset_table(self):
        model = self.catalog.getCatalogModel()
        table = model.schemas[self.schema_name].tables[self.table_name]

        create_asset_table(self.catalog, table, 'ID')


    def test_create_asset_upload_spec(self):
        create_asset_upload_spec()
        self.fail()
