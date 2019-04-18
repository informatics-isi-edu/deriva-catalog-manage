import unittest
from unittest import TestCase
import os
import csv
import sys
import tempfile
import warnings
import logging


@unittest.skip("showing class skipping")
class TestCreate_model_elements(TestCase):
    def setUp(self):
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