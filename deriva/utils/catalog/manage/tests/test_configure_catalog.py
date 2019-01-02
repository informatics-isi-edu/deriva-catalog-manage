from unittest import TestCase
import datetime
import os
import csv
import sys
import string
import tempfile
import random
import warnings

from tableschema import exceptions
from deriva.utils.catalog.manage.deriva_csv import DerivaCSV
import deriva.utils.catalog.manage.dump_catalog as dump_catalog
from deriva.core import get_credential
import deriva.core.ermrest_model as em
from deriva.utils.catalog.manage.utils import TempErmrestCatalog

warnings.filterwarnings("ignore", category=DeprecationWarning)

if sys.version_info >= (3, 0):
    from urllib.parse import urlparse
if sys.version_info < (3, 0) and sys.version_info >= (2, 5):
    from urlparse import urlparse


def generate_test_csv(columncnt):
    """
    Generate a test CSV file for testing derivaCSV routines.  First row returned will be a header.
    :param columncnt: Number of columns to be used in the CSV.
    :return: generator function and a map of the column names and types.
    """
    type_list = ['int4', 'boolean', 'float8', 'date', 'text']
    column_types = ['int4'] + [type_list[i % len(type_list)] for i in range(columncnt)]
    column_headings = ['id'] + ['field {}'.format(i) for i in range(len(column_types))]

    missing_value = .2  # What fraction of values should be empty.

    base = datetime.datetime.today()
    date_list = [base - datetime.timedelta(days=x) for x in range(0, 100)]

    def col_value(c):
        v = ''

        if random.random() > missing_value:
            if c == 'boolean':
                v = random.choice(['true', 'false'])
            elif c == 'int4':
                v = random.randrange(-1000, 1000)
            elif c == 'float8':
                v = random.uniform(-1000, 1000)
            elif c == 'text':
                v = ''.join(random.sample(string.ascii_letters + string.digits, 5))
            elif c == 'date':
                v = str(random.choice(date_list))
        return v

    def row_generator(header=True):
        row_count = 1
        while True:
            if header is True:
                row = column_headings
                header = False
            else:
                row = [row_count]
                row_count += 1
                row.extend([col_value(i) for i in column_types])
            yield row

    return row_generator(), [{'name': i[0], 'type': i[1]} for i in zip(column_headings, column_types)]


class TestConfigureCatalog(TestCase):
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

        self.table_size = 1000
        self.column_count = 20
        self.test_dir = tempfile.mkdtemp()

        (row, self.headers) = generate_test_csv(self.column_count)

        self.tablefile = '{}/{}.csv'.format(self.test_dir, self.table_name)
        with open(self.tablefile, 'w', newline='') as f:
            tablewriter = csv.writer(f)
            for i, j in zip(range(self.table_size + 1), row):
                tablewriter.writerow(j)

        # Make upload directory:
            schema/file/id/file1, file2, ....for

    def tearDown(self):
        self.catalog.delete_ermrest_catalog(really=True)

    def _create_test_table(self):
        pyfile = '{}/{}.py'.format(self.test_dir, self.table_name)
        try:
            self.table.convert_to_deriva(outfile=pyfile)
            tablescript = dump_catalog.load_module_from_path(pyfile)
            tablescript.main(self.catalog, 'table')
        except ValueError as e:
            print(e)

