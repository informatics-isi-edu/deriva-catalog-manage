import unittest

import sys
print(sys.path)

import deriva.utils.catalog.manage.deriva_csv as deriva_csv

class TestNameMap(unittest.TestCase):

    def setUp(self):
        self.server = 'host.local'
        self.catalog_id = 1
        self.schema = 'TestSchema'

    def test_no_map(self):
        print(deriva_csv.__file__)
        map_columns = None
        table = deriva_csv.DerivaCSV('Test.csv', self.server, self.catalog_id, self.schema, map_columns=map_columns)

        self.assertEqual(table.map_name('foo bar'), 'foo bar')


if __name__ == '__main__':
    unittest.main()
