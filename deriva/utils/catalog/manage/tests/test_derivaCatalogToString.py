from unittest import TestCase
import tempfile
import sys
import deriva.core.ermrest_model as em
from utils import LoopbackCatalog, TempErmrestCatalog
from deriva.core import ErmrestCatalog, get_credential
from dump_catalog import DerivaCatalogToString, DerivaConfig, load_module_from_path

if sys.version_info >= (3, 0):
    from urllib.parse import urlparse
if sys.version_info < (3, 0) and sys.version_info >= (2, 5):
    from urlparse import urlparse


class TestDerivaCatalogToString(TestCase):
    def setUp(self):
        self.server = 'dev.isrd.isi.edu'
        self.credentials = get_credential(self.server)
        DerivaConfig('config.py')
        self._variables = {k: v for k, v in DerivaConfig.groups.items()}
        self._variables.update(DerivaConfig.tags)

    def test_substitute_variables(self):
        pass

    def test_variable_to_str(self):
        pass

    def test_tag_variables_to_str(self):
        pass

    def test_annotations_to_str(self):
        pass

    def test_schema_to_str(self):
        pass

    def test_catalog_to_str(self):
        with TempErmrestCatalog('https',self.server, credentials=self.credentials) as catalog:
            model = catalog.getCatalogModel()
            model.create_schema(catalog, em.Schema.define('TestSCchema'))
            stringer = DerivaCatalogToString(catalog, variables=self._variables)
            catalog_string = stringer.catalog_to_str()
            tdir = tempfile.mkdtemp()
            with open('{}/TestCatalog.py'.format(tdir), mode='w') as f:
                print(catalog_string)
                print(catalog_string, file=f)
                m = load_module_from_path(f.name)
                print(m.__file__)
                print(m.__dict__)

                with TempErmrestCatalog('https', self.server, credentials=self.credentials) as test_catalog:
                    server = urlparse(test_catalog.get_server_uri()).hostname
                    catalog_id = catalog.get_server_uri().split('/')[-1]
                    m.main(test_catalog, 'catalog')
                    test_model = test_catalog.getCatalogModel()
                self.assertEqual(test_model, model)

    def test_table_annotations_to_str(self):
        pass

    def test_column_annotations_to_str(self):
        pass

    def test_foreign_key_defs_to_str(self):
        pass

    def test_key_defs_to_str(self):
        pass

    def test_column_defs_to_str(self):
        pass

    def test_table_def_to_str(self):
       pass

    def test_table_to_str(self):
        pass
