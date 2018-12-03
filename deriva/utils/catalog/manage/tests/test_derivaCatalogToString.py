from unittest import TestCase
import tempfile
import deriva.core.ermrest_model as em
from loopback_catalog import LoopbackCatalog
from dump_catalog import DerivaCatalogToString, DerivaConfig, load_module_from_path

class TestDerivaCatalogToString(TestCase):
    def setUp(self):
        self._catalog = LoopbackCatalog()
        DerivaConfig('config.py')
        self._variables = {k: v for k, v in DerivaConfig.groups.items()}
        self._variables.update(DerivaConfig.tags)

        self.stringer = DerivaCatalogToString(self._catalog.getCatalogModel(), 'host.local', 1, variables=self._variables)



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
        catalog_string = self.stringer.catalog_to_str()
#        with tempfile.NamedTemporaryFile(mode='w', suffix='.py') as f:
        with open('foo.py', mode='w') as f:
            print(catalog_string, file=f)
            m = load_module_from_path(f.name)
            print(m.__name__, m.__file__)


            newcatalog = LoopbackCatalog()
            m.main('catalog', newcatalog._server, newcatalog._catalog_id, catalog=newcatalog)

        newmodel = DerivaCatalogToString(newcatalog.getCatalogModel(), 'host.local', 1, variables=self._variables)
        self.assertEqual(newmodel._model, self.stringer._model)

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
