from unittest import TestCase

from update_catalog import CatalogUpdater
from loopback_catalog import LoopbackCatalog
import deriva.core.ermrest_model as em

class TestCatalogUpdater(TestCase):
    def setUp(self):
        self._catalog = LoopbackCatalog()
        self._updater = CatalogUpdater( 'host.local', 1, catalog=self._catalog)

    def test_update_catalog_1(self):
        updater = self._updater
        model = self._catalog.getCatalogModel()

        # Check if basic setting works....
        updated_annotations = {'tag:misd.isi.edu,2015:display': {'name': 'foo'}}
        updated_acls = {'owner': ['*']}

        updater.update_catalog('acls', updated_annotations, updated_acls)
        self.assertEqual(model.acls, updated_acls)

        updater.update_catalog('annotations', updated_annotations, updated_acls)
        self.assertEqual(model.annotations, updated_annotations)

        # Check updates...
        updated_annotations = {'tag:misd.isi.edu,2015:display': {'name': 'bar'},
                                'tag:isrd.isi.edu,2016:export': {'templates'}}
        updated_acls = {'owner': ['carl']}

        # Check updates...
        updater.update_catalog('acls', updated_annotations, updated_acls)
        self.assertEqual(model.acls, updated_acls)

        updater.update_catalog('annotations', updated_annotations, updated_acls)
        self.assertEqual(model.annotations, updated_annotations)

        # Check replace.
        updated_annotations = {'tag:isrd.isi.edu,2016:export': {'newtemplates'}}
        updater.update_catalog('annotations', updated_annotations, updated_acls, replace=True)
        self.assertEqual(model.annotations, updated_annotations)


    def test_update_schema(self):
        updater = self._updater
        model = self._catalog.getCatalogModel()

        # Create empty schema.
        schema_name = 'TestSchema'
        updater.update_schema('schema',  em.Schema.define(schema_name))
        self.assertEqual(model.schemas[schema_name].name, schema_name)

        updated_annotations = {'tag:misd.isi.edu,2015:display': {'name': 'foo'}}
        updated_acls = {'owner': ['*']}
        updated_comment = 'Updated comment'

        # Check if basic setting works....
        updated_annotations = {'tag:misd.isi.edu,2015:display': {'name': 'foo'}}
        updated_acls = {'owner': ['*']}
        updated_comment='Updated comment'
        schema_def = em.Schema.define(schema_name, comment=updated_comment, acls=updated_acls, annotations=updated_annotations)
        updater.update_schema('acls', schema_def)
        self.assertEqual(model.schemas[schema_name].acls, updated_acls)

        updater.update_schema('comment', schema_def)
        self.assertEqual(model.schemas[schema_name].comment, updated_comment)

        updater.update_schema('annotations', schema_def)
        self.assertEqual(model.schemas[schema_name].annotations, updated_annotations)

        # Check updates...
        updated_annotations = {'tag:misd.isi.edu,2015:display': {'name': 'bar'},
                                'tag:isrd.isi.edu,2016:export': {'templates'}}
        updated_acls = {'owner': ['carl']}
        updated_comment='Updated comment two'
        schema_def = em.Schema.define(schema_name, comment=updated_comment, acls=updated_acls,
                                      annotations=updated_annotations)

        # Check updates...
        updater.update_schema('acls', schema_def)
        self.assertEqual(model.schemas[schema_name].acls, updated_acls)

        updater.update_schema('comment',  schema_def)
        self.assertEqual(model.schemas[schema_name].comment, updated_comment)

        updater.update_schema('annotations', schema_def)
        self.assertEqual(model.schemas[schema_name].annotations, updated_annotations)

        # Check replace.
        updated_annotations = {'tag:isrd.isi.edu,2016:export': {'newtemplates'}}
        schema_def = em.Schema.define(schema_name, comment=updated_comment, acls=updated_acls,
                                      annotations=updated_annotations)
        updater.update_schema('annotations', schema_def, replace=True)
        self.assertEqual(model.schemas[schema_name].annotations, updated_annotations)


    def test_update_table(self):
        updater = self._updater
        model = self._catalog.getCatalogModel()

        schema_name='TestSchema'
        table_name = 'TestTable'

        # Create empty table.
        updater.update_schema('schema',  em.Schema.define('TestSchema'))
        self.assertEqual(model.schemas[schema_name].name, 'TestSchema')
        updater.update_table('table', schema_name, em.Table.define('TestTable'))
        self.assertEqual(model.schemas[schema_name].tables[table_name].name, 'TestTable')


