from unittest import TestCase

from deriva.utils.catalog.manage.update_catalog import CatalogUpdater
from deriva.utils.catalog.manage.utils import LoopbackCatalog, TempErmrestCatalog
from deriva.core import get_credential
import deriva.core.ermrest_model as em


class TestCatalogUpdater(TestCase):
    def setUp(self):
        self.server = 'dev.isrd.isi.edu'
        self.credentials = get_credential(self.server)

    def test_update_catalog(self):
        with TempErmrestCatalog('https', self.server, credentials=self.credentials) as catalog:
            updater = CatalogUpdater(catalog)

            # Check if basic setting works....
            updated_annotations = {'tag:misd.isi.edu,2015:display': {'name': 'foo'}}
            updated_acls = {'insert': ['bill']}

            updater.update_catalog('acls', updated_annotations, updated_acls)
            self.assertEqual(catalog.getCatalogModel().acls['insert'], updated_acls['insert'])

            updater.update_catalog('annotations', updated_annotations, updated_acls)
            self.assertEqual(catalog.getCatalogModel().annotations, updated_annotations)

            # Check updates...
            updated_annotations = {'tag:misd.isi.edu,2015:display': {'name': 'bar'},
                                   'tag:isrd.isi.edu,2016:export': {'templates': 1}}
            updated_acls = {'insert': ['carl']}

            # Check updates...
            updater.update_catalog('acls', updated_annotations, updated_acls)
            self.assertEqual(catalog.getCatalogModel().acls['insert'], updated_acls['insert'])

            updater.update_catalog('annotations', updated_annotations, updated_acls)
            self.assertEqual(catalog.getCatalogModel().annotations, updated_annotations)

            # Check replace.
            updated_annotations = {'tag:isrd.isi.edu,2016:export': {'newtemplates': {}}}
            updater.update_catalog('annotations', updated_annotations, updated_acls, replace=True)
            self.assertEqual(catalog.getCatalogModel().annotations, updated_annotations)

    def test_update_schema(self):
        with TempErmrestCatalog('https', self.server, credentials=self.credentials) as catalog:
            updater = CatalogUpdater(catalog)

            # Create empty schema.
            schema_name = 'TestSchema'
            updater.update_schema('schema', em.Schema.define(schema_name))
            self.assertEqual(catalog.getCatalogModel().schemas[schema_name].name, schema_name)

            updated_annotations = {'tag:misd.isi.edu,2015:display': {'name': 'foo'}}
            updated_acls = {'owner': ['carl']}
            updated_comment = 'Updated comment'

            # Check if basic setting works....
            updated_annotations = {'tag:misd.isi.edu,2015:display': {'name': 'foo'}}
            updated_acls = {'owner': ['bob']}
            updated_comment = 'Updated comment'
            schema_def = em.Schema.define(schema_name, comment=updated_comment, acls=updated_acls,
                                          annotations=updated_annotations)
            updater.update_schema('acls', schema_def)
            self.assertEqual(catalog.getCatalogModel().schemas[schema_name].acls, updated_acls)

            updater.update_schema('comment', schema_def)
            self.assertEqual(catalog.getCatalogModel().schemas[schema_name].comment, updated_comment)

            updater.update_schema('annotations', schema_def)
            self.assertEqual(catalog.getCatalogModel().schemas[schema_name].annotations, updated_annotations)

            # Check updates...
            updated_annotations = {'tag:misd.isi.edu,2015:display': {'name': 'bar'},
                                   'tag:isrd.isi.edu,2016:export': {'templates': []}}
            updated_acls = {'owner': ['carl']}
            updated_comment = 'Updated comment two'
            schema_def = em.Schema.define(schema_name, comment=updated_comment, acls=updated_acls,
                                          annotations=updated_annotations)

            # Check updates...
            updater.update_schema('acls', schema_def)
            self.assertEqual(catalog.getCatalogModel().schemas[schema_name].acls, updated_acls)

            updater.update_schema('comment', schema_def)
            self.assertEqual(catalog.getCatalogModel().schemas[schema_name].comment, updated_comment)

            updater.update_schema('annotations', schema_def)
            self.assertEqual(catalog.getCatalogModel().schemas[schema_name].annotations, updated_annotations)

            # Check replace.
            updated_annotations = {'tag:isrd.isi.edu,2016:export': {'newtemplates': {}}}
            schema_def = em.Schema.define(schema_name, comment=updated_comment, acls=updated_acls,
                                          annotations=updated_annotations)
            updater.update_schema('annotations', schema_def, replace=True)
            self.assertEqual(catalog.getCatalogModel().schemas[schema_name].annotations, updated_annotations)

    def test_update_table(self):
        with TempErmrestCatalog('https', self.server, credentials=self.credentials) as catalog:
            updater = CatalogUpdater(catalog)

            schema_name = 'TestSchema'
            table_name = 'TestTable'

            # Create empty table.
            updater.update_schema('schema', em.Schema.define('TestSchema'))
            self.assertEqual(catalog.getCatalogModel().schemas[schema_name].name, 'TestSchema')
            updater.update_table('table', schema_name, em.Table.define('TestTable'))
            self.assertEqual(catalog.getCatalogModel().schemas[schema_name].tables[table_name].name, 'TestTable')
