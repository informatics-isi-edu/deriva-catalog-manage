from unittest import TestCase


import logging

from deriva.core import get_credential, DerivaServer
import deriva.core.ermrest_model as em
from deriva.utils.catalog.components.configure_catalog import DerivaCatalogConfigure
from deriva.utils.catalog.components.deriva_model import DerivaTable, DerivaCatalogError, chaise_tags,\
     DerivaSourceSpec, DerivaVisibleSources, DerivaContext, DerivaColumn, DerivaModel

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class TestVisibleSources(TestCase):
    server = 'dev.isrd.isi.edu'
    catalog_id = None
    schema_name = 'TestSchema'
    table_name = 'TestTable'
    catalog = None

    @classmethod
    def setUpClass(cls):
        credentials = get_credential(cls.server)
        catalog = DerivaServer('https', TestVisibleSources.server, credentials=credentials).create_ermrest_catalog()
        cls.catalog_id = catalog._catalog_id
        logger.info('Catalog_id is {}'.format(cls.catalog_id))

        cls.catalog = DerivaCatalogConfigure(cls.server, catalog_id=cls.catalog_id)
        with DerivaModel(cls.catalog) as m:
            model = m.catalog_model()
            model.create_schema(cls.catalog.ermrest_catalog, em.Schema.define(cls.schema_name))
            model.schemas[cls.schema_name].create_table(cls.catalog.ermrest_catalog,
                                                        em.Table.define(cls.table_name, []))
            model.schemas[cls.schema_name].create_table(cls.catalog.ermrest_catalog,
                                                        em.Table.define(cls.table_name + '1', []))
            for i in ['Foo', 'Foo1', 'Foo2']:
                DerivaColumn(cls.catalog[cls.schema_name][cls.table_name], i, 'text').create()
                DerivaColumn(cls.catalog[cls.schema_name][cls.table_name + '1'], i, 'text').create()

            model.schemas[cls.schema_name].tables[cls.table_name + '1'].create_key(
                cls.catalog.ermrest_catalog,
                em.Key.define(['Foo2'], constraint_names=[(cls.schema_name, '{}_{}_key'.format(cls.table_name, 'Foo3'))]))

            model.schemas[cls.schema_name].tables[cls.table_name].create_fkey(cls.catalog.ermrest_catalog,
                em.ForeignKey.define(['Foo2'], cls.schema_name, cls.table_name + '1', ['Foo2'],
                                     constraint_names=[[cls.schema_name, 'TestTable1_Foo2_fkey']]))


    @classmethod
    def tearDownClass(self):
        self.catalog.ermrest_catalog.delete_ermrest_catalog(really=True)

    def test_normalize(self):
        table = self.catalog[self.schema_name][self.table_name]
        self.assertEqual(DerivaSourceSpec(table, 'Foo').spec, {'source': 'Foo'},
                         msg="column spec failed")
        self.assertEqual(DerivaSourceSpec(table, 'Foo').column_name, 'Foo',
                         msg="column name for source spec failed")
        self.assertEqual(DerivaSourceSpec(table, {'source':'Foo'}).spec, {'source': 'Foo'},
                         msg="simple source spec failed")
        self.assertEqual(DerivaSourceSpec(table, [self.schema_name, 'TestTable1_Foo2_fkey']).spec,
            {'source': [{'outbound': ['TestSchema', 'TestTable1_Foo2_fkey']}, 'RID']})
        with self.assertRaises(DerivaCatalogError):
            DerivaSourceSpec(table, 'Foo3')

    def test_normalize_positions(self):
        DerivaVisibleSources._normalize_positions({'all'})

    def test_insert_sources(self):
        table = self.catalog[self.schema_name][self.table_name]
        table.annotations[chaise_tags.visible_columns] = {}

        vs = table.visible_columns
        vs.insert_context('*')
        vs.insert_sources([DerivaSourceSpec(table, 'Foo'), DerivaSourceSpec(table, 'Foo2')])
        vs.dump()


