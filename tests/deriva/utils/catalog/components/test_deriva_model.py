from unittest import TestCase
import warnings
import logging

from deriva.core import get_credential, DerivaServer
import deriva.core.ermrest_model as em
from deriva.utils.catalog.components.deriva_model import *
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

warnings.filterwarnings("ignore", category=DeprecationWarning)

server = 'dev.isrd.isi.edu'
catalog_id = 1
catalog = None
schema_name = 'TestSchema'
ermrest_catalog = None


def clean_schema(schema_name):
    model = ermrest_catalog.getCatalogModel()
    for t in model.schemas[schema_name].tables.values():
        print(t)
        for k in t.foreign_keys:
            k.delete(catalog.ermrest_catalog, t)
    for t in [i for i in model.schemas[schema_name].tables.values()]:
        t.delete(catalog.ermrest_catalog,model.schemas[schema_name])


def setUpModule():
    global catalog, catalog_id, ermrest_catalog

    credentials = get_credential(server)
    catalog = DerivaServer('https', server, credentials=credentials).create_ermrest_catalog()
    catalog_id = catalog._catalog_id
    logger.info('Catalog_id is {}'.format(catalog_id))

    catalog = DerivaCatalog(server, catalog_id=catalog_id)
    ermrest_catalog = catalog.ermrest_catalog
    with DerivaModel(catalog) as m:
        model = m.catalog_model()
        model.create_schema(catalog.ermrest_catalog, em.Schema.define(schema_name))

def tearDownModule():
    catalog.ermrest_catalog.delete_ermrest_catalog(really=True)


class TestVisibleSources(TestCase):
    t1 = None
    t2 = None
    @classmethod
    def setUpClass(cls):
        model = catalog.ermrest_catalog.getCatalogModel()
        t1 = model.schemas[schema_name].create_table(catalog.ermrest_catalog, em.Table.define('TestTable', []))
        t2 = model.schemas[schema_name].create_table(catalog.ermrest_catalog, em.Table.define('TestTable1', []))

        for i in ['Foo', 'Foo1', 'Foo2']:
            t1.create_column(ermrest_catalog, em.Column.define(i, em.builtin_types['text']))
            t2.create_column(ermrest_catalog, em.Column.define(i, em.builtin_types['text']))

        t2.create_key(
            ermrest_catalog,
            em.Key.define(['Foo2'], constraint_names=[(schema_name, 'TestTable_Foo2_key')])
        )

        t1.create_fkey(
            ermrest_catalog,
            em.ForeignKey.define(['Foo2'], schema_name, 'TestTable1', ['Foo2'],
                                 constraint_names=[[schema_name, 'TestTable1_Foo2_fkey']])
        )

    def setUp(self):
        clean_schema('TestSchema')

    def test_normalize(self):
        table = self.t1
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
        table = ermrest_catalog.getCatalogModel()[schema_name][self.table_name]
        table.annotations[chaise_tags.visible_columns] = {}

        vs = table.visible_columns
        vs.insert_context('*')
        vs.insert_sources([DerivaSourceSpec(table, 'Foo'), DerivaSourceSpec(table, 'Foo2')])
        vs.dump()


class TestDerivaTable(TestCase):
    def setUp(self):
        clean_schema(schema_name)
        model = ermrest_catalog.getCatalogModel()
        t1 = model.schemas[schema_name].create_table(catalog.ermrest_catalog, em.Table.define('TestTable', []))
        t2 = model.schemas[schema_name].create_table(catalog.ermrest_catalog, em.Table.define('TestTable1', []))

        for i in ['Foo', 'Foo1', 'Foo2']:
            t1.create_column(ermrest_catalog, em.Column.define(i, em.builtin_types['text']))
            t2.create_column(ermrest_catalog, em.Column.define(i, em.builtin_types['text']))

        t2.create_key(
            catalog.ermrest_catalog,
            em.Key.define(['Foo2'], constraint_names=[(schema_name, 'TestTable_Foo2_key')])
        )

        t1.create_fkey(
            catalog.ermrest_catalog,
            em.ForeignKey.define(['Foo2'], schema_name, 'TestTable1', ['Foo2'],
                                 constraint_names=[[schema_name, 'TestTable1_Foo2_fkey']])
        )

    def test_visible_columns(self):
        table = catalog[schema_name][self.table_name]

    def test_column(self):
        table = catalog['public']['ERMrest_Client']
        self.assertEqual(table['RID'].name, 'RID')
        self.assertEqual(table.column('RID').name, 'RID')
        self.assertEqual(table.columns['RID'].name, 'RID')
        self.assertTrue( {'RID','RCB','RMB','RCT','RMT'} < {i.name for i in table.columns})
        table['RID'].dump()
        self.assertIsInstance(table['RID'].definition(), em.Column)

    def test_derivacolumn_create_delete(self):
        table = model.schemas[schema_name].create_table(catalog.ermrest_catalog, em.Table.define('TestTable', []))
        col = DerivaColumn(table, 'Foo','text')
        col.create()
        self.assertEqual(table['Foo'].name, 'Foo')
        col.delete()
        with self.assertRaises(DerivaCatalogError):
            table['Foo']

    def test_table_column_funcs(self):
        table = self.t1
        table.visible_columns.insert_context('*')
        table.create_columns(DerivaColumn(table, 'Foo', 'text'))
        assert (table['Foo'].name == 'Foo')
        print('Column added')
        table.visible_columns.dump()
        print('visible columns.')
        table.column('Foo').delete()

    def test_keys(self):
        table = self.t1
        table.visible_columns.insert_context('*')
        table.create_columns([DerivaColumn(table, 'Foo1', 'text'),
                              DerivaColumn(table, 'Foo2', 'text'),
                              DerivaColumn(table, 'Foo3', 'text')])

    def test_columns(self):
        pass

    def test_create_columns(self):
        pass

    def test_rename_column(self):
        pass

    def test_rename_columns(self):
        pass



