import unittest
from unittest import TestCase
import warnings
import logging

from deriva.core import get_credential, DerivaServer
import deriva.core.ermrest_model as em
from deriva.utils.catalog.components.deriva_model import *

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings('ignore', category=ResourceWarning)

server = 'dev.isrd.isi.edu'
catalog_id = 1
catalog = None
schema_name = 'TestSchema'
ermrest_catalog = None


def clean_schema(schema_name):
    with DerivaModel(catalog) as m:
        model = m.catalog_model()
        for t in model.schemas[schema_name].tables.values():
            for k in t.foreign_keys:
                k.delete(catalog.ermrest_catalog, t)
        for t in [i for i in model.schemas[schema_name].tables.values()]:
            t.delete(catalog.ermrest_catalog, model.schemas[schema_name])


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
    # catalog.ermrest_catalog.delete_ermrest_catalog(really=True)
    pass


class TestVisibleSources(TestCase):

    @classmethod
    def setUpClass(cls):
        global catalog
        clean_schema('TestSchema')

    def setUp(self):
        clean_schema(schema_name)
        with DerivaModel(catalog) as m:
            model = m.catalog_model()
            t1 = model.schemas[schema_name].create_table(catalog.ermrest_catalog, em.Table.define('TestTable1', []))
            t2 = model.schemas[schema_name].create_table(catalog.ermrest_catalog, em.Table.define('TestTable2', []))

            for i in ['Foo', 'Foo1', 'Foo2']:
                t1.create_column(ermrest_catalog, em.Column.define(i, em.builtin_types['text']))
                t2.create_column(ermrest_catalog, em.Column.define(i, em.builtin_types['text']))

            t2.create_key(
                ermrest_catalog,
                em.Key.define(['Foo2'], constraint_names=[(schema_name, 'TestTable1_Foo2_key')])
            )

            t1.create_fkey(
                ermrest_catalog,
                em.ForeignKey.define(['Foo2'], schema_name, 'TestTable2', ['Foo2'],
                                     constraint_names=[[schema_name, 'TestTable1_Foo2_fkey']])
            )

            main = model.schemas[schema_name].create_table(
                catalog.ermrest_catalog,
                em.Table.define('Main',
                                [em.Column.define('text_col', em.builtin_types['text']),
                                 em.Column.define('f1_fkey', em.builtin_types['text'])]))

            f1 = model.schemas[schema_name].create_table(
                catalog.ermrest_catalog,
                em.Table.define('F1', [em.Column.define('f1_text', em.builtin_types['text'])]))

            f2 = model.schemas[schema_name].create_table(
                catalog.ermrest_catalog,
                em.Table.define('F2', [em.Column.define('main_fkey', em.builtin_types['text'])]))

            main_f3 = model.schemas[schema_name].create_table(
                catalog.ermrest_catalog,
                em.Table.define('Main_F3',
                                [em.Column.define('main_fkey', em.builtin_types['text']),
                                 em.Column.define('f3_fkey', em.builtin_types['text'])]))

            f3 = model.schemas[schema_name].create_table(
                catalog.ermrest_catalog,
                em.Table.define('F3',
                                [em.Column.define('f3_text', em.builtin_types['text'])]
                                )
            )

            main.create_fkey(ermrest_catalog,
                             em.ForeignKey.define(['f1_fkey'], 'TestSchema', 'F1', ['RID'],
                                                  constraint_names=[('TestSchema', 'fk1_cons')]
                                                  ))

            f2.create_fkey(ermrest_catalog,
                             em.ForeignKey.define(['main_fkey'], 'TestSchema', 'Main', ['RID'],
                                                  constraint_names=[('TestSchema', 'fk2_cons')]
                                                  ))

            main_f3.create_fkey(ermrest_catalog,
                             em.ForeignKey.define(['main_fkey'], 'TestSchema', 'Main', ['RID'],
                                                  constraint_names=[('TestSchema', 'fk3_cons')]
                                                  ))
            main_f3.create_fkey(ermrest_catalog,
                             em.ForeignKey.define(['f3_fkey'], 'TestSchema', 'F3', ['RID'],
                                                  constraint_names=[('TestSchema', 'main_f3_cons')]
                                                  ))
        catalog.refresh()

    def test_source_spec(self):
        # Normal Columns
        main = catalog['TestSchema']['Main']
        self.assertEqual(DerivaSourceSpec(main, 'text_col').spec, {'source': 'text_col'})
        with self.assertRaises(DerivaSourceError):
            DerivaSourceSpec(main, 'Foobar')
        self.assertEqual(DerivaSourceSpec(main, {'source': 'text_col'}).spec, {'source': 'text_col'})
        self.assertEqual(DerivaSourceSpec(main, {'source': 'RID', 'entity': False}).spec, {'source': 'RID', 'entity': False})

        # Key Columns
        self.assertEqual(DerivaSourceSpec(main, ['TestSchema', 'Main_RIDkey1']).spec, {'source': 'RID'})
        with self.assertRaises(DerivaCatalogError):
            DerivaSourceSpec(main, ['TestSchema', 'Foobar'])

        # ForeignKey Columns
        self.assertEqual(DerivaSourceSpec(main, ['TestSchema', 'fk1_cons']).spec,
                         {"source": [{"outbound": ["TestSchema", "fk1_cons"]}, "RID"]})
        self.assertEqual(DerivaSourceSpec(main, {"source": [{"outbound": ["TestSchema", "fk1_cons"]}, "RID"]}).spec,
                         {"source": [{"outbound": ["TestSchema", "fk1_cons"]}, "RID"]})

        with self.assertRaises(DerivaCatalogError):
            DerivaSourceSpec(main, {"source": [{"outbound": ["TestSchema1", "fk1_cons"]}, "RID"]})
        with self.assertRaises(DerivaCatalogError):
            DerivaSourceSpec(main, {"source": [{"outbound": ["TestSchema", "fk1_cons1"]}, "RID"]})

        # Inbound foreignkey columns
        self.assertEqual(DerivaSourceSpec(main,["TestSchema", "fk2_cons"]).spec,
                                          {"source": [{"inbound": ["TestSchema", "fk2_cons"]}, "RID"]})
        self.assertEqual(DerivaSourceSpec(main, ["TestSchema", "fk3_cons"]).spec,
                                          {"source": [{"inbound": ["TestSchema", "fk3_cons"]}, "RID"]})
        self.assertEqual(DerivaSourceSpec(main,
                                          {"source": [{"inbound": ["TestSchema", "fk2_cons"]}, "RID"]}).spec,
                                          {"source": [{"inbound": ["TestSchema", "fk2_cons"]}, "RID"]})
        self.assertEqual(DerivaSourceSpec(main,
                                          {"source": [{"inbound": ["TestSchema", "fk3_cons"]},
                                                      {"outbound": ["TestSchema", "main_f3_cons"]}, "RID"]}).spec,
                         {"source": [{"inbound": ["TestSchema", "fk3_cons"]},
                                     {"outbound": ["TestSchema", "main_f3_cons"]}, "RID"]})

        self.assertEqual(
            DerivaSourceSpec(main, 'f1_fkey').make_outbound().spec,
            {"source": [{"outbound": ["TestSchema", "fk1_cons"]}, "RID"]}
        )
        self.assertEqual(
            DerivaSourceSpec(main, {"source": [{"outbound": ["TestSchema", "fk1_cons"]}, "RID"]}).make_column().spec,
            {"source": 'f1_fkey'}
        )



    def test_normalize_positions(self):
        DerivaVisibleSources._normalize_positions({'all'})

    def test_insert_sources(self):
        t1 = ermrest_catalog.getCatalogModel().schemas['TestSchema'].tables['TestTable1']
        t1.annotations[chaise_tags.visible_columns] = {}

        table = catalog['TestSchema']['TestTable1']
        vs = table.visible_columns
        vs.insert_context('*')
        vs.insert_sources([DerivaSourceSpec(table, 'Foo'), DerivaSourceSpec(table, 'Foo2')])
        self.assertIn({'source': 'Foo2'}, table.visible_columns['*'])


class TestDerivaTable(TestCase):

    def setUp(self):
        clean_schema(schema_name)

    def test_lookup_table(self):
        with DerivaModel(catalog) as m:
            model = m.catalog_model()
            t1 = model.schemas[schema_name].create_table(catalog.ermrest_catalog, em.Table.define('TestTable', []))
            table = catalog[schema_name].tables['TestTable']
            print(table, catalog[schema_name])
            self.assertEqual(table.name, 'TestTable')

    def test_create_table(self):
        table = catalog[schema_name].create_table('TestTable1', [], comment='My test table')
        self.assertEqual(table.name, 'TestTable1')
        self.assertEqual(table.comment, 'My test table')
        table.comment = "My new comment"
        self.assertEqual(table.comment, 'My new comment')
        table = catalog[schema_name].create_table('TestTable2', [DerivaColumn.define('Foo', type='text')])
        self.assertEqual(table.name, 'TestTable2')
        self.assertEqual(table.visible_columns['*'],
                         [{'source': 'RID'},
                          {'source': 'RCT'},
                          {'source': 'RMT'},
                          {'source': 'RCB'},
                          {'source': 'RMB'},
                          {'source': 'Foo'}])
        table.delete()
        with self.assertRaises(DerivaCatalogError):
            catalog[schema_name]['TestTable2']

    def test_column_access(self):
        table = catalog['public']['ERMrest_Client']
        self.assertEqual(table['RID'].name, 'RID')
        self.assertEqual(table.column('RID').name, 'RID')
        self.assertEqual(table.columns['RID'].name, 'RID')
        self.assertEqual(table.columns[2].name, 'RMT')
        self.assertTrue({'RID', 'RCB', 'RMB', 'RCT', 'RMT'} < {i.name for i in table.columns})
        print(table['RID'])
        self.assertIsInstance(table['RID'].definition(), em.Column)
        with self.assertRaises(DerivaCatalogError):
            catalog['public']['foobar']

    def test_column_add(self):
        table = catalog[schema_name].create_table('TestTable1', [])
        table.create_columns(DerivaColumn.define('Foo1', 'text'))
        self.assertIn('Foo1', table.columns)
        self.assertIn({'source': 'Foo1'}, table.visible_columns['*'])

    def test_deriva_column_delete(self):
        table = catalog[schema_name].create_table('TestTable', [DerivaColumn.define('Foo', 'text')])
        table.visible_columns.insert_context('*')
        self.assertEqual(table['RID'].name, 'RID')
        print(DerivaSourceSpec(table, 'Foo').spec)
        print(table.visible_columns)
        table['Foo'].delete()
        with self.assertRaises(DerivaCatalogError):
            table['Foo']
        self.assertNotIn({'source': 'Foo'}, table.visible_columns)
        self.assertNotIn({'source': 'Foo'}, table.visible_columns['*'])
        self.assertNotIn({'source': 'Foo'}, table.visible_columns['entry'])
        print(table.visible_columns)

    def test_keys(self):
        table = catalog[schema_name].create_table('TestTable1',
                                                  [DerivaColumn.define('Foo1', 'text'),
                                                   DerivaColumn.define('Foo2', 'text'),
                                                   DerivaColumn.define('Foo3', 'text')],
                                                  key_defs=[DerivaKey.define(['Foo1', 'Foo2'])])
        table.create_key(['Foo1'], comment='My Key')

        self.assertEqual(table.key('Foo1').name, 'TestTable1_Foo1_key')
        self.assertEqual([i.name for i in table.key('Foo1').columns], ['Foo1'])
        self.assertEqual(table.key(['Foo1', 'Foo2']).name, 'TestTable1_Foo1_Foo2_key')
        self.assertEqual({i.name for i in table.key(['Foo1', 'Foo2']).columns}, {'Foo1', 'Foo2'})
        self.assertEqual(table.key('TestTable1_Foo1_key').name, 'TestTable1_Foo1_key')
        self.assertEqual(table.keys['TestTable1_Foo1_key'].name, 'TestTable1_Foo1_key')
        self.assertIn('TestTable1_Foo1_key', table.keys)

        self.assertEqual(table.key('Foo1').columns['Foo1'].name, 'Foo1')
        self.assertEqual(table.key('Foo1').columns[6].name, 'Foo2')
        with self.assertRaises(DerivaCatalogError):
            table.create_key(['Foo1'], comment='My Key')
        with self.assertRaises(DerivaCatalogError):
            table.keys['TestTable1_Foo1']

        table.keys['Foo1'].delete()
        with self.assertRaises(DerivaCatalogError):
            table.keys['TestTable1_Foo1']
        table.create_key(['Foo1'], comment='My Key')
        self.assertIn('TestTable1_Foo1_key', table.keys)

        # Try to delete column in compound key
        with self.assertRaises(DerivaCatalogError):
            table.columns['Foo2'].delete()

    def test_fkeys(self):
        table1 = catalog[schema_name].create_table('TestTable1',
                                                   [DerivaColumn.define('Foo1a', 'text'),
                                                    DerivaColumn.define('Foo2a', 'text'),
                                                    DerivaColumn.define('Foo3a', 'text')],
                                                   key_defs=[DerivaKey.define(['Foo1a']),
                                                             DerivaKey.define(['Foo1a', 'Foo2a'])])

        table2 = catalog[schema_name].create_table('TestTable2',
                                                   [DerivaColumn.define('Foo1', 'text'),
                                                    DerivaColumn.define('Foo2', 'text'),
                                                    DerivaColumn.define('Foo3', 'text')],
                                                   key_defs=[DerivaKey.define(['Foo1'])],
                                                   fkey_defs=[DerivaForeignKey.define(['Foo1'], table1, ['Foo1a']),
                                                              DerivaForeignKey.define(['Foo1', 'Foo2'], table1,
                                                                                      ['Foo1a', 'Foo2a'])]
                                                   )
        logger.info('test table created')

        self.assertEqual(table2.foreign_key(['Foo1']).name, 'TestTable2_Foo1_fkey')
        self.assertEqual(table2.foreign_keys['Foo1'].name, 'TestTable2_Foo1_fkey')
        self.assertEqual(table2.foreign_keys['TestTable2_Foo1_fkey'].name, 'TestTable2_Foo1_fkey')

        self.assertEqual({i.name for i in table2.foreign_keys['TestTable2_Foo1_Foo2_fkey'].columns}, {'Foo1', 'Foo2'})
        self.assertEqual({i.name for i in table2.foreign_keys['TestTable2_Foo1_Foo2_fkey'].referenced_columns},
                         {'Foo1a', 'Foo2a'})
        self.assertEqual(table2.foreign_keys['TestTable2_Foo1_Foo2_fkey'].referenced_columns['Foo1a'].name, "Foo1a")

        self.assertTrue(table1.referenced_by['TestTable2_Foo1_fkey'])
        self.assertTrue(table1.referenced_by['Foo1a'])
        self.assertEqual(table1.referenced_by[['Foo1a']].name, 'TestTable2_Foo1_fkey')
        with self.assertRaises(DerivaCatalogError):
            table1.referenced_by['Foo1']

        with self.assertRaises(DerivaCatalogError):
            table2.foreign_keys['Bar']

        print(table2.visible_columns['*'])
        self.assertIn({'source': [{'outbound': ('TestSchema', 'TestTable2_Foo1_fkey')}, 'RID']},
                      table2.visible_columns['*'])

        self.assertIn({'source': [{'inbound': ('TestSchema', 'TestTable2_Foo1_fkey')}, 'RID']},
                      table1.visible_foreign_keys['*'])
        self.assertIn({'source': [{'inbound': ('TestSchema', 'TestTable2_Foo1_Foo2_fkey')}, 'RID']},
                      table1.visible_foreign_keys['*'])

    def test_fkey_add(self):
        table1 = catalog[schema_name].create_table('TestTable1',
                                                   [DerivaColumn.define('Foo1a', 'text'),
                                                    DerivaColumn.define('Foo2a', 'text'),
                                                    DerivaColumn.define('Foo3a', 'text')],
                                                   key_defs=[DerivaKey.define(['Foo1a', ]),
                                                             DerivaKey.define(['Foo1a', 'Foo2a'])])

        table2 = catalog[schema_name].create_table('TestTable2',
                                                   [DerivaColumn.define('Foo1', 'text'),
                                                    DerivaColumn.define('Foo2', 'text'),
                                                    DerivaColumn.define('Foo3', 'text')],
                                                   key_defs=[DerivaKey.define(['Foo1'])],
                                                   )
        table1.create_key(['Foo2a'])
        table2.create_key(['Foo2'])
        table2.create_foreign_key(['Foo1'], table1, ['Foo1a'])
        table2.create_foreign_key(['Foo1', 'Foo2'], table1, ['Foo1a', 'Foo2a'])

        self.assertEqual(table2.foreign_key(['Foo1']).name, 'TestTable2_Foo1_fkey')
        self.assertEqual(table2.foreign_keys['Foo1'].name, 'TestTable2_Foo1_fkey')
        self.assertEqual(table2.foreign_keys['TestTable2_Foo1_fkey'].name, 'TestTable2_Foo1_fkey')

        self.assertTrue(table1.referenced_by['TestTable2_Foo1_fkey'])
        self.assertTrue(table1.referenced_by['Foo1a'])
        self.assertEqual(table1.referenced_by[['Foo1a']].name, 'TestTable2_Foo1_fkey')
        with self.assertRaises(DerivaCatalogError):
            table1.referenced_by['Foo1']

        self.assertIn({'source': [{'outbound': ('TestSchema', 'TestTable2_Foo1_fkey')}, 'RID']},
                      table2.visible_columns['*'])

        self.assertIn({'source': [{'inbound': ('TestSchema', 'TestTable2_Foo1_fkey')}, 'RID']},
                      table1.visible_foreign_keys['*'])
        self.assertIn({'source': [{'inbound': ('TestSchema', 'TestTable2_Foo1_Foo2_fkey')}, 'RID']},
                      table1.visible_foreign_keys['*'])

        table2.foreign_keys['Foo1'].delete()
        with self.assertRaises(DerivaCatalogError):
            table2.columns['Foo1'].delete()

        with self.assertRaises(DerivaCatalogError):
            self.assertEqual(table2.foreign_key(['Foo1']).name, 'TestTable2_Foo1_fkey')

        # Column entry in visible_columns should have changed back to simple column reference.
        self.assertIn({'source': 'Foo1'}, table2.visible_columns['*'])
        with self.assertRaises(DerivaCatalogError):
            self.assertTrue(table1.referenced_by['Foo1a'])

        self.assertNotIn({'source': [{'outbound': ('TestSchema', 'TestTable2_Foo1_fkey')}, 'RID']},
                      table2.visible_columns['*'])

        self.assertNotIn({'source': [{'inbound': ('TestSchema', 'TestTable2_Foo1_fkey')}, 'RID']},
                      table1.visible_foreign_keys['*'])


    def test_copy_columns(catalog):
        table = catalog[schema_name].create_table('TestTable1', [DerivaColumn.define('Foo1a', 'text')])
        delete_columns(table, ['Foobar', 'RCB1', 'ID1'])
        table.copy_columns(
            {'Field_1': 'Foobar', 'RCB': 'RCB1', 'Bozo': 'int4', 'Bozo1': {'type': 'text', 'default': 23}})

    def test_copy_columns_between_tables(catalog):
        table = catalog.schema_model('TestSchema').table_model('Foo')
        table.copy_columns({'Field_1': {'name': 'Foobar', 'default': 23}, 'RCB': 'RCB1', 'ID': 'ID1', 'Bozo': 'text'})

    def test_rename_columns(catalog):
        table = catalog.schema_model('TestSchema').table_model('Foo')
        delete_columns(table, ['Foobar', 'Foobar1', 'RCB1', 'RCB2', 'ID1'])
        table.copy_columns({'Field_1': 'Foobar', 'RCB': 'RCB1'})
        print('renaming columns....')
        table.rename_columns({'Foobar': 'Foobar1', 'RCB1': 'RCB2'})

    def test_copy_table(catalog):
        try:
            catalog.schema_model('TestSchema').table_model('Foo1').delete()
        except KeyError:
            pass
        column_map = {'Field_1': 'Field_1A', 'ID': {'name': 'ID1', 'nullok': False, 'fill': 1},
                      'Status': {'type': 'int4', 'nullok': False, 'fill': 1}}
        table = catalog.schema_model('TestSchema').table_model('Foo')
        foo1 = table.copy_table('TestSchema', 'Foo1', column_map=column_map)
        return foo1

    def test_move_table(catalog):
        try:
            catalog.schema_model('TestSchema').table_model('Foo1').delete()
        except KeyError:
            pass
        column_map = {'Field_1': 'Field_1A', 'Foobar': 'int4', 'ID': {'name': 'ID1', 'nullok': False, 'fill': 1},
                      'Status': {'type': 'int4', 'nullok': False, 'fill': 1}}

        table = catalog.schema_model('TestSchema').table_model('Foo')
        column_defs = [DerivaColumnDef(table, 'Status', 'int4', nullok=False)]
        foo1 = table.move_table('TestSchema', 'Foo1', column_map=column_map, delete=False)
        return foo1

    def test_link_tables(self):
        table1 = catalog[schema_name].create_table('TestTable1', [DerivaColumn.define('Foo1a', 'text')])
        table2 = catalog[schema_name].create_table('TestTable2',[DerivaColumn.define('Foo1', 'text')])

        table1.link_tables(table2)

    def test_associate_tables(self):
        table1 = catalog[schema_name].create_table('TestTable1', [DerivaColumn.define('Foo1a', 'text')])
        table2 = catalog[schema_name].create_table('TestTable2',[DerivaColumn.define('Foo1', 'text')])
        table1.associate_tables(table2)
