import random
import datetime
import string
import os
import csv

from deriva.core import get_credential, DerivaServer
import deriva.core.ermrest_model as em
from deriva.utils.catalog.manage.deriva_csv import DerivaCSV
from deriva.utils.catalog.components.configure_catalog import DerivaCatalogConfigure
from deriva.utils.catalog.components.model_elements import DerivaTable, \
    DerivaColumnDef, DerivaKey, DerivaForeignKey, DerivaVisibleSources, DerivaContext


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

# Create directories for testing upload spec.
def upload_test():
    os.makedirs('upload_test', exist_ok=True)
    os.chdir('upload_test')
    create_upload_dirs(schema_name, table_name, range(1, 3))

    for i in os.listdir('assets/{}/{}'.format(schema_name, table_name)):
        filename = 'assets/{}/{}/{}/{}'.format(schema_name, table_name, i, 'foo.txt')
        with open(filename, "w") as f:
            f.write("FOOBAR {}\n".format(i))


def create_upload_dirs(schema_name, table_name, iditer):
    os.makedirs('records/{}'.format(schema_name), exist_ok=True)
    for i in iditer:
        asset_dir = 'assets/{}/{}/{}'.format(schema_name, table_name, i)
        os.makedirs(asset_dir, exist_ok=True)
    return


table_size = 10
column_count = 5
schema_name = 'TestSchema'
table_name = 'Foo'
public_table_name = 'Foo_Public'

# Create test datasets
csv_file = table_name + '.csv'
csv_file_public = public_table_name + ".csv"

def load_csvs(catalog):
    (row, headers) = generate_test_csv(column_count)
    with open(csv_file, 'w', newline='') as f:
        tablewriter = csv.writer(f)
        for i, j in zip(range(table_size + 1), row):
            tablewriter.writerow(j)
    
    (row, headers) = generate_test_csv(column_count)
    with open(csv_file_public, 'w', newline='') as f:
        tablewriter = csv.writer(f)
        for i, j in zip(range(table_size + 1), row):
            tablewriter.writerow(j)

    # Upload CSVs into catalog, creating two new tables....
    csv_foo = DerivaCSV(csv_file, schema_name, column_map=['ID'], key_columns='id')
    csv_foo.create_validate_upload_csv(catalog, convert=True, create=True, upload=True)
    
    csv_foo_public = DerivaCSV(csv_file_public, schema_name, column_map=True, key_columns='id')
    csv_foo_public.create_validate_upload_csv(catalog, convert=True, create=True, upload=True)

    table = catalog.schema('TestSchema').table('Foo')
    table.configure_table_defaults(public=True)
    table.create_default_visible_columns(really=True)
    table_public = catalog.schema('TestSchema').table('Foo')
    table_public.configure_table_defaults(public=True)
    table_public.create_default_visible_columns(really=True)
    

# Create a test catalog
server = 'dev.isrd.isi.edu'
credentials = get_credential(server)
catalog_id = 55001

def create_test_catalog():
    new_catalog = DerivaServer('https', server, credentials).create_ermrest_catalog()
    catalog_id = new_catalog._catalog_id
    #new_catalog = ErmrestCatalog('https',host, catalog_id, credentials=credentials)
    print('Catalog_id is', catalog_id)
    catalog = DerivaCatalogConfigure(server, catalog_id=catalog_id)

    # Set up catalog into standard configuration
    catalog.configure_baseline_catalog(catalog_name='test', admin='isrd-systems')

    schema = catalog.create_schema(schema_name)
    return catalog   

# Mess with tables:

#print('Creating asset table')

def delete_tables(catalog, tlist):
    for i in tlist:
        try:
            catalog.schema('TestSchema').table(i).delete()
        except KeyError:
            pass
        
def delete_columns(table, tlist):
    for i in tlist:
        try:
            table.delete_columns([i])
        except KeyError:
            pass

tlist = ['Collection_Foo', 'Collection1_Foo', 'Collection_Foo_Public', 'Collection1_Foo_Public','Collection','Collection1','Collection_Status']
def create_collection(catalog):
    schema = catalog.schema('TestSchema')
    tlist = ['Collection_Foo', 'Collection1_Foo', 'Collection_Foo_Public', 'Collection1_Foo_Public', 'Collection',
             'Collection1', 'Collection_Status']
    delete_tables(catalog, tlist)
        
    test_schema = catalog.schema('TestSchema')
    print('Creating collection')
    collection = test_schema.create_table('Collection',
                             [em.Column.define('Name',
                                               em.builtin_types['text']),
                              em.Column.define('Description',
                                               em.builtin_types['markdown']),
                              em.Column.define('Status', em.builtin_types['text'])]
                             )
    collection.configure_table_defaults()
    collection.associate_tables(schema_name, table_name)
    collection.associate_tables(schema_name, public_table_name)
    collection.create_default_visible_columns(really=True)
    collection.create_default_visible_foreign_keys(really=True)
    
    collection_status = test_schema.create_vocabulary('Collection_Status', 'TESTCATALOG:{RID}')
    collection.link_vocabulary('Status', collection_status)
    
    print('Adding element to collection')
    collection.datapath().insert([{'Name': 'Foo', 'Description':'My collection'}])
    return collection

def move_collection(catalog):
    schema = catalog.schema('TestSchema')
    tlist = ['Collection1_Foo','Collection1_Foo_Public','Collection1']
    delete_tables(catalog, tlist)
    schema.table('Collection').move_table('TestSchema', 'Collection1')
    schema.table('Collection_Foo').move_table('TestSchema','Collection1_Foo')

def create_link():
    pass

# table.create_asset_table('ID')


def test_create_columns(catalog):
    table = catalog.schema('TestSchema').table('Foo')
    table.create_columns(
        [em.Column.define('TestCol', em.builtin_types['text']),
                        em.Column.define('TestCol1',em.builtin_types['text'])])
    table.create_columns(DerivaColumnDef(table, 'TestCol3', em.builtin_types['text']), positions={'*'})
    
def test_delete_columns(catalog):
    table = catalog.schema('TestSchema').table('Foo')
    table.delete_columns(['TestCol','TestCol1', 'TestCol3'])


def test_copy_columns(catalog):
    table = catalog.schema('TestSchema').table('Foo')
    delete_columns(table, ['Foobar','RCB1','ID1'])
    table.copy_columns({'Field_1':'Foobar', 'RCB':'RCB1'})
    
def test_copy_columns_between_tables(catalog):
    table = catalog.schema('TestSchema').table('Foo')
    table.copy_columns({'Field_1':'Foobar', 'RCB':'RCB1', 'ID':'ID1'})
    

def test_rename_columns(catalog):
    table = catalog.schema('TestSchema').table('Foo')
    delete_columns(table, ['Foobar', 'Foobar1', 'RCB1', 'RCB2','ID1'])
    table.copy_columns({'Field_1':'Foobar', 'RCB':'RCB1'})
    print('renaming columns....')
    table.rename_columns({'Foobar':'Foobar1', 'RCB1':'RCB2'})
    
    
def test_create_key(catalog):
    table = catalog.schema('TestSchema').table('Foo')
    table.create_columns([table.definition('FKey_Column', em.builtin_types['text'])])    
    table.create_key(em.Key.define(['Field_1','Field_2'], constraint_names=[(schema_name,'Foo_Field_1_Field_2')]))
    table.create_fkey(DerivaForeignKey(table, ))
    
    
def test_copy_table(catalog):
    try:
        catalog.schema('TestSchema').table('Foo1').delete()
    except KeyError:
        pass
    column_map = {'Field_1':'Field_1A', 'ID': {'name':'ID1', 'nullok':False, 'fill': 1}, 
                  'Status': {'type': 'int4', 'nullok': False, 'fill': 1}}
    table = catalog.schema('TestSchema').table('Foo')
    foo1 = table.copy_table('TestSchema','Foo1', column_map=column_map)
    return foo1


def test_move_table(catalog):
    try:
        catalog.schema('TestSchema').table('Foo1').delete()
    except KeyError:
        pass
    column_map = {'Field_1':'Field_1A', 'Foobar':'int4', 'ID': {'name':'ID1', 'nullok':False, 'fill': 1}, 
                  'Status': {'type': 'int4', 'nullok': False, 'fill': 1}}
        
    table = catalog.schema('TestSchema').table('Foo')
    column_defs = [DerivaColumnDef(table, 'Status', 'int4', nullok=False)]
    foo1 = table.move_table('TestSchema','Foo1', column_map=column_map, delete=False)
    return foo1


def test_tables():
    print('Renaming column')
    collection.rename_column('Status','MyStatus')
    print('Rename done')

    foo_table = DerivaTable(catalog, schema_name, "Foo")

    foo_table.delete_columns(['Field_3'])

    foo_table.move_table('WWW','Fun',
                        column_defs=[em.Column.define('NewColumn', em.builtin_types['text'], nullok=False)],
                         column_map={'ID':'NewID'}, column_fill={'NewColumn': 'hi there'}, delete_table=False)
    
    
def test_move_columns():
    pass