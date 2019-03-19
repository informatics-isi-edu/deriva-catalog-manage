from attrdict import AttrDict
import random
import datetime
import string
import os
from deriva.core import ErmrestCatalog, get_credential, DerivaServer
import deriva.core.ermrest_model as em
from deriva.utils.catalog.manage.deriva_csv import DerivaCSV
from deriva.utils.catalog.manage.configure_catalog import DerivaCatalogConfigure, DerivaTableConfigure
import deriva.utils.catalog.components.model_elements as model_elements
from deriva.core.ermrest_config import tag as chaise_tags
import configure_catalog
from deriva.utils.catalog.components.model_elements import DerivaCatalog, DerivaSchema, DerivaTable
import csv

server = 'dev.isrd.isi.edu'
credentials = get_credential(server)
catalog_id = 55001

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

# Create a test catalog

new_catalog = DerivaServer('https', server, credentials).create_ermrest_catalog()
catalog_id = new_catalog._catalog_id
#new_catalog = ErmrestCatalog('https',host, catalog_id, credentials=credentials)
print('Catalog_id is', catalog_id)

# Create a new schema and upload the CSVs into it.
catalog = DerivaCatalogConfigure(server, catalog_id=catalog_id)

# Set up catalog into standard configuration
catalog.configure_baseline_catalog(catalog_name='test', admin='isrd-systems')

schema = catalog.create_schema(schema_name)
test_schema = schema

# Upload CSVs into catalog, creating two new tables....
csvtable = DerivaCSV(csv_file, schema_name, column_map=['ID'], key_columns='id')
csvtable.create_validate_upload_csv(catalog, convert=True, create=True, upload=True)

csvtable_public = DerivaCSV(csv_file_public, schema_name, column_map=True, key_columns='id')
csvtable_public.create_validate_upload_csv(catalog, convert=True, create=True, upload=True)

# Now get the two tables we just created from the CSVs.  Do this two different ways, just for fun.
table = catalog.schema(schema_name).table(table_name)
table_public = catalog.schema(schema_name).table(public_table_name)

table.configure_table_defaults(public=True)
table.create_default_visible_columns(really=True)

table_public.configure_table_defaults(public=True)
table_public.create_default_visible_columns(really=True)

# Mess with tables:
table.set_annotation(chaise_tags.visible_columns,{'detailed':['RMT']})

print('Creating asset table')
table.create_asset_table('ID')

print('Creating collection')
collection = test_schema.create_table('Collection',
                         [em.Column.define('Name',
                                           em.builtin_types['text']),
                          em.Column.define('Description',
                                           em.builtin_types['markdown']),
                          em.Column.define('Status', em.builtin_types['text'])]
                         )
collection.configure_table_defaults()
collection.create_default_visible_columns(really=True)

collection.associate_tables(schema_name, table_name)
collection.associate_tables(schema_name, public_table_name)

test_schema.create_vocabulary('Collection_Status', 'TESTCATALOG:{RID}')
collection.link_vocabulary('Status', schema_name, 'Collection_Status')

print('Adding element to collection')
collection.datapath().insert([{'Name': 'Foo', 'Description':'My collection'}])

print('Renaming column')
collection.rename_column('Status','MyStatus')
print('Rename done')
collection.apply()

print('Apply done')


chaise_url = 'https://{}/chaise/recordset/#{}/{}:{}'.format(server, catalog_id, schema_name,public_table_name)
group_url = 'https://{}/chaise/recordset/#{}/{}:{}'.format(server, catalog_id, 'public','Catalog_Group')

print(chaise_url)

foo_table = DerivaTable(catalog, schema_name, "Foo")
foo_table.delete_columns(['Field_1'])
foo_table.move_table('WWW','Fun',
                    column_defs=[em.Column.define('NewColumn', em.builtin_types['text'])],
                     column_map={'ID':'NewID'})