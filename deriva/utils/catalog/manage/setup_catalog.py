from attrdict import AttrDict
from deriva.core import ErmrestCatalog, get_credential, DerivaServer
from deriva.utils.catalog.manage.tests.test_derivaCSV import generate_test_csv
import deriva.core.ermrest_model as em
from deriva.utils.catalog.manage.deriva_csv import DerivaCSV
from deriva.utils.catalog.components.model_elements import create_asset_table, create_default_visible_columns

import configure_catalog
import csv

server = 'dev.isrd.isi.edu'
credentials = get_credential(server)
catalog_id = 55001

catalog = DerivaServer('https', server, credentials).create_ermrest_catalog()
catalog_id = catalog._catalog_id
#catalog = ErmrestCatalog('https',server, catalog_id, credentials=credentials)
print('Catalog_id is', catalog_id)

table_size = 10
column_count = 5
schema_name = 'TestSchema'
table_name = 'Foo'
csv_file = table_name + '.csv'

(row, headers) = generate_test_csv(column_count)
with open(csv_file, 'w', newline='') as f:
    tablewriter = csv.writer(f)
    for i, j in zip(range(table_size + 1), row):
        tablewriter.writerow(j)

model_root = catalog.getCatalogModel()
model_root.create_schema(catalog, em.Schema.define(schema_name))

csvtable = DerivaCSV(csv_file, schema_name, column_map=True, key_columns='id')
csvtable.create_validate_upload_csv(catalog, convert=True, create=True, upload=True)

model_root = catalog.getCatalogModel()
table= model_root.schemas[schema_name].tables[table_name]

configure_catalog.configure_baseline_catalog(catalog, catalog_name='test', admin='isrd-systems')
configure_catalog.configure_table_defaults(catalog, table, public=True)

create_asset_table(catalog, table, 'Id')
create_default_visible_columns(catalog,(schema_name, table_name))

# mkdir records/TestSchema
# mkdir assets/TestSchema/Foo/1
# cp test.txt into Foo/1
# generate CSV to put into Foo.csv


chaise_url = 'https://{}/chaise/recordset/#{}/{}:{}'.format(server, catalog_id,schema_name,table_name)
print(chaise_url)



