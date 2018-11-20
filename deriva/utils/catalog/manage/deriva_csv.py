from __future__ import print_function
import os
import sys
import tempfile
import autopep8
import re
import datetime
import itertools
import argparse
from requests import HTTPError

if sys.version_info > (3, 5):
    import importlib.util
elif sys.version_info > (3, 3):
    from importlib.machinery import SourceFileLoader
else:
    import imp


from tableschema import Table, Schema, Field, exceptions
import goodtables

from deriva.core import ErmrestCatalog, get_credential
from deriva.utils.catalog.manage.dump_catalog import \
    print_variable, print_tag_variables, print_annotations, print_table_def, tag_map

# We should get range info in there....
table_schema_type_map = {
    'timestamp': ('timestamp', 'default'),
    'jsonb': ('object', 'default'),
    'float4': ('number', 'default'),
    'int4': ('integer', 'default'),
    'int8': ('integer', 'default'),
    'float8': ('number', 'default'),
    'text': ('string', 'default'),
    'markdown': ('string', 'default'),
    'date': ('date', 'default'),
    'json': ('object', 'default'),
    'boolean': ('boolean', 'default'),
    'int2': ('integer', 'default'),
    'timestamptz': ('datetime', 'default'),
    'timestamp[]': ('any', 'default'),
    'jsonb[]': ('array', 'default'),
    'float4[]': ('any', 'default'),
    'int4[]': ('integer', 'default'),
    'int8[]': ('integer', "default"),
    'float8[]': ('number', 'default'),
    'text[]': ('any', 'default'),
    'date[]': ('any', 'default'),
    'json[]': ('array', 'default'),
    'boolean[]': ('boolean', 'default'),
    'int2[]': ('integer', 'default'),
    'timestamptz[]': ('any', 'default'),
    'ermrest_uri': ('string', 'uri'),
    'ermrest_rid': ('string', 'default'),
    'ermrest_rct': ('datetime', 'default'),
    'ermrest_rmt': ('datetime', 'default'),
    'ermrest_rcb': ('string', 'default'),
    'ermrest_rmb': ('string', 'default'),
}

table_schema_ermrest_type_map = {
    'string:default': 'text',
    'string:email': 'text',
    'string:uri': 'ermrest_uri',
    'string:binary': 'text',
    'string:uuid': 'text',
    'number:default': 'float8',
    'integer:default': 'int4',
    'boolean:default': 'boolean',
    'object:default': 'json',
    'array:default': 'json[]',
    'date:default': 'date',
    'date:any': 'date',
    'time:default': 'timestampz',
    'time:any': 'timestampz',
    'datetime:default': 'date',
    'datetime:any': 'date',
    'year:default': 'date',
    'yearmonth:default': 'date'
}


class DerivaCSVError(HTTPError):
    def __init__(self, chunk_size, chunk_number, http_err):
        self.chunk_number = chunk_number
        self.chunk_size = chunk_size
        self.reason = http_err


def cannonical_deriva_name(name):
    exclude_list = ['nM']
    split_words = '[A-Z]+[a-z0-9]*|[a-z0-9]+|\(.*?\)|[+\/\-*@<>%&=]'
    return '_'.join(list(map(lambda x: x if x in exclude_list else x[0].upper() + x[1:], re.findall(split_words, name))))


def table_schema_from_catalog(server, catalog_id, schema_name, table_name, skip_system_columns=True, outfile=None):
    """
    Create a TableSchema by querying an ERMRest catalog and converting the model format.
    :param server: Server on which the catalog resides
    :param catalog_id: Catalog ID to use for the model
    :param schema_name: Schema from which to get the table
    :param table_name: Table whose model you want to convert
    :param outfile: if this argument is specified, dump the scheme into the specified file.
    :return: table schema representation of the model
    """
    credential = get_credential(server)
    catalog = ErmrestCatalog('https', server, catalog_id, credentials=credential)
    model_root = catalog.getCatalogModel()
    schema = model_root.schemas[schema_name]
    table = schema.tables[table_name]
    fields = []
    primary_key = None
    for col in table.column_definitions:
        if col.name in ['RID', 'RCB', 'RMB', 'RCT', 'RMT'] and skip_system_columns:
            continue
        field = {
            "name": col.name,
            "type": table_schema_type_map[col.type.typename][0],
            "constraints": {}
        }
        if table_schema_type_map[col.type.typename][1] != 'default':
            field['format'] = table_schema_type_map[col.type.typename][1]
        if col.display:
            field['title'] = col.display['name']
        if col.comment:
            field['description'] = col.comment
        # Now see if column is unique.  For this to be true, it must be in the list of keys for the table, and
        #  the unique column list must be a singleton.
        if [col.name] in [i.unique_columns for i in table.keys]:
            field['constraints']['unique'] = True
        if not col.nullok:
            field['constraints']['required'] = True
            # See if there is a primary key value aside from the RID column
        if field['constraints'].get('unique', False) and field['constraints'].get('required', False):
            primary_key = [col.name]
        fields.append(field)

    try:
        table_schema = Schema({'fields': fields, 'missingValues': ['', 'N/A', 'NULL']}, strict=True)
        if primary_key:
            table_schema.descriptor['primaryKey'] = primary_key
        table_schema.commit(strict=True)
        if outfile:
            table_schema.save(outfile)
    except exceptions.ValidationError as exception:
        print('error.....')
        print(exception.errors)
    return table_schema


def print_table_annotations(table, stream):
    print_tag_variables({}, tag_map, stream)
    print_annotations({}, tag_map, stream, var_name='table_annotations')
    print_variable('table_comment', None, stream)
    print_variable('table_acls', {}, stream)
    print_variable('table_acl_bindings', {}, stream)


def print_column_annotations(schema, stream):
    column_annotations = {}
    column_acls = {}
    column_acl_bindings = {}
    column_comment = {}

    for i in schema.fields:
        if 'description' in i.descriptor and not (i.descriptor['description'] == '' or not i.descriptor['description']):
            column_comment[i.name] = i.comment

    print_variable('column_annotations', column_annotations, stream)
    print_variable('column_comment', column_comment, stream)
    print_variable('column_acls', column_acls, stream)
    print_variable('column_acl_bindings', column_acl_bindings, stream)
    return


def print_foreign_key_defs(table_schema, stream):
    s = 'fkey_defs = [\n'
    for fkey in table_schema.foreign_keys:
        s += """    em.ForeignKey.define({},
            '{}', '{}', {},
            constraint_names={},\n""".format([c['column_name'] for c in fkey.foreign_key_columns],
                                             fkey.referenced_columns[0]['schema_name'],
                                             fkey.referenced_columns[0]['table_name'],
                                             [c['column_name'] for c in fkey.referenced_columns],
                                             fkey.names)

        for i in ['annotations', 'acls', 'acl_bindings', 'on_update', 'on_delete', 'comment']:
            a = getattr(fkey, i)
            if not (a == {} or a is None or a == 'NO ACTION' or a == ''):
                v = "'" + a + "'" if re.match('comment|on_update|on_delete', i) else a
                s += "        {}={},\n".format(i, v)
        s += '    ),\n'

    s += ']'
    print(autopep8.fix_code(s, options={}), file=stream)


def print_key_defs(table_schema, schema_name, table_name, stream):
    s = 'key_defs = [\n'
    constraint_name = (schema_name, cannonical_deriva_name('{}_{}_Key)'.format(table_name, 'RID')))
    s += """    em.Key.define({},
                 constraint_names=[{!r}],\n),\n""".format(['RID'], constraint_name)

    if len(table_schema.primary_key) > 1:
        constraint_name = \
                (schema_name, cannonical_deriva_name('{}_{}_Key)'.format(table_name, '_'.join(table_schema.primary_key))))
        s += """    em.Key.define({},
                     constraint_names=[{!r}],\n),\n""".format(table_schema.primary_key, constraint_name)

    for col in table_schema.fields:
        if col.constraints.get('unique', False):
            constraint_name = (schema_name, cannonical_deriva_name('{}_{}_Key)'.format(table_name, col.name)))
            s += """    em.Key.define([{!r}],
                     constraint_names=[{!r}],\n""".format(col.name, constraint_name)
            s += '),\n'
    s += ']'
    print(autopep8.fix_code(s, options={}), file=stream)
    return


def print_column_defs(table_schema, stream):
    """
    Print out a list of the deriva_py column definions, one for each field in the schema.
    :param table_schema: A table schema object
    :param stream: Output file
    :return:
    """
    provide_system = True
    system_columns = ['RID', 'RCB', 'RMB', 'RCT', 'RMT']

    s = 'column_defs = ['
    for col in table_schema.fields:
        # Don't include system columns in the list of column definitions.
        if col.name in system_columns:
            continue
        t = "{}:{}".format(col.type, col.format)
        s += "    em.Column.define('{}', em.builtin_types['{}'],".format(col.name, table_schema_ermrest_type_map[t])
        s += "nullok={},".format(not col.required)
        try:
            s += "comment=column_comment['{}'],".format(col.descriptor['description'])
        except KeyError:
            pass
        s += '),\n'
    s += ']'
    print(autopep8.fix_code(s, options={'aggressive': 8}), file=stream)
    return provide_system


def print_table(server, catalog_id, table_schema, schema_name, table_name, stream):
    print("""import argparse
from deriva.core import ErmrestCatalog, get_credential, DerivaPathError
from deriva.utils.catalog.manage import update_catalog

import deriva.core.ermrest_model as em

table_name = '{}'
schema_name = '{}'
""".format(table_name, schema_name), file=stream)

    print_column_annotations(table_schema, stream)
    provide_system = print_column_defs(table_schema, stream)
    print_table_annotations(table_schema, stream)
    print_key_defs(table_schema, schema_name, table_name, stream)
    print_foreign_key_defs(table_schema, stream)
    print_table_def(provide_system, stream)
    print('''
def main(skip_args=False, mode='annotations', replace=False, server={0!r}, catalog_id={1}):
    
    if not skip_args:
        mode, replace, server, catalog_id = update_catalog.parse_args(server, catalog_id, is_table=True)
    update_catalog.update_table(mode, replace, server, catalog_id, schema_name, table_name, 
                                table_def, column_defs, key_defs, fkey_defs,
                                table_annotations, table_acls, table_acl_bindings, table_comment,
                                column_annotations, column_acls, column_acl_bindings, column_comment)


if __name__ == "__main__":
    main()'''.format(server, catalog_id), file=stream)
    return


def convert_table_to_deriva(table_loc, server, catalog_id, schema_name, table_name=None, outfile=None,
                            map_column_names=False, exact_match=False, key_columns=None):
    """
    Read in a table, try to figure out the type of its columns and output a deriva-py program that can be used to create
    the table in a catalog.

    :param table_loc: Path or URL to the table
    :param server: Default server in which the table should be created.
    :param catalog_id: Default catalog_id into which the table should be created
    :param schema_name: Schema to be used for the table definition.
    :param table_name: Table name to be used.  Will default to file name.
    :param outfile: Where to put the deriva_py program.
    :param map_column_names:
    :param key_columns: a list of columns that will make up the primary key.
    :return: dictionary that has the column name mapping derived by this routine.
    """
    column_map = {}

    if not table_name:
        table_name = os.path.splitext(os.path.basename(table_loc))[0]
        table_name = cannonical_deriva_name(table_name)
    if not outfile:
        outfile = table_name + '.py'

    # Here is the tricky bit: Inference is based on a sample and then a threshold.  Unfortunatley, a missing value will
    # look like a text field, so if there are many missing values, this can skew the result.  There may also be
    # issues that arrise from the sampling.  To try to balance these, we will get a large number of rows, but then
    # tolerate some "off" values.
    confidence=1 if exact_match else .75

    table = Table(table_loc, sample_size=10000)
    table.infer(limit=10000, confidence=confidence)
    print(table.schema.descriptor)
    for c in table.schema.fields:
        column_map[c.name] = cannonical_deriva_name(c.name) if map_column_names else c.name
        c.descriptor['name'] = column_map[c.name]

    if key_columns:
        if map_column_names:
            key_columns = [cannonical_deriva_name(c) for c in key_columns]
        if not type(key_columns) is list:
            key_columns = [key_columns]
        # Set the primary key value
        for i in key_columns:
            if i not in column_map:
                print('Missing key column')
        table.schema.descriptor['primaryKey'] = key_columns
        for i, col in enumerate(table.schema.fields):
            if col.name in key_columns:
                table.schema.descriptor['fields'][i]['constraints'] = {'required': True, 'unique': True}
    table.schema.commit()

    with open(outfile, 'w') as stream:
        print_table(server, catalog_id, table.schema, schema_name, table_name, stream)
    return column_map


def upload_table_to_deriva(tabledata, server, catalog_id, schema_name,
                           key_columns=None, table_name=None,
                           convert_table=True, derivafile=None, map_column_names=False,
                           create_table=False, validate=True, load_data=True,
                           chunk_size=1000, starting_chunk=1, validation_rows=10000):
    """

    :param tabledata: Location of the source table. Can be file name or URL
    :param server: Server on which the catalog exists.
    :param catalog_id: Catalog ID of target catalog
    :param schema_name: Schema into which the table will be uploaded
    :param key_columns: list of columns that form a primary key in the source table (non-null and unique)
    :param table_name: Name of table to upload. If not provided, the filename of the CSV is used for the table name
    :param convert_table: If set to true, use table inference to infer types for columns of table and create a deriva-py program
    :param derivafile: Specify the file name of where the deriva-py program to create the table exisits
    :param create_table: If true, create a table in the catalog.  If derivafile argument is not specified, then infer table definition
    :param validate: Run table validation on input before trying to upload
    :param load_data:
    :param chunk_size: Number of rows to upload at one time.
    :param starting_chunk: What chunk number to start at.  Can be used to continue a failed upload.
    :return:
    """

    # Convert date time to string so we can push it out in JSON....
    def date_to_text(erows):
        for row_number, headers, row in erows:
            row = [str(x) if type(x) is datetime.date else x for x in row]
            yield (row_number, headers, row)

    # Helper function to do chunking...
    def row_grouper(n, iterable):
            iterable = iter(iterable)
            return iter(lambda: list(itertools.islice(iterable, n)), [])

    # If tablename is not specified, use the file name of the data file as the table name.
    if not table_name:
        table_name = os.path.splitext(os.path.basename(tabledata))[0]
        table_name = cannonical_deriva_name(table_name)

    if create_table or convert_table:
        print('Creating table definition {}:{}'.format(schema_name, table_name))
        with tempfile.TemporaryDirectory() as tdir:
            # If convertdir is set, put deriva-py program in current directory.
            odir = os.path.dirname(os.path.abspath(tabledata)) if convert_table else tdir
            if (not derivafile) or convert_table:
                derivafile = '{}/{}.py'.format(odir, table_name)
                convert_table_to_deriva(tabledata, server, catalog_id, schema_name, table_name=table_name,
                                        key_columns=key_columns, map_column_names=map_column_names,
                                        outfile=derivafile)
            if create_table:
                # Now create the table.
                if sys.version_info > (3, 5):
                    modspec = importlib.util.spec_from_file_location("table_name", derivafile)
                    tablescript = importlib.util.module_from_spec(modspec)
                    modspec.loader.exec_module(tablescript)
                elif sys.version_info > (3, 3):
                    tablescript = SourceFileLoader("table_name", derivafile)
                else:
                    tablescript = imp.load_source("table_name", derivafile)

                tablescript.main(skip_args=True, mode='table')

    if validate:
        table_schema = table_schema_from_catalog(server, catalog_id, schema_name, table_name)
        report = goodtables.validate(tabledata, row_limit=validation_rows, schema=table_schema.descriptor)
        if not report['valid']:
            for i in report['tables'][0]['errors']:
                print(i)
            return 0, 1, report

    if load_data:
        print("Loading table....")
        credential = get_credential(server)
        catalog = ErmrestCatalog('https', server, catalog_id, credentials=credential)
        pb = catalog.getPathBuilder()
        table_schema = table_schema_from_catalog(server, catalog_id, schema_name, table_name)
        source_table = Table(tabledata, schema=table_schema.descriptor, post_cast=[date_to_text])
        target_table = pb.schemas[schema_name].tables[table_name]

        if table_schema.primary_key == []:
            row_groups = [source_table.read(keyed=True)]
            chunk_size = len(row_groups[0])
        else:
            row_groups = row_grouper(chunk_size, source_table.iter(keyed=True))
        chunk_cnt = 1
        row_cnt = 0
        for rows in row_groups:
            if chunk_cnt < starting_chunk:
                chunk_cnt += 1
                continue
            try:
                target_table.insert(rows, add_system_defaults=True)
                print('Completed chunk {}'.format(chunk_cnt))
                chunk_cnt += 1
                row_cnt += len(rows)
            except HTTPError as e:
                print('Failed on chunk {} (chunksize {})'.format(chunk_cnt, chunk_size))
                print(e)
                print(e.response.text)
                raise DerivaCSVError(chunk_size, chunk_cnt, e)
        return row_cnt, chunk_size, chunk_cnt


def main():
    # Argument parser
    parser = argparse.ArgumentParser(description="Load CSV and other table formats into deriva catalog")

    parser.add_argument('tabledata', help='Location of tablelike date to be added to catalog')
    parser.add_argument('server', help='Catalog server name')
    parser.add_argument('--catalog', default=1, help='ID number of desired catalog')
    parser.add_argument('schema', help='Name of the schema to be used for table')
    parser.add_argument('--table', default=None, help='Name of table to be managed (Default:Filename)')
    parser.add_argument('--key_columns', default=[],
                        help='List of columns to be used as key when creating table schema')
    parser.add_argument('--convert', action='store_true', help='Generate a deriva-py program to create the table [Default:False]')
    parser.add_argument('--map_column_names', action='store_true',
                        help='Automatically convert column names to cannonical form [Default:True]')
    parser.add_argument('--derivafile', default=None, help='Filename for output deriva-py program')

    parser.add_argument('--chunk_size', default=10000, help='Number of rows to use in chunked upload [Default:10000]')
    parser.add_argument('--starting_chunk', default=1, help='Starting chunk number [Default:1]')
    parser.add_argument('--skip_validate', action='store_true', help='Validate the table before uploading [Default:True]')
    parser.add_argument('--create_table', action='store_true',
                        help='Automatically create catalog table based on column type inference [Default:False]')
    parser.add_argument('--skip_upload', action='store_true', help='Load data into catalog [Default:True]')

    args = parser.parse_args()
    print(args)

    upload_table_to_deriva(args.tabledata, args.server, args.catalog, args.schema,
                           key_columns=args.key_columns, table_name=args.table,
                           convert_table=args.convert, derivafile=args.derivafile, map_column_names=args.map_column_names,
                           create_table=args.create_table, validate= not args.skip_validate, load_data= not args.skip_upload,
                           chunk_size=args.chunk_size, starting_chunk=args.starting_chunk)
    return


if __name__ == "__main__":
    main()
