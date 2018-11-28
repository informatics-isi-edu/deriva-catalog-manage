from __future__ import print_function
from __future__ import absolute_import
import os
import sys
import tempfile
import re
import datetime
import itertools
import argparse
from yapf.yapflib.yapf_api import FormatCode
from decimal import Decimal
from requests import HTTPError
from tableschema import Table, Schema, Field, exceptions
import goodtables
from attrdict import AttrDict

from deriva.core import ErmrestCatalog, get_credential
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.dump_catalog import \
    variable_to_str, tag_variables_to_str, annotations_to_str, table_def_to_str, load_module_from_path, yapf_style

groups = {}

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


def cannonical_deriva_name(name, map_columns = None):
    """
    A simple function that attempts to map a column name into a name that follows the deriva naming convetions.
    :param name: Column name to be mapped
    :return: Resulting column name.
    """
    if not map_columns:
        map_columns = []
    map_dict = {i.upper():i for i in map_columns}

    # TODO This can be made much more robust.  Right now handled mixed camel and snake case and parenthesis

    # Split words based on capitol first letter, or existing underscore.
    split_words = '[A-Z]+[a-z0-9]*|[a-z0-9]+|\(.*?\)|[+\/\-*@<>%&=]'
    word_list = re.findall(split_words, name)
    mname = '_'.join(list(map(lambda x: map_dict[x.upper()] if x.upper() in map_dict else x[0].upper() + x[1:], word_list)))

    return mname


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

        if field['type'] == 'boolean':
            field['trueValues'] =  [ "true", "True", "TRUE", "1", "T", "t" ]
            field['falseValues'] =  [ "false", "False", "FALSE", "0" , "F", "f"]

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


def table_annotations_to_str(table, variables=None):
    s = tag_variables_to_str({}, variables)
    s += annotations_to_str({}, 'table_annotations', variables=variables)
    s += variable_to_str('table_comment', None, variables)
    s += variable_to_str('table_acls', {}, variables)
    s += variable_to_str('table_acl_bindings', {}, variables)
    return s


def column_annotations_to_str(schema, variables=None):
    column_annotations = {}
    column_acls = {}
    column_acl_bindings = {}
    column_comment = {}
    for i in schema.fields:
        if 'description' in i.descriptor and not (i.descriptor['description'] == '' or not i.descriptor['description']):
            column_comment[i.name] = i.comment
    s = variable_to_str('column_annotations', column_annotations, variables)
    s += variable_to_str('column_comment', column_comment, variables)
    s += variable_to_str('column_acls', column_acls)
    s += variable_to_str('column_acl_bindings', column_acl_bindings, variables)
    return s


def foreign_key_defs_to_str(table_schema, variables=None):
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
    return s


def key_defs_to_str(table_schema, schema_name, table_name):
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
    return s


def column_defs_to_str(table_schema):
    """
    Print out a list of the deriva_py column definions, one for each field in the schema.
    :param table_schema: A table schema object
    :return:
    """

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
    return s


def table_to_str(server, catalog_id, table_schema, schema_name, table_name, variables=None):
    s = """import argparse
from attrdict import AttrDict
from deriva.core import ErmrestCatalog, get_credential, DerivaPathError
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage import update_catalog

table_name = '{table_name}'

schema_name = '{schema_name}'

{groups}

{column_annotations}

{column_defs}

{table_annotations}

{key_defs}

{fkey_defs}

{table_def}


def main(skip_args=False, mode='annotations', replace=False, server={server!r}, catalog_id={catalog_id}):

    if not skip_args:
        mode, replace, server, catalog_id = update_catalog.parse_args(server, catalog_id, is_table=True)
    update_catalog.update_table(mode, replace, server, catalog_id, schema_name, table_name, 
                                table_def, column_defs, key_defs, fkey_defs,
                                table_annotations, table_acls, table_acl_bindings, table_comment,
                                column_annotations, column_acls, column_acl_bindings, column_comment)


if __name__ == "__main__":
    main()""".format(server=server, catalog_id=catalog_id, table_name=table_name, schema_name=schema_name,
                         groups=variable_to_str('groups', groups),
                         column_annotations=column_annotations_to_str(table_schema),
                         column_defs=column_defs_to_str(table_schema),
                         table_annotations=table_annotations_to_str(table_schema),
                         key_defs=key_defs_to_str(table_schema, schema_name, table_name),
                         fkey_defs=foreign_key_defs_to_str(table_schema),
                         table_def=table_def_to_str(True))
    return s


def convert_table_to_deriva(table_loc, server, catalog_id, schema_name, table_name=None, outfile=None,
                            map_column_names=False, exact_match=False, key_columns=None, variables=None):
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

    if map_column_names:
        table_name = cannonical_deriva_name(table_name, map_column_names)
    if not outfile:
        outfile = table_name + '.py'

    # Here is the tricky bit: Inference is based on a sample and then a threshold.  Unfortunately, a missing value will
    # look like a text field, so if there are many missing values, this can skew the result.  There may also be
    # issues that arise from the sampling.  To try to balance these, we will get a large number of rows, but then
    # tolerate some "off" values.
    confidence = 1 if exact_match else .75


    # Figure out how many rows we have so we can make sure we look at the whole file when we do an infer.
    row_cnt = len(Table(table_loc).read())

    table = Table(table_loc, sample_size=row_cnt)
    table.infer(limit=row_cnt, confidence=confidence)

    column_types = {}
    for c in table.schema.fields:
        column_map[c.name] = cannonical_deriva_name(c.name, map_column_names) if map_column_names else c.name
        c.descriptor['name'] = column_map[c.name]
        column_types.setdefault(table_schema_ermrest_type_map[c.type + ':' + c.format],[]).append(c.name)

    if key_columns:
        if map_column_names:
            key_columns = [cannonical_deriva_name(c, map_column_names) for c in key_columns]
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
        table_string = table_to_str(server, catalog_id, table.schema, schema_name, table_name, variables)
        table_string = FormatCode(table_string, style_config=yapf_style)[0]
        print(table_string, file=stream)
    return column_map, column_types


def upload_table_to_deriva(tabledata, server, catalog_id, schema_name,
                           table_name=None, map_column_names=False,
                           chunk_size=1000, starting_chunk=1):
    """

    :param tabledata: Location of the source table. Can be file name or URL
    :param server: Server on which the catalog exists.
    :param catalog_id: Catalog ID of target catalog
    :param schema_name: Schema into which the table will be uploaded
    :param table_name: Name of table to upload. If not provided, the filename of the CSV is used for the table name
    :param chunk_size: Number of rows to upload at one time.
    :param starting_chunk: What chunk number to start at.  Can be used to continue a failed upload.
    :return:
    """

    # Convert date time to string so we can push it out in JSON....
    def date_to_text(erows):
        for row_number, headers, row in erows:
            row = [str(x) if type(x) is datetime.date or type(x) is Decimal else x for x in row]
            yield (row_number, headers, row)

    # Helper function to do chunking...
    def row_grouper(n, iterable):
            iterable = iter(iterable)
            return iter(lambda: list(itertools.islice(iterable, n)), [])

    # If tablename is not specified, use the file name of the data file as the table name.
    if not table_name:
        table_name = os.path.splitext(os.path.basename(tabledata))[0]
        table_name = cannonical_deriva_name(table_name, map_column_names)

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


def validate_csv(table, server, catalog_id, schema_name, table_name=None, validation_limit=None, map_column_names=None):
    """
    For the specified table data, validate the contents of the table against an existing table in a catalog.
    :param table:  file name or URL from where to obtain table data
    :param server:  server where the catalog is
    :param catalog_id:  catalog id
    :param schema_name: schema name where the table is
    :param table_name: table to validate against.  Uses the file name of the data if not specified.
    :param validation_limit: How much of the table to check. Defaults to entire table.
    :return: an error report and the number of rows in the table as a tuple
    """

    if not table_name:
        table_name = os.path.splitext(os.path.basename(table))[0]

    if not validation_limit:
        # Validate the whole file.  In this case, we need to read the file to figure out how many rows there are.
        validation_limit = len(Table(table).read())

    table_schema = table_schema_from_catalog(server, catalog_id, schema_name, table_name)

    # First, just check the headers to make sure they line up under mapping.
    report = goodtables.validate(table, schema=table_schema.descriptor, checks=['non-matching-header'])
    if not report['valid'] and map_column_names:
        mapped_headers = map(lambda x: cannonical_deriva_name(x, map_column_names), report['tables'][0]['headers'])
        bad_headers = list(filter(lambda x: x[0] != x[1], zip(table_schema.field_names, mapped_headers)))
        if bad_headers:
            report['headers'] = [ x[1] for x in bad_headers ]
            return report['valid'], report, validation_limit
    report = goodtables.validate(table, row_limit=validation_limit, schema=table_schema.descriptor,
                                 skip_checks=['non-matching-header'])

    return report['valid'], report,  validation_limit


def create_validate_upload_csv(tabledata, server, catalog_id, schema_name,
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

    # If tablename is not specified, use the file name of the data file as the table name.
    if not table_name:
        table_name = os.path.splitext(os.path.basename(tabledata))[0]
        table_name = cannonical_deriva_name(table_name, map_column_names)

    if create_table or convert_table:
        print('Creating table definition {}:{}'.format(schema_name, table_name))
        with tempfile.TemporaryDirectory() as tdir:
            # If convertdir is set, put deriva-py program in current directory.
            odir = os.path.dirname(os.path.abspath(tabledata)) if convert_table else tdir
            if (not derivafile) or convert_table:
                # Create variable substitutions.
                variables = {k: v for k, v in groups.items()}
                variables.update(chaise_tags)

                derivafile = '{}/{}.py'.format(odir, table_name)
                convert_table_to_deriva(tabledata, server, catalog_id, schema_name, table_name=table_name,
                                        key_columns=key_columns, map_column_names=map_column_names,
                                        outfile=derivafile)
            if create_table:
                tablescript = load_module_from_path(derivafile)
                # Now create the table.
                tablescript.main(skip_args=True, mode='table')

    if validate:
        report, row_cnt = validate_csv(tabledata, server, catalog_id,
                                       schema_name, table_name=table_name, map_column_names=map_column_names)
        if not report['valid']:
            for i in report['tables'][0]['errors']:
                print(i)
            return 0, 1, report

    if load_data:
        print("Loading table....")
        row_cnt, chunk_size, chunk_cnt = \
            upload_table_to_deriva(tabledata, server, catalog_id, schema_name, table_name=table_name,
                                   chunk_size=chunk_size, starting_chunk=starting_chunk, map_column_names=map_column_names)

        return row_cnt, chunk_size, chunk_cnt


def main():
    global groups
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
    parser.add_argument('--config', default=None, help='python script to set up configuration variables)')

    args = parser.parse_args()

    if args.config:
        config = load_module_from_path(args.config)
        groups = AttrDict(config.groups)

    create_validate_upload_csv(args.tabledata, args.server, args.catalog, args.schema,
                               key_columns=args.key_columns, table_name=args.table,
                               convert_table=args.convert, derivafile=args.derivafile, map_column_names=args.map_column_names,
                               create_table=args.create_table, validate=not args.skip_validate,
                               load_data=not args.skip_upload, chunk_size=args.chunk_size, starting_chunk=args.starting_chunk)
    return


if __name__ == "__main__":
    main()
