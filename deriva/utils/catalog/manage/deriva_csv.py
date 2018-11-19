from __future__ import print_function
import os
import tempfile
import autopep8
import re
import datetime
import importlib.util

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


def cannonical_deriva_name(name):
    exclude_list = ['nM']
    split_words = '[A-Z]+[a-z0-9]*|[a-z0-9]+|\(.*?\)'
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
        table_schema = Schema({'fields': fields, 'missingValues' : ['', 'N/A', 'NULL']}, strict=True)
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
                            map_column_names=False, key_columns=None):
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

    # We want the inference to be conservative, so we set base decsision on all of the rows in the table.
    table = Table(table_loc, sample_size=10000)
    table.infer(limit=10000, confidence=1)
    for c in table.schema.fields:
        column_map[c.name] = cannonical_deriva_name(c.name) if map_column_names else c.name
        c.descriptor['name'] = column_map[c.name]

    if key_columns:
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


def upload_table_to_deriva(table_loc, server, catalog_id, schema_name,
                           key_columns=None, table_name=None, create_table=False, validate=True):
    """

    :param table_loc: Location of the source table. Can be file name or URL
    :param server: Server on which the catalog exists.
    :param catalog_id: Catalog ID of target catalog
    :param schema_name: Schema into which the table will be uploaded
    :param key_columns: list of columns that form a primary key in the source table (non-null and unique)
    :param table_name: Name of table to upload. If not provided, the filename of the CSV is used for the table name
    :param create_table: If true, then infer the types of the table columns and create a table in the catalog
    :param validate: Run table validation on input before trying to upload
    :return:
    """

    # Convert date time to string so we can push it out in JSON....
    def date_to_text(erows):
        for row_number, headers, row in erows:
            row = [str(x) if type(x) is datetime.date else x for x in row]
            yield (row_number, headers, row)

    if not table_name:
        table_name = os.path.splitext(os.path.basename(table_loc))[0]

    if create_table:
        print('Creating table definition {}:{}'.format(schema_name, table_name))
        with tempfile.TemporaryDirectory() as tdir:
            fname = '{}/{}.py'.format(tdir, table_name)
            convert_table_to_deriva(table_loc, server, catalog_id, schema_name,
                                    table_name=table_name, key_columns=key_columns,
                                    outfile=fname)
            modspec = importlib.util.spec_from_file_location("table_name", fname)
            tablescript = importlib.util.module_from_spec(modspec)
            modspec.loader.exec_module(tablescript)
            tablescript.main(skip_args=True, mode='table')

    table_schema = table_schema_from_catalog(server, catalog_id, schema_name, table_name)
    if validate:
        report = goodtables.validate(table_loc, schema=table_schema.descriptor)
        if not report['valid']:
            return report
    print("Loading table....")
    credential = get_credential(server)
    catalog = ErmrestCatalog('https', server, catalog_id, credentials=credential)
    pb = catalog.getPathBuilder()
    source_table = Table(table_loc, schema=table_schema.descriptor, post_cast=[date_to_text])
    target_table = pb.schemas[schema_name].tables[table_name]
    entities = source_table.read(keyed=True)
    target_table.insert(entities, add_system_defaults=True)
    return


def main():
    return


if __name__ == "__main__":
    main()
