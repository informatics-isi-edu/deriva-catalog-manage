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
from tableschema.schema import _TypeGuesser
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


class DerivaCSV(Table):

    def __init__(self, source, server, catalog_id, schema_name, table_name=None, map_column_names=True,
                 schema=None, strict=False, post_cast=[], storage=None, **options):

        super(DerivaCSV, self).__init__(source, schema=schema, strict=strict, post_cast=post_cast, storage=storage,
                                        **options)
        # If tablename is not specified, use the file name of the data file as the table name.

        self.source = source
        self.table_name = table_name
        self._map_column_names = map_column_names

        if not self.table_name:
            self.table_name = os.path.splitext(os.path.basename(source))[0]
        if self._map_column_names:
            self.table_name = self.CannonicalName(self.table_name, self._map_column_names)

        self.schema_name = schema_name
        self._server = server
        self._catalog_id = catalog_id
        self.row_count = None

        # Create variable substitutions.
        self._variables = {k: v for k, v in groups.items()}
        self._variables.update(chaise_tags)
        return

    def infer(self):
        """https://github.com/frictionlessdata/tableschema-py#schema
        """
        # Do initial infer to set up headers and schema.
        Table.infer(self)

        missing_values = ['']
        rows = self.read(cast=False)
        headers = self.headers

        # Get descriptor
        guesser = _TypeGuesser()
        descriptor = {'fields': []}
        type_matches = {}
        for header in headers:
            descriptor['fields'].append({'name': header})
        for rindex, row in enumerate(rows):
            # Normalize rows with invalid dimensions for sanity
            row_length = len(row)
            headers_length = len(headers)
            if row_length > headers_length:
                row = row[:len(headers)]
            if row_length < headers_length:
                diff = headers_length - row_length
                fill = [''] * diff
                row = row + fill
            # build a column-wise lookup of type matches
            for cindex, value in enumerate(row):
                # We skip over empty elements.  For non-empty, we find the most specific type that we can use for the
                # element, and then compare with other rows and keep the most general.
                if value not in missing_values:
                    rv = list(guesser.cast(value))
                    best_type = min(i[2] for i in rv)
                    typeid = list(filter(lambda x: x[2] == best_type, rv))[0]
                    if type_matches.get(cindex):
                        type_matches[cindex] = typeid if typeid[2] > type_matches[cindex][2] else type_matches[cindex]
                    else:
                        type_matches[cindex] = typeid
        self.row_count = rindex
        for index, results in type_matches.items():
            descriptor['fields'][index].update({'type': results[0], 'format': results[1]})

        # Now update the schema to have the inferred values.
        self.schema.descriptor['fields'] = descriptor['fields']
        self.schema.commit()
        return

    def Validate(self, validation_limit=None):
            """
            For the specified table data, validate the contents of the table against an existing table in a catalog.
            :param validation_limit: How much of the table to check. Defaults to entire table.
            :return: an error report and the number of rows in the table as a tuple
            """

            if not self.row_count:
                self.row_count = len(Table.read(self))

            if not validation_limit:
                # Validate the whole file.
                validation_limit = self.row_count

            table_schema = self.TableSchemaFromCatalog()

            # First, just check the headers to make sure they line up under mapping.
            report = goodtables.validate(self.source, schema=table_schema.descriptor, checks=['non-matching-header'])
            if not report['valid'] and self._map_column_names:
                mapped_headers = map(lambda x: self.CannonicalName(x, self._map_column_names),
                                     report['tables'][0]['headers'])
                bad_headers = list(filter(lambda x: x[0] != x[1], zip(table_schema.field_names, mapped_headers)))
                if bad_headers:
                    report['headers'] = [x[1] for x in bad_headers]
                    return report['valid'], report, validation_limit
            report = goodtables.validate(self.source, row_limit=validation_limit, schema=table_schema.descriptor,
                                         skip_checks=['non-matching-header'])

            return report['valid'], report

    def TableSchemaFromCatalog(self, skip_system_columns=True, outfile=None):
        """
        Create a TableSchema by querying an ERMRest catalog and converting the model format.

        :param outfile: if this argument is specified, dump the scheme into the specified file.
        :param skip_system_columns: Don't include system columns in the schema.
        :return: table schema representation of the model
        """
        credential = get_credential(self._server)
        catalog = ErmrestCatalog('https', self._server, self._catalog_id, credentials=credential)
        model_root = catalog.getCatalogModel()
        schema = model_root.schemas[self.schema_name]
        table = schema.tables[self.table_name]
        fields = []
        primary_key = None
        table_schema = None

        for col in table.column_definitions:
            if col.name in ['RID', 'RCB', 'RMB', 'RCT', 'RMT'] and skip_system_columns:
                continue
            field = {
                "name": col.name,
                "type": table_schema_type_map[col.type.typename][0],
                "constraints": {}
            }

            if field['type'] == 'boolean':
                field['trueValues'] = ["true", "True", "TRUE", "1", "T", "t"]
                field['falseValues'] = ["false", "False", "FALSE", "0", "F", "f"]

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
            self.schema.descriptor['fields'] = fields
            self.schema.descriptor['missingValues'] = ['', 'N/A', 'NULL']
            if primary_key:
                self.schema.descriptor['primaryKey'] = primary_key
            self.schema.commit(strict=True)
            if outfile:
                table_schema.save(outfile)
        except exceptions.ValidationError as exception:
            print('error.....')
            print(exception.errors)

        return self.schema

    def UploadToDeriva(self, chunk_size=1000, starting_chunk=1):
        """
        Upload the source table to deriva.

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

        print("Loading table....")
        credential = get_credential(self._server)
        catalog = ErmrestCatalog('https', self._server, self._catalog_id, credentials=credential)
        pb = catalog.getPathBuilder()
        self.TableSchemaFromCatalog()
        source_table = Table(self.source, schema=self.schema.descriptor, post_cast=[date_to_text])
        target_table = pb.schemas[self.schema_name].tables[self.table_name]

        if not self.schema.primary_key:
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

    def ConvertToDeriva(self, outfile=None, schemafile=None, key_columns=None):
        """
        Read in a table, try to figure out the type of its columns and output a deriva-py program that can be used
        to create the table in a catalog.

        :param outfile: Where to put the deriva_py program.
        :param schemafile: dump tableschema output
        :param key_columns: a list of columns that will make up the primary key.
        :return: dictionary that has the column name mapping derived by this routine.
        """
        column_map = {}

        if not outfile:
            outfile = self.table_name + '.py'

        if not schemafile:
            self.infer()
            self.schema.save(self.table_name + '.json')

        column_types = {}
        for c in self.schema.fields:
            column_map[c.name] = self.CannonicalName(c.name) if self._map_column_names else c.name
            c.descriptor['name'] = column_map[c.name]
            column_types.setdefault(table_schema_ermrest_type_map[c.type + ':' + c.format], []).append(c.name)

        if key_columns:
            if self._map_column_names:
                key_columns = [self.CannonicalName(c) for c in key_columns]
            if not type(key_columns) is list:
                key_columns = [key_columns]
            # Set the primary key value
            for i in key_columns:
                if i not in column_map:
                    print('Missing key column')
            self.schema.descriptor['primaryKey'] = key_columns
            for i, col in enumerate(self.schema.fields):
                if col.name in key_columns:
                    self.schema.descriptor['fields'][i]['constraints'] = {'required': True, 'unique': True}
        self.schema.commit()

        with open(outfile, 'w') as stream:
            table_string = self.TableToStr()
            table_string = FormatCode(table_string, style_config=yapf_style)[0]
            print(table_string, file=stream)
        return column_map, column_types

    def CreateValidateUploadCSV(self, convert=True, validate=True, create=False, upload=False,
                                key_columns=None,
                                derivafile=None,
                                chunk_size=1000, starting_chunk=1):
        """

        :param key_columns: list of columns that form a primary key in the source table (non-null and unique)
        :param convert: If true, use table inference to infer types for columns of table and create a deriva-py program
        :param derivafile: Specify the file name of where the deriva-py program to create the table exisits
        :param create: If true, create a table in the catalog.
               If derivafile argument is not specified, then infer table definition
        :param validate: Run table validation on input before trying to upload
        :param upload:
        :param chunk_size: Number of rows to upload at one time.
        :param starting_chunk: What chunk number to start at.  Can be used to continue a failed upload.
        :return:
        """

        if create or convert:
            print('Creating table definition {}:{}'.format(self.schema_name, self.table_name))
            with tempfile.TemporaryDirectory() as tdir:
                # If convertdir is set, put deriva-py program in current directory.
                odir = os.path.dirname(os.path.abspath(self.source)) if convert else tdir
                if (not derivafile) or convert:
                    derivafile = '{}/{}.py'.format(odir, self.table_name)
                    self.ConvertToDeriva(key_columns=key_columns, outfile=derivafile)
                if create:
                    tablescript = load_module_from_path(derivafile)
                    # Now create the table.
                    tablescript.main(skip_args=True, mode='table')

        if validate:
            valid, report, row_cnt = self.Validate()
            if not valid:
                for i in report['tables'][0]['errors']:
                    print(i)
                return 0, 1, report

        if upload:
            print("Loading table....")
            row_cnt, chunk_size, chunk_cnt = \
                self.UploadToDeriva(chunk_size=chunk_size, starting_chunk=starting_chunk)

            return row_cnt, chunk_size, chunk_cnt

    def CannonicalName(self, name, map_columns=None):
        """
        A simple function that attempts to map a column name into a name that follows the deriva naming convetions.
        :param name: Column name to be mapped
        :param map_columns
        :return: Resulting column name.
        """
        if not map_columns:
            map_columns = self._map_column_names
        if not isinstance(map_columns, list):
            map_columns = []
        map_dict = {i.upper(): i for i in map_columns}

        # TODO This can be made much more robust.  Right now handled mixed camel and snake case and parenthesis

        # Split words based on capitol first letter, or existing underscore.
        split_words = '[A-Z]+[a-z0-9]*|[a-z0-9]+|\(.*?\)|[+\/\-*@<>%&=]'
        word_list = re.findall(split_words, name)
        mname = '_'.join(list(map(lambda x: map_dict[x.upper()] if x.upper() in map_dict else x[0].upper() + x[1:], word_list)))

        return mname

    def TableAnnotationsToStr(self):
        s = tag_variables_to_str({}, variables=self._variables)
        s += annotations_to_str({}, 'table_annotations', variables=self._variables)
        s += variable_to_str('table_comment', None, self._variables)
        s += variable_to_str('table_acls', {}, self._variables)
        s += variable_to_str('table_acl_bindings', {}, self._variables)
        return s

    def ColumnAnnotationsToStr(self):
        column_annotations = {}
        column_acls = {}
        column_acl_bindings = {}
        column_comment = {}
        for i in self.schema.fields:
            if 'description' in i.descriptor and not (i.descriptor['description'] == '' or not i.descriptor['description']):
                column_comment[i.name] = i.comment
        s = variable_to_str('column_annotations', column_annotations, self._variables)
        s += variable_to_str('column_comment', column_comment, self._variables)
        s += variable_to_str('column_acls', column_acls)
        s += variable_to_str('column_acl_bindings', column_acl_bindings, self._variables)
        return s

    def ForeignKeyDefsToStr(self):
        s = 'fkey_defs = [\n'
        for fkey in self.schema.foreign_keys:
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

    def KeyDefsToStr(self):
        s = 'key_defs = [\n'
        constraint_name = (self.schema_name, self.CannonicalName('{}_{}_Key)'.format(self.table_name, 'RID')))
        s += """    em.Key.define({},
                     constraint_names=[{!r}],\n),\n""".format(['RID'], constraint_name)

        if len(self.schema.primary_key) > 1:
            constraint_name = \
                    (self.schema_name,
                     self.CannonicalName('{}_{}_Key)'.format(self.table_name, '_'.join(self.schema.primary_key))))
            s += """    em.Key.define({},
                         constraint_names=[{!r}],\n),\n""".format(self.schema.primary_key, constraint_name)

        for col in self.schema.fields:
            if col.constraints.get('unique', False):
                constraint_name = (self.schema_name,
                                   self.CannonicalName('{}_{}_Key)'.format(self.table_name, col.name)))
                s += """    em.Key.define([{!r}],
                         constraint_names=[{!r}],\n""".format(col.name, constraint_name)
                s += '),\n'
        s += ']'
        return s

    def ColumnDefsToStr(self):
        """
        Print out a list of the deriva_py column definions, one for each field in the schema.
        :return:
        """

        system_columns = ['RID', 'RCB', 'RMB', 'RCT', 'RMT']

        s = 'column_defs = ['
        for col in self.schema.fields:
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

    def TableToStr(self):
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
        main()""".format(server=self._server, catalog_id=self._catalog_id,
                         table_name=self.table_name, schema_name=self.schema_name,
                         groups=variable_to_str('groups', groups),
                         column_annotations=self.ColumnAnnotationsToStr(),
                         column_defs=self.ColumnDefsToStr(),
                         table_annotations=self.TableAnnotationsToStr(),
                         key_defs=self.KeyDefsToStr(),
                         fkey_defs=self.ForeignKeyDefsToStr(),
                         table_def=table_def_to_str(True))
        return s


def main():
    global groups
    # Argument parser
    parser = argparse.ArgumentParser(description="Load CSV and other table formats into deriva catalog")

    parser.add_argument('tabledata', help='Location of tablelike date to be added to catalog')
    parser.add_argument('server', help='Catalog server name')
    parser.add_argument('schema', help='Name of the schema to be used for table')
    parser.add_argument('--catalog', default=1, help='ID number of desired catalog (Default:1)')
    parser.add_argument('--table', default=None, help='Name of table to be managed (Default:tabledata filename)')
    parser.add_argument('--key_columns', default=[],
                        help='List of columns to be used as key when creating table schema')
    parser.add_argument('--convert', action='store_true',
                        help='Generate a deriva-py program to create the table [Default:False]')
    parser.add_argument('--map_column_names', action='store_true',
                        help='Automatically convert column names to cannonical form [Default:True]')
    parser.add_argument('--derivafile', default=None, help='Filename for output deriva-py program')
    parser.add_argument('--schemafile', default=None, help='File which contains table schema to be used')
    parser.add_argument('--chunk_size', default=10000, help='Number of rows to use in chunked upload [Default:10000]')
    parser.add_argument('--starting_chunk', default=1, help='Starting chunk number [Default:1]')
    parser.add_argument('--validate', action='store_true',
                        help='Validate the table before uploading [Default:True]')
    parser.add_argument('--create_table', action='store_true',
                        help='Automatically create catalog table based on column type inference [Default:False]')
    parser.add_argument('--upload', action='store_true', help='Load data into catalog [Default:False]')
    parser.add_argument('--config', default=None, help='python script to set up configuration variables)')

    args = parser.parse_args()

    if args.config:
        config = load_module_from_path(args.config)
        groups = AttrDict(config.groups)

    table = DerivaCSV(args.tabledata, args.server, args.catalog, args.schema,
                        table_name=args.table, map_column_names=args.map_column_names)

    table.CreateValidateUploadCSV(
        key_columns=args.key_columns,
        convert=args.convert, derivafile=args.derivafile, create=args.create_table, validate=args.validate,
        upload=args.upload,
        chunk_size=args.chunk_size, starting_chunk=args.starting_chunk)
    return


if __name__ == "__main__":
    main()
