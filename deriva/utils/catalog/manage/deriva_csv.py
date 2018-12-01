from __future__ import print_function
from __future__ import absolute_import
import os
import tempfile
import re
import datetime
import itertools
import argparse
from decimal import Decimal
from requests import HTTPError
from tableschema import Table, Schema, exceptions
from tableschema.schema import _TypeGuesser
import goodtables

from deriva.core import ErmrestCatalog, get_credential
from deriva.core.ermrest_config import tag as chaise_tags

import deriva.core.ermrest_model as em
from deriva.utils.catalog.manage.dump_catalog import DerivaConfig, DerivaCatalogToString, load_module_from_path
from deriva.utils.catalog.manage.loopback_catalog import LoopbackCatalog

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


class DerivaUploadError(HTTPError):
    def __init__(self, chunk_size, chunk_number, http_err):
        self.chunk_number = chunk_number
        self.chunk_size = chunk_size
        self.reason = http_err


class DerivaCSVError(Exception):
    def __init__(self, msg):
        self.msg = msg


class DerivaModel:
    """
    Class to represent a CSV schema as a dervia catalog model. This class takes a table schema, performs name
    mapping of column names and generates a derviva-py model.
    """

    def __init__(self, csvschema):
        self._csvschema = csvschema
        self._catalog = LoopbackCatalog()
        self.model = em.Model({})
        self.column_map = {}
        self.type_map = {}

        schema_def = em.Schema.define(csvschema.schema_name, comment="Schema from tableschema")
        schema = self.model.create_schema(self._catalog, schema_def)

        table_def = em.Table.define(csvschema.table_name)
        table = schema.create_table(self._catalog, table_def)

        self.__deriva_columns(table, csvschema)
        self.__deriva_keys(table, csvschema)
        return

    def __deriva_columns(self, table, csvschema):
        """
        Add deriva_py column definitions, one for each field in the schema.
        :return:
        """

        system_columns = ['RID', 'RCB', 'RMB', 'RCT', 'RMT']
        for col in csvschema.schema.fields:
            # Don't include system columns in the list of column definitions.
            if col.name in system_columns:
                continue
            mapped_name = csvschema.map_name(col.name)
            self.column_map[col.name] = mapped_name
            self.type_map.setdefault(table_schema_ermrest_type_map[col.type + ':' + col.format], []).append(col.name)

            t = "{}:{}".format(col.type, col.format)
            c = em.Column.define(mapped_name, em.builtin_types[table_schema_ermrest_type_map[t]],
                                 nullok=not col.required, comment=col.descriptor.get('description', ''))
            table.create_column(self._catalog, c)
        return

    def __deriva_keys(self, table, csvschema):

        # Create a key definition for the RID column.
        constraint_name = (csvschema.schema_name, csvschema.map_name('{}_{}_Key)'.format(csvschema.table_name, 'RID')))
        k = em.Key.define(['RID'], constraint_names=[constraint_name])
        table.create_key(self._catalog, k)

        # Create a key definition for the primary key.
        if len(csvschema.schema.primary_key) > 1:
            constraint_name = \
                (csvschema.schema_name,
                 csvschema.map_name('{}_{}_Key)'.format(csvschema.table_name, '_'.join(csvschema.schema.primary_key))))
            k = em.Key.define([csvschema.schema.primary_key], constraint_names=[constraint_name])
            table.create_key(self._catalog, k)

        # Create a key definition for any columns that have a unique constraint.
        for col in csvschema.schema.fields:
            mapped_name = csvschema.map_name(col.name)
            if col.constraints.get('unique', False):
                constraint_name = (csvschema.schema_name,
                                   csvschema.map_name('{}_{}_Key)'.format(csvschema.table_name, mapped_name)))
                k = em.Key.define([col.name], constraint_names=[constraint_name])
                table.create_key(self._catalog, k)
        return


class DerivaCSV(Table):

    def __init__(self, source, server, catalog_id, schema_name, table_name=None, map_columns=True, key_columns=None,
                 schema=None, strict=False, post_cast=None, storage=None, config=None, **options):
        """

        :param source:
        :param server:
        :param catalog_id:
        :param schema_name:
        :param table_name:
        :param map_columns:
        :param key_columns:
        :param schema:
        :param strict:
        :param post_cast: Passed to tableschema
        :param storage:  Passed to tableschema
        :param options: Options passed on to tableschema init function
        """

        if not post_cast:
            post_cast = []
        super(DerivaCSV, self).__init__(source, schema=schema, strict=strict, post_cast=post_cast, storage=storage,
                                        **options)

        DerivaConfig(config)
        groups = DerivaConfig.groups

        self.source = source
        self.table_name = table_name
        self._map_columns = map_columns
        self._key_columns = key_columns
        self.schema_name = schema_name
        self._server = server
        self._catalog_id = catalog_id
        self.row_count = None
        self.validation_report = None

        # Pull apart column map.  If its just a list, then its a word map, otherwise, its a column_map.
        if self._map_columns:
            self._column_map = {}
            self._word_map = []
            if isinstance(self._map_columns, list):
                # We have a word map.
                self._word_map = self._map_columns
            elif isinstance(self._map_columns, dict):
                self._word_map = self._map_columns.get('word_map', [])
                self._column_map = self._map_columns

        # If tablename is not specified, use the file name of the data file as the table name.
        if not self.table_name:
            self.table_name = os.path.splitext(os.path.basename(source))[0]

        # Make the table name consistent with the naming strategy
        if self._map_columns:
            self.table_name = self.map_name(self.table_name)

        # Create variable substitutions that will be used in annotations, tags, and such...
        self._variables = {k: v for k, v in groups.items()}
        self._variables.update(chaise_tags)

        # Do initial infer to set up headers and schema.
        Table.infer(self)

        # Headers have to be unique
        if len(self.headers) != len(set(self.headers)):
            raise DerivaCSVError(msg='Duplicated column name in table')

        # Keys column can be a  single column or a list of a list of columns.  Normlize to list of lists...
        if self._key_columns is None or self._key_columns == []:
            self._key_columns = []
        elif not isinstance(self._key_columns, list):
            self._key_columns = [[self._key_columns]]

        # Set the primary key value.  Use primary_key if there is one in the schema.  Otherwise, use the first
        # key in the key_columns list.
        if self.schema.primary_key:
            primary_key = self.schema.primary_key if isinstance(self.schema.primary_key, list) else \
                [self.schema.primary_key]
            key_columns.append(self.schema.primary_key)
        elif self._key_columns:
            primary_key = self._key_columns[0]
        else:
            primary_key = []

        # Make sure primary key actually names columns...
        if len(primary_key) > 0:
            if all(map(lambda x: x in self.schema.field_names, primary_key)):
                self.schema.descriptor['primaryKey'] = primary_key
            else:
                print('Missing key column: ', primary_key)

        # Capture the key columns.
        for k in self._key_columns:
            # All columns good?
            if all(map(lambda x: x in self.schema.field_names, k)):
                if len(k) == 1:
                    self.schema.get_field(k[0]).descriptor['constraints'] = {'required': True, 'unique': True}
            else:
                raise DerivaCSVError('Cannot handle composite keys on table')
        self.schema.commit(strict=True)

        return

    def infer(self, limit=100, confidence=.75):
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

        rindex = 0
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

    def validate(self, validation_limit=None):
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

        table_schema = self.table_schema_from_catalog()

        # First, just check the headers to make sure they line up under mapping.
        report = goodtables.validate(self.source, schema=table_schema.descriptor, checks=['non-matching-header'])
        if not report['valid'] and self._map_columns:
            mapped_headers = map(lambda x: self.map_name(x), report['tables'][0]['headers'])
            bad_headers = list(filter(lambda x: x[0] != x[1], zip(table_schema.field_names, mapped_headers)))
            if bad_headers:
                report['headers'] = [x[1] for x in bad_headers]
                return report['valid'], report, validation_limit
        report = goodtables.validate(self.source, row_limit=validation_limit, schema=table_schema.descriptor,
                                     skip_checks=['non-matching-header'])
        self.validation_report = report

        return report['valid'], report

    def table_schema_from_catalog(self, skip_system_columns=True, outfile=None):
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
            catalog_schema = Schema(descriptor={'fields': fields, 'missingValues': ['', 'N/A', 'NULL']}, strict=True)
            if primary_key:
                catalog_schema.descriptor['primaryKey'] = primary_key
            catalog_schema.commit(strict=True)
            if outfile:
                table_schema.save(outfile)
        except exceptions.ValidationError as exception:
            print('error.....')
            print(exception.errors)
            raise exception

        return catalog_schema

    def upload_to_deriva(self, chunk_size=1000, starting_chunk=1):
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
        self.table_schema_from_catalog()
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
                raise DerivaUploadError(chunk_size, chunk_cnt, e)
        return row_cnt, chunk_size, chunk_cnt

    def convert_to_deriva(self, outfile=None, schemafile=None):
        """
        Read in a table, try to figure out the type of its columns and output a deriva-py program that can be used
        to create the table in a catalog.

        :param outfile: Where to put the deriva_py program.
        :param schemafile: dump tableschema output
        :return: dictionary that has the column name mapping derived by this routine.
        """

        if not outfile:
            outfile = self.table_name + '.py'

        if schemafile is True:
            schemafile = self.table_name + '.json'
        if not schemafile:
            self.infer()
            self.schema.save(self.table_name + '.json')

        deriva_model = DerivaModel(self)

        stringer = DerivaCatalogToString(deriva_model, self._server, self._catalog_id, variables=self._variables)
        table_string = stringer.table_to_str(self.schema_name, self.table_name)
        with open(outfile, 'w') as stream:
            print(table_string, file=stream)
        return deriva_model.column_map, deriva_model.type_map

    def create_validate_upload_csv(self, convert=True, validate=False, create=False, upload=False,
                                   derivafile=None, schemafile=None,
                                   chunk_size=1000, starting_chunk=1):
        """

        :param convert: If true, use table inference to infer types for columns of table and create a deriva-py program
        :param derivafile: Specify the file name of where the deriva-py program to create the table exists
        :param schemafile: File that contains tableschema. May be input or output depending on other arguments
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
                    self.convert_to_deriva(outfile=derivafile, schemafile=schemafile)
                if create:
                    tablescript = load_module_from_path(derivafile)
                    # Now create the table.
                    tablescript.main(skip_args=True, mode='table')

        if validate:
            valid, report = self.validate()
            if not valid:
                for i in report['tables'][0]['errors']:
                    print(i)
                return 0, 1, report

        if upload:
            print("Loading table....")
            row_cnt, chunk_size, chunk_cnt = \
                self.upload_to_deriva(chunk_size=chunk_size, starting_chunk=starting_chunk)

            return row_cnt, chunk_size, chunk_cnt

    def map_name(self, name, column_map=None, word_map=None):
        """
        A simple function that attempts to map a column name into a name that follows the deriva naming convetions.
        :param name: Column name to be mapped
        :param column_map: map of column names that should be used directly without additional modification
        :param word_map: list of words with capitolization that should be used exactly if there is a case insensitive
                          match
        :return: Resulting column name.
        """

        if self._map_columns is False:
            return

        if word_map is None:
            word_map = self._word_map
        if column_map is None:
            column_map = self._column_map

        word_dict = {i.upper(): i for i in word_map}

        # TODO This can be made much more robust.  Right now handled mixed camel and snake case and parenthesis
        if name in column_map:
            mname = column_map[name]
        else:
            # Split words based on capitol first letter, or existing underscore.  Capitolize the first letter of each
            # word unless it is in the provided word list.
            split_words = '[A-Z]+[a-z0-9]*|[a-z0-9]+|\(.*?\)|[+\/\-*@<>%&=]'
            word_list = re.findall(split_words, name)
            word_map = map(lambda x: word_dict[x.upper()] if x.upper() in word_dict else x[0].upper() + x[1:],
                           word_list)
            mname = '_'.join(list(word_map))

        return mname


def main():
    # Argument parser
    parser = argparse.ArgumentParser(description="Load CSV and other table formats into deriva catalog")

    parser.add_argument('tabledata', help='Location of tablelike data to be added to catalog')
    parser.add_argument('server', help='Catalog server name')
    parser.add_argument('schema', help='Name of the schema to be used for table')
    parser.add_argument('--catalog', default=1, help='ID number of desired catalog (Default:1)')
    parser.add_argument('--table', default=None, help='Name of table to be managed (Default:tabledata filename)')
    parser.add_argument('--key_columns', default=[],
                        help='List of columns to be used as key when creating table schema.')
    parser.add_argument('--convert', action='store_true',
                        help='Generate a deriva-py program to create the table [Default:True]')
    parser.add_argument('--map_columns', default=True,
                        help='Convert column names to cannonical form [Default:True]. Can specify mappings')
    parser.add_argument('--derivafile', default=None,
                        help='Filename for deriva-py program. Can be input or output depending on other arguments. [Default: table name]')
    parser.add_argument('--schemafile', default=None, help='File which contains table schema to be used')
    parser.add_argument('--chunk_size', default=10000, help='Number of rows to use in chunked upload [Default:10000]')
    parser.add_argument('--starting_chunk', default=1, help='Starting chunk number [Default:1]')
    parser.add_argument('--validate', action='store_true',
                        help='Validate the table before uploading [Default:False]')
    parser.add_argument('--create_table', action='store_true',
                        help='Automatically create catalog table based on column type inference [Default:False]')
    parser.add_argument('--upload', action='store_true', help='Load data into catalog [Default:False]')
    parser.add_argument('--config', default=None, help='python script to set up configuration variables)')

    args = parser.parse_args()
    print(args)

    table = DerivaCSV(args.tabledata, args.server, args.catalog, args.schema, config=args.config,
                      table_name=args.table, map_columns=args.map_columns)

    table.create_validate_upload_csv(
        convert=args.convert, validate=args.validate, create=args.create_table, upload=args.upload,
        derivafile=args.derivafile, schemafile=args.schemafile,
        chunk_size=args.chunk_size, starting_chunk=args.starting_chunk)
    return


if __name__ == "__main__":
    main()
