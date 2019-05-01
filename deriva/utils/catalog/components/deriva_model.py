import logging
import logging.config
import pprint
from collections import namedtuple, OrderedDict
from enum import Enum
from urllib.parse import urlparse

# test
import deriva.core.ermrest_model as em
import tabulate
from deriva.core import ErmrestCatalog, get_credential
from deriva.core.ermrest_config import MultiKeyedList
from deriva.core.ermrest_config import tag as chaise_tags

logger = logging.getLogger(__name__)
logging.basicConfig(format='[%(lineno)d] %(funcName)20s() %(message)s')
logger.setLevel(logging.DEBUG)

chaise_tags['catalog_config'] = 'tag:isrd.isi.edu,2019:catalog-config'
CATALOG_CONFIG__TAG = 'tag:isrd.isi.edu,2019:catalog-config'


class DerivaCatalogError(Exception):
    def __init__(self, obj, msg):
        self.msg = msg
        self.obj = obj

class DerivaSourceError(DerivaCatalogError):
    def __init__(self, obj, msg):
        super().__init__(obj, msg)

class DerivaMethodFilter:
    def __init__(self, include=None, exclude=None):
        self.include = include
        self.exclude = exclude

    def filter(self, record):
        if self.include:
            return record.funcName in self.include
        if self.exclude:
            return record.funcName not in self.exclude
        return True

# Add filters: ['source_spec'] to use filter.

logger_config = {
    'disable_existing_loggers': True,
    'version': 1,
    'filters': {
        'method_filter': {
            '()': DerivaMethodFilter,
            'include': ['delete_visible_source']
        }
    },
    'formatters': {
        'class': {
            'style': '{',
            'format': '{levelname} {name}.{funcName}: {message}'
        },
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'formatter': 'class',
  #          'filters': ['method_filter'],
            'class': 'logging.StreamHandler',
        },
    },
    'loggers': {
        'deriva_model': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': False
        },
        'deriva_model.DerviaModel': {
        },
        'deriva_model.DerivaCatalog': {
        },
        'deriva_model.DerivaColumnMap': {
        },
        'deriva_model.DerivaSchema': {

        },
        'deriva_model.DerivaVisibleSources': {
            'level': 'DEBUG'
        },
        'deriva_model.DerivaSourceSpec': {
            'level': 'DEBUG'
        },
        'deriva_model.DerivaTable': {
        },
        'deriva_model.DerviaColumn': {
        },
        'deriva_model.DerivaKey': {
        },
        'deriva_model.DerivaForeignKey': {
        }
    },
}

logging.config.dictConfig(logger_config)


class DerivaLogging:
    def __init__(self, **kwargs):
        self.logger = logging.getLogger('{}.{}'.format('deriva_model', type(self).__name__))


class DerivaContext(Enum):
    compact = "compact"
    compact_brief = "compact/brief"
    compact_select = "compact/select"
    detailed = "detailed"
    entry = "entry"
    entry_edit = "entry/edit"
    entry_create = "entry/create"
    filter = "filter"
    row_name = "row_name"
    row_name_title = "row_name/title"
    row_name_compact = "row_name/compact"
    row_name_detailed = "row_name/detailed"
    star = "*"
    all = "all"


class ElementList:
    """
    This is a helper class that is used to allow lists of model elements to be retrieved or iterated over.
    """

    def __init__(self, fvalue, lst):
        """

        :param fvalue: A function that retrieves the value from a model
        :param lst: A lost of model elements
        """
        self.fvalue = fvalue
        self.lst = lst

    def __getitem__(self, item):
        return self.fvalue(item)

    def __contains__(self, item):
        try:
            self.__getitem__(item)
            return True
        except DerivaCatalogError:
            return False

    def __len__(self):
        return len(self.lst)

    def __delitem__(self, key):
        key_name = self.fvalue(key).table.schema_name, self.fvalue(key).name
        self.lst.__delitem__(key_name)

    def __iter__(self):
        def element_iterator(lst):
            for s in lst:
                yield self.fvalue(s)

        return element_iterator(self.lst)


class DerivaDictValue:
    # TODO This is not really doing anything.  Could change to UserDict
    def __init__(self, value):
        self.value = value

    def __getitem__(self, item):
        return self.value[item]

    def __setitem__(self, instance, value):
        self.update({instance: value})

    def __contains__(self, item):
        try:
            self.__getitem__(item)
            return True
        except KeyError:
            return False

    def update(self, items):
        self.value.update(items)

    def items(self):
        return self.value.items()

    def keys(self):
        return self.value.keys()

    def __iter__(self):
        def dictval_iterator(val):
            for i in val:
                yield i

        return dictval_iterator(self.value)


class DerivaModel(DerivaLogging):
    contexts = {i for i in DerivaContext if i is not DerivaContext("all")}

    def __init__(self, catalog):
        super().__init__()
        self.catalog = catalog

    def __enter__(self):
        self.catalog.nesting += 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.catalog.nesting -= 1
        if self.catalog.nesting == 0:
            self.catalog.apply()

    def model_element(self, obj):
        try:
            return self.column_model(obj)
        except (AttributeError, KeyError):
            pass
        try:
            return self.key_model(obj)
        except (AttributeError, KeyError):
            pass
        try:
            return self.foreign_key_model(obj)
        except (AttributeError, KeyError):
            pass
        try:
            return self.table_model(obj)
        except (AttributeError, KeyError):
            pass
        try:
            return self.schema_model(obj)
        except (AttributeError, KeyError):
            return self.catalog_model()

    def schema_exists(self, schema_name):
        try:
            return self.catalog.model_instance.schemas[schema_name]
        except KeyError:
            return False

    def table_exists(self, schema, table_name):
        try:
            return self.schema_model(schema).tables[table_name]
        except KeyError:
            return False

    def column_exists(self, table, column_name):
        try:
            return self.table_model(table).column_definitions[column_name]
        except KeyError:
            return False

    def key_exists(self, table, key_id):
        key_name = (table.schema_name, key_id) if type(key_id) == str else key_id
        try:
            return self.table_model(table).keys[key_name]
        except (KeyError, TypeError):
            with DerivaModel(self.catalog) as m:
                # See if we can look up the key by its unique columns
                cols = set(key_id) if isinstance(key_id, list) else {key_id}
                for k in m.table_model(table).keys:
                    if set(k.unique_columns) == cols:
                        return k
        return False

    def foreign_key_exists(self, table, fkey_id):
        fkey_name = (table.schema_name, fkey_id) if type(fkey_id) == str else fkey_id
        self.logger.debug('table: %s fkey_id: %s fkey_name %s', table.name, fkey_id, fkey_name)
        try:
            self.logger.debug('foreign keys %s', self.table_model(table).foreign_keys[fkey_name])
            return self.table_model(table).foreign_keys[fkey_name]
        except (KeyError, TypeError):
            with DerivaModel(self.catalog) as m:
                # See if we can look up the key by its unique columns
                cols = set(fkey_id) if isinstance(fkey_id, list) else {fkey_id}
                for k in m.table_model(table).foreign_keys:
                    if {c['column_name'] for c in k.foreign_key_columns} == cols:
                        return k
            return False

    def catalog_model(self):
        return self.catalog.model_instance

    def schema_model(self, schema):
        return self.catalog_model().schemas[schema.name]

    def table_model(self, table):
        return self.schema_model(table.schema).tables[table.name]

    def column_model(self, column):
        return self.table_model(column.table).column_definitions[column.name]

    def key_model(self, key):
        return self.table_model(key.table).keys[(key.table.schema_name, key.name)]

    def foreign_key_model(self, fkey):
        return self.table_model(fkey.table).foreign_keys[(fkey.table.schema_name, fkey.name)]


class DerivaCore(DerivaLogging):
    def __init__(self, catalog):
        self.catalog = catalog
        super().__init__()

    @property
    def annotations(self):
        with DerivaModel(self.catalog) as m:
            return DerivaDictValue(m.model_element(self).annotations)


class DerivaCatalog(DerivaCore):
    model_instance = None

    def __init__(self, host, scheme='https', catalog_id=1):
        """
        Initialize a DerivaCatalog.
        :param host:
        :param scheme:
        :param catalog_id:
        """

        self.nesting = 0

        super().__init__(self)

        self.ermrest_catalog = ErmrestCatalog(scheme, host, catalog_id, credentials=get_credential(host))
        self.model_instance = self.ermrest_catalog.getCatalogModel()
        self.schema_classes = {}

    def apply(self):
        self.model_instance.apply(self.ermrest_catalog)
        return self

    def refresh(self):
        assert (self.nesting == 0)
        logger.debug('Refreshing model')
        server_url = urlparse(self.ermrest_catalog.get_server_uri())
        catalog_id = server_url.path.split('/')[-1]
        self.ermrest_catalog = ErmrestCatalog(server_url.scheme,
                                              server_url.hostname,
                                              catalog_id,
                                              credentials=get_credential(server_url.hostname))
        self.model_instance = self.ermrest_catalog.getCatalogModel()

    def __str__(self):
        return '\n'.join([i.name for i in self.schemas])

    def update_referenced_by(self):
        """Introspects the 'foreign_keys' and updates the 'referenced_by' properties on the 'Table' objects.
        """
        with DerivaModel(self) as m:
            for schema in self:
                for table in schema:
                    m.table_model(table).referenced_by = MultiKeyedList([])
            self.model_instance.update_referenced_by()

    def getPathBuilder(self):
        return self.ermrest_catalog.getPathBuilder()

    def _make_schema_instance(self, schema_name):
        return DerivaSchema(self, schema_name)

    def schema(self, schema_name):
        with DerivaModel(self) as m:
            if m.schema_exists(schema_name):
                return self.schema_classes.setdefault(schema_name, self._make_schema_instance(schema_name))
            else:
                raise DerivaCatalogError(self, 'schema {} not defined'.format(schema_name))

    def create_schema(self, schema_name, comment=None, acls={}, annotations={}):
        self.model_instance.create_schema(self.ermrest_catalog,
                                          em.Schema.define(
                                              schema_name,
                                              comment=comment,
                                              acls=acls,
                                              annotations=annotations
                                          )
                                          )
        return self.schema(schema_name)

    def get_groups(self):
        if chaise_tags.catalog_config in self.annotations:
            return self.annotations[chaise_tags.catalog_config]['groups']
        else:
            raise DerivaCatalogError(self, msg='Attempting to configure table before catalog is configured')

    def __getitem__(self, schema_name):
        return self.schemas.__getitem__(schema_name)

    def __iter__(self):
        return self.schemas.__iter__()

    def __contains__(self, item):
        return self.schemas.__contains__(item)

    @property
    def schemas(self):
        with DerivaModel(self) as m:
            return ElementList(self.schema, m.catalog_model().schemas)

    @property
    def acls(self):
        with DerivaModel(self) as m:
            return DerivaDictValue(m.catalog_model().acls)


class DerivaSchema(DerivaCore):
    def __init__(self, catalog, schema_name):
        super().__init__(catalog)
        self.schema_name = schema_name
        self.schema = self
        self.table_classes = {}

    def __str__(self):
        return '\n'.join([t.name for t in self.tables])

    @property
    def display(self):
        return self.annotations[chaise_tags.display]

    @display.setter
    def display(self, value):
        self.annotations[chaise_tags.display] = value

    @property
    def name(self):
        return self.schema_name

    @property
    def comment(self):
        with DerivaModel(self.catalog) as m:
            return m.schema_model(self).comment

    @comment.setter
    def comment(self, value):
        with DerivaModel(self.catalog) as m:
            m.schema_model(self).comment = value

    @property
    def acls(self):
        with DerivaModel(self.catalog) as m:
            return DerivaDictValue(m.schema_model(self).acls)

    def _make_table_instance(self, schema_name, table_name):
        return DerivaTable(self.catalog, schema_name, table_name)

    def table(self, table_name):
        with DerivaModel(self.catalog) as m:
            if m.table_exists(self, table_name):
                return self.table_classes.setdefault(table_name,
                                                     self._make_table_instance(self.name, table_name))
            else:
                raise DerivaCatalogError(self, 'table {}:{} not defined'.format(self.name, table_name))

    def create_table(self, table_name, column_defs,
                     key_defs=[], fkey_defs=[],
                     comment=None,
                     acls={},
                     acl_bindings={},
                     annotations={}):

        # Now that we know the table name, patch up the key and fkey defs to have the correct name.
        proto_table = namedtuple('ProtoTable', ['catalog', 'schema', 'schema_name', 'name'])
        for k in key_defs:
            k.update_table(proto_table(self.catalog, self.schema, self.name, table_name))
        for k in fkey_defs:
            k.update_table(proto_table(self.catalog, self.schema, self.name, table_name))

        table = self._create_table(em.Table.define(
            table_name, [col.definition() for col in column_defs],
            key_defs=[key.definition() if isinstance(key, DerivaKey) else key for key in key_defs],
            fkey_defs=[fkey.definition() if type(fkey) is DerivaForeignKey else fkey for fkey in fkey_defs],
            comment=comment,
            acls=acls, acl_bindings=acl_bindings,
            annotations=annotations))

        with DerivaModel(self.catalog) as m:
            for fkey in fkey_defs:
                # Add foreign key to appropriate referenced_by list
                m.table_model(fkey.referenced_table).referenced_by.append((m.foreign_key_model(fkey)))
                _, _, inbound_sources = fkey.referenced_table.sources()
                fkey.referenced_table.visible_foreign_keys.insert_sources(inbound_sources)

        column_sources, outbound_sources, inbound_sources = table.sources(merge_outbound=True)
        table.visible_columns.insert_context(DerivaContext('*'), column_sources)
        table.visible_columns.insert_context(DerivaContext('entry'), column_sources)

        table.visible_foreign_keys.insert_context(DerivaContext('*'), inbound_sources)

        return table

    def _create_table(self, table_def):
        with DerivaModel(self.catalog) as m:
            schema = m.schema_model(self)
            schema.create_table(self.catalog.ermrest_catalog, table_def)
        table = self.table(table_def['table_name'])
        table.deleted = False  # Table may have been previously been deleted.
        return table

    def create_vocabulary(self, vocab_name, curie_template, uri_template='/id/{RID}', column_defs=[],
                          key_defs=[], fkey_defs=[],
                          comment=None,
                          acls={}, acl_bindings={},
                          annotations={}
                          ):
        return self._create_table(
            em.Table.define_vocabulary(vocab_name, curie_template, uri_template=uri_template,
                                       column_defs=column_defs,
                                       key_defs=key_defs, fkey_defs=fkey_defs, comment=comment,
                                       acls=acls, acl_bindings=acl_bindings,
                                       annotations=annotations)
        )

    def __getitem__(self, table_name):
        return self.tables.__getitem__(table_name)

    def __iter__(self):
        return self.tables.__iter__()

    def __contains__(self, table_name):
        self.tables.__contains__(table_name)

    @property
    def tables(self):
        with DerivaModel(self.catalog) as m:
            return ElementList(self.table, m.schema_model(self).tables)


class DerivaColumnMap(DerivaLogging, OrderedDict):
    def __init__(self, table, column_map, dest_table):
        self.table = table
        super().__init__()
        self.logger.debug('table: %s dest_table: %s column_map: %s ', table.name, dest_table.name, column_map)
        self.update(self._normalize_column_map(table, column_map, dest_table))

    def _normalize_column_map(self, table, column_map, dest_table):
        """
        Put a column map into a standard format which is a dictionary in the form of {source-name: DerivaColumnDef}
        where source-name can be in the form of a column or key name.
        A simplified format which is just the SrcCol:DestCo is converted.
        dest_table is used to specify the target table of the mapping if it is not included as part
        of the DerviaColumnSpec.  Entries for each column in columns are also added.

        Once the column_map is normalized, mappings for keys and foreign keys are added based on the columns that are
        being mapped.  We use ordered dictionaries to make the order of the columns consistant with the order of the
        columns, then the order of the column map.
.
        :param column_map:
        :param dest_table:
        :return:
        """

        def normalize_column(k, v):
            """
            The form of a column can be one of:
                column_name: DerivaColumnDef|em.Column
                new_column: typename|dict
                column: new_name
            These are all put into a standard form of name: DerivaColumnDef, with the table attribute set to dest_table
            if provided.

            :param k: Name of the column being mapped
            :param v: Either the name of the new colu\n or a dictionary of new column attributes.
            :return:
            """
            self.logger.debug('column: %s map_value: %s', k, v)
            if isinstance(v, (DerivaColumn, DerivaKey, DerivaForeignKey)):
                return v

            try:
                # Get the existing column definition if it exists.
                col = table[k]  # Get current definition for the column
            except DerivaCatalogError:
                # Column is new, so create a default definition for it. If value is a string, then its the type.
                return DerivaColumn(**{'name': k, 'table': dest_table, **({'type': v} if type(v) is str else v)})

            # We have a column remap in the form of col: new_name or col: dictionary
            # Create a proper dictionary spec for the value adding in a table entry in the case if needed.

            v = {'name': v} if type(v) is str else v
            return DerivaColumn(
                **{'table': dest_table,
                   'type': col.type,
                   'nullok': col.nullok,
                   'default': col.default,
                   'fill': col.fill,
                   'comment': col.comment,
                   'acls': col.acls,
                   'acl_bindings': col.acl_bindings}, **v)

        # Go through the columns in order and add map entries, converting any map entries that are just column names
        # or dictionaries to DerivaColumnDefs

        column_map = {k: normalize_column(k, v) for k, v in column_map.items()}

        # Collect up all of the column name maps.
        column_name_map = {k: v.name for k, v in column_map.items()}
        self.logger.debug('column_map: %s column_name_map %s', column_map, column_name_map)
        self.logger.debug('keys: %s \nfkeys %s',
                          [key.name for key in table.keys],
                          [fkey.name for fkey in table.foreign_keys])

        self.logger.debug('mapped key columns %s',
                          [[column_name_map.get(c.name, c.name) for c in key.columns] for key in table.keys])
        self.logger.debug('mapped fkey columns %s %s',
                          [[column_name_map.get(c.name, c.name) for c in fkey.columns] for fkey in table.foreign_keys],
            [(len(fkey.columns), next(iter(fkey.columns)).name in column_name_map) for fkey in table.foreign_keys])

        # Get new key and fkey definitions by mapping to new column names.
        column_map.update(
            {key.name:
                DerivaKey(
                    table=dest_table,
                    columns=[column_name_map.get(c.name, c.name) for c in key.columns],
                    comment=key.comment,
                    annotations=key.annotations
                )
                for key in table.keys if
                len(key.columns) == 1 and table._key_in_columns(column_name_map.keys(), key.columns, dest_table)
            }
        )

        column_map.update(
            {
                fkey.name:
                    DerivaForeignKey(
                        table=dest_table,
                        columns=[column_name_map.get(c.name, c.name) for c in fkey.columns],
                        dest_table=dest_table,
                        dest_columns=[c.name for c in fkey.referenced_columns],
                        comment=fkey.comment,
                        acls=fkey.acls,
                        acl_bindings=fkey.acl_bindings
                    )
                for fkey in table.foreign_keys
                if len(fkey.columns) == 1 and table._key_in_columns(column_name_map.keys(), fkey.columns, dest_table)
            }
        )
        self.logger.debug('normalized column map %s', column_map)
        return column_map

    def get_columns(self):
        return {k: v for k, v in self.items() if isinstance(v, DerivaColumn)}

    def get_keys(self):
        return {k: v for k, v in self.items() if isinstance(v, DerivaKey)}

    def get_foreign_keys(self):
        return {k: v for k, v in self.items() if isinstance(v, DerivaForeignKey)}

    def get_names(self):
        field = 'name'
        return {k: getattr(v, field) for k, v in self.items() if getattr(v, field)}


class DerivaVisibleSources(DerivaLogging):
    def __init__(self, table, tag):
        super().__init__()
        self.table = table
        self.tag = tag
        self.logger.debug('table: %s tag: %s', table.name, tag)

    def __str__(self):
        return pprint.pformat(self.table.annotations[self.tag])

    def __repr__(self):
        return pprint.pformat(self.table.annotations[self.tag])

    def __getitem__(self, item):
        return self.table.annotations[self.tag][item]

    def __iter__(self):
        return self.table.annotations[self.tag].__iter__()

    def validate(self):
        for c, l in self.table.annotations[self.tag].items():
            for j in l:
                DerivaSourceSpec(self.table, j)

    def clean(self, dryrun=False):
        new_vs = {}
        for c, l in self.table.get_annotation(self.tag).items():
            new_context = []
            for j in l:
                try:
                    new_context.append(DerivaSourceSpec(self.table, j).spec)
                except DerivaCatalogError:
                    print("Removing {} {}".format(c, j))
            new_vs.update({c: new_context})
        if not dryrun:
            self.table.annotations[self.tag] = new_vs

    @staticmethod
    def _normalize_positions(positions):
        """
        A position can be in the form:
          {context: {key:list}, context: {key:list} ...}
          {key:list, ...}
          {context,context}
        where context can be all.  Convert these into a standard format:
           { context: {key:list} ...}
        :param positions: position list
        :return: normalized position.
        """
        # If just a set of contexts, convert to normal form.
        if isinstance(positions, set) or positions == {}:
            return OrderedDict({DerivaContext(j): {}
                                for i in positions
                                for j in (DerivaModel.contexts
                                          if DerivaContext(i) is DerivaContext("all") else [i])})

        try:
            # Map all contexts to enum values...
            return OrderedDict({DerivaContext(j): v
                                for k, v in positions.items()
                                for j in (DerivaModel.contexts
                                          if DerivaContext(k) is DerivaContext("all") else [k])})

        except ValueError:
            # Keys are not valid context name, so we must have keylist dictionary.
            return OrderedDict({k: positions for k in DerivaModel.contexts})

    def insert_context(self, context, sources=[], replace=False):
        self.logger.debug('context: %s sources: %s', context, [i.spec for i in sources])
        context = DerivaContext(context)
        # Map over sources and make sure that they are all ok before we instert...
        sources = [DerivaSourceSpec(self.table, j).spec for j in sources]
        if self.tag not in self.table.annotations:
            self.table.annotations[self.tag] = {context.value: sources}
        elif context.value not in self.table.annotations[self.tag] or replace:
            self.table.annotations[self.tag][context.value] = sources
        return

    def insert_sources(self, source_list, positions={}):
        """
        Insert a set of columns into a source list.  If column is included in a foreign-key, make source an outgoing
        spec.
        :param source_list: A column map which will indicate the sources to be included.
        :param positions: where it insert the so
        :return:
        """

        positions = self._normalize_positions({'all'} if positions == {} else positions)
        self.logger.debug('positions: %s', positions)
        self.logger.debug('table: %s sources: %s', self.table.name, [i.spec for i in source_list])
        with DerivaModel(self.table.catalog):
            # Identify any columns that are references to assets and collect up associated columns.
            skip_columns, assets = [], []

            for col in [i.column_name for i in source_list]:
                self.logger.debug('source col %s', col)
                if col == 'pseudo_column':
                    continue
                if chaise_tags.asset in self.table[col].annotations:
                    assets.append(col)
                    skip_columns.extend(self.table[col][chaise_tags.asset].values())

            sources = {}
            try:
                s = self.table.annotations[self.tag]
            except KeyError:
                s = self.table.annotations[self.tag] = {}
            for context, context_list in s.items():
                if DerivaContext(context) not in positions.keys():
                    continue

                # Get list of column names that are in the spec, mapping back simple FK references.
                source_specs = [DerivaSourceSpec(self.table, i) for i in source_list]
                new_context = [DerivaSourceSpec(self.table, i).spec for i in context_list]
                self.logger.debug('getting source names %s %s', source_specs, new_context)

                for source in source_specs:
                    self.logger.debug('source: %s %s', source.spec, source.spec in new_context)
                    if (context == 'entry' and source.column_name in skip_columns) or source.spec in new_context:
                        # Skip over asset columns in entry context and make sure we don't have repeat column specs.
                        continue
                    new_context.append(source.spec)

                sources[context] = new_context
            self.logger.debug('updated sources: %s', sources)
            sources = self._reorder_sources(sources, positions)
            self.logger.debug('reordered sources: source:%s',sources)
            # All is good, so update the visible columns annotation.
            self.logger.debug('updated sources: source:%s \nannotations: %s', sources, self.table.annotations[self.tag])
            self.table.annotations[self.tag] = {**self.table.annotations[self.tag], **sources}
            self.logger.debug('updated sources: %s', self.table.annotations[self.tag])

    def rename_columns(self, column_map, validate=True):
        """
        Go through a list of visible specs and rename the spec, returning a new visible column spec.
        :param column_map:
        :return:
        """
        self.logger.debug('column_map %s %s', column_map, pprint.pformat(self.table.annotations[self.tag]))
        vc = {
            k: [
                j for i in v for j in (
                    [i] if (DerivaSourceSpec(self.table, i, validate=validate).rename_column(column_map) == i)
                    else [DerivaSourceSpec(self.table, i, validate=validate).rename_column(column_map)]
                )
            ] for k, v in self.table.annotations[self.tag].items()
        }
        return vc

    def copy_visible_source(self, from_context):
        pass

    def make_outbound(self, columns, contexts=[], validate=True):
        self.logger.debug('tag: %s columns: %s vc before %s', self.tag, columns, self.table.annotations[self.tag])
        context_names = [i.value for i in (DerivaContext if contexts == [] else contexts)]
        columns = [columns] if isinstance(columns, str) else columns
        for context, vc_list in self.table.annotations[self.tag].items():
            # Get list of column names that are in the spec, mapping back simple FK references.
            if context not in context_names:
                continue
            for s in vc_list:
                print('s', s)
                for col in columns:
                    if {'source': col} == s:
                        s.make_outbound(validate)

    def make_column(self, columns, contexts=[], validate=True):
        self.logger.debug('tag: %s columns: %s vc before %s', self.tag, columns, self.table.annotations[self.tag])
        context_names = [i.value for i in (DerivaContext if contexts == [] else contexts)]
        columns = [columns] if isinstance(columns, str) else columns
        for context, vc_list in self.table.annotations[self.tag].items():
            # Get list of column names that are in the spec, mapping back simple FK references.
            if context not in context_names:
                continue
            for s in vc_list:
                for col in columns:
                    if {'source': col} == s:
                        s.make_column(validate)

    def delete_visible_source(self, columns, contexts=[]):
        self.logger.debug('tag: %s columns: %s vc before %s', self.tag, columns, self.table.annotations[self.tag])
        context_names = [i.value for i in (DerivaContext if contexts == [] else contexts)]
        columns = [columns] if isinstance(columns, str) else columns
        for context, vc_list in self.table.annotations[self.tag].items():
            # Get list of column names that are in the spec, mapping back simple FK references.
            if context not in context_names:
                continue
            for col in columns:
                col_spec = DerivaSourceSpec(self.table, col)
                self.logger.debug('checking %s %s %s', col, col_spec, vc_list)
                if col_spec.spec in vc_list:
                    self.logger.debug('deleting %s', col)
                    vc_list.remove(col_spec.spec)
        self.logger.debug('vc after %s', self.table.annotations[self.tag])

    def reorder_visible_source(self, positions):
        vc = self._reorder_sources(self.table.get_annotation(self.tag), positions)
        self.table.set_annotation(self.tag, {**self.table.get_annotation(self.tag), **vc})

    def _reorder_sources(self, sources, positions):
        """
        Reorder the columns in a visible columns specification.  Order is determined by the positions argument. The
        form of this is a dictionary whose elements are:
            context: {key_column: column_list, key_column:column_list}
        The columns in the specified context are then reorded so that the columns in the column list follow the column
        in order.  Key column specs are processed in order specified. The context name 'all' can be used to indicate
        that the order should be applied to all contexts currently in the visible_columns annotation.  The context name
        can also be omitted an positions can be in the form of {key_column: columnlist, ...} and the context all is
        implied.

        :param sources:
        :param positions:
        :return:
        """

        if positions == {}:
            return sources

        # Set up positions to apply to all contexts if you have {key_column: column_list} form.
        positions = self._normalize_positions(positions)
        new_sources = {}
        for context, source_list in sources.items():
            deriva_context = DerivaContext(context)
            if deriva_context not in positions.keys():
                continue

            # Get the list of column names for the spec.
            source_names = []
            for i in range(len(source_list)):
                name = DerivaSourceSpec(self.table, source_list[i]).column_name
                source_names.append(name + str(i) if name == 'pseudo_column' else name)

            self.logger.debug('source_names %s',source_names)
            # Now build up a map that has the indexes of the reordered columns.  Include the columns in order
            # Unless they are in the column_list, in which case, insert them immediately after the key column.
            reordered_names = source_names[:]

            for key_col, column_list in positions[deriva_context].items():
                if not (set(column_list + [key_col]) <= set(source_names)):
                    raise DerivaCatalogError(self, 'Invalid position specification in reorder columns')
                mapped_list = [j for i in reordered_names if i not in column_list
                               for j in [i] + (
                                   column_list
                                   if i == key_col
                                   else []
                               )
                               ]
                reordered_names = mapped_list

            new_sources[context] = [source_list[source_names.index(i)] for i in reordered_names]
        return {**sources, **new_sources}


class DerivaVisibleColumns(DerivaVisibleSources):
    def __init__(self, table):
        super().__init__(table, chaise_tags.visible_column)


class DerivaVisibleForeignKeys(DerivaVisibleSources):
    def __init__(self, table):
        super().__init__(table, chaise_tags.visible_foreign_keys)


class DerivaSourceSpec(DerivaLogging):
    def __init__(self, table, spec, validate=True):
        super().__init__()
        self.logger.debug('table: %s spec: %s', table.name, spec)
        self.table = table
        self.spec = spec.spec if isinstance(spec, DerivaSourceSpec) else self._normalize_source_spec(spec)
        self.logger.debug('normalized: %s', self.spec)
        if validate:
            self.validate()
        self.logger.debug('initialized: table %s spec: %s', table.name, self.spec)

    def __str__(self):
        return pprint.pformat(self.spec)

    @property
    def column_name(self):
        return self._referenced_columns()

    @property
    def source(self):
        return self.spec['source']

    @source.setter
    def source(self, value):
        self.spec['source'] = value

    def source_type(self):
        if type(self.source) is str:
            return 'column'
        elif isinstance(self.source, (list, tuple)) and len(self.source) == 2:
            if 'inbound' in self.source[0]:
                return 'inbound'
            elif 'outbound' in self.source[0]:
                return 'outbound'
        return None

    def validate(self):
        """
        Check the values of a normalized spec and make sure that all of the columns and keys in the source exist.
        :return:
        """
        spec = self._normalize_source_spec(self.spec)
        source_entry = spec['source']
        if type(spec['source']) is str:
            if spec['source'] not in [i.name for i in self.table.columns]:
                raise DerivaSourceError(self, 'Invalid source entry {}'.format(spec))
        else:
            # We have a path of FKs so follow the path to make sure that all of the constraints line up.
            path_table = self.table
            for c in source_entry[0:-1]:
                if 'inbound' in c and len(c['inbound']) == 2:
                    self.logger.debug('validating %s: %s', path_table.name, c)
                    path_table = path_table.referenced_by[c['inbound']].table
                elif 'outbound' in c and len(c['outbound']) == 2:
                    self.logger.debug('validating %s: %s', path_table.name, c)
                    path_table = path_table.foreign_keys[c['outbound']].referenced_table
                else:
                    raise DerivaSourceError(self, 'Invalid source entry {}'.format(c))

            if source_entry[-1] not in path_table.keys:
                raise DerivaSourceError(self, 'Invalid source entry {}'.format(source_entry[-1]))
        return spec

    def _normalize_source_spec(self, spec):
        """
        Convert a source spec into a uniform form using the new source notations.
        :param spec:
        :return:
        """
        self.logger.debug('normalize_source_spec %s %s', self.table.name, spec)
        if type(spec) is str:
            if spec in self.table.columns:
                spec = {'source': spec}
            elif spec in self.table.foreign_keys:
                spec = {'source': [{'outbound': (self.table.schema_name, spec)}, 'RID']}
            elif spec in self.table.referenced_by:
                spec = {'source': [{'inbound': (self.table.schema_name, spec)}, 'RID']}
            else:
                raise DerivaSourceError(self, 'Invalid source entry {}'.format(spec))
        # Check for old style foreign key notation and turn into inbound or outbound source.
        elif isinstance(spec, (tuple, list)) and len(spec) == 2 and spec[0] == self.table.schema_name:
            if spec[1] in self.table.keys:
                return {'source': next(iter(self.table.keys[spec[1]].columns)).name}
            elif spec[1] in self.table.foreign_keys:
                return {'source': [{'outbound': spec}, 'RID']}
            elif spec[1] in self.table.referenced_by:
                return {'source': [{'inbound': spec}, 'RID']}
            else:
                raise DerivaSourceError(self, 'Invalid source entry {}'.format(spec))
        else:
            # We have a spec that is already in source form.
            # every element of pseudo column source except the last must be either an inbound or outbound spec.
            if not (isinstance(spec['source'], str) or
                    all(map(lambda x: len(x.get('inbound',x.get('outbound',[]))) == 2, spec['source'][0:-1]))):
                raise DerivaSourceError(self, 'Invalid source entry is not in key list{}'.format(spec))
        return spec

    def rename_column(self, column_map):
        self.logger.debug('spec: %s map %s ', self.spec, column_map)
        if self.column_name != 'pseudo_column':
            if self.column_name in column_map:
                # See if the column is used as a simple foreign key.
                try:

                    fkey_name = self.table.foreign_keys[self.source].name
                    self.logger.debug('column %s foreign key name %s %s', self.source, fkey_name, fkey_name in column_map)
                except DerivaCatalogError:
                    fkey_name = None

                # If we are renaming a column, and it is used in a foreign_key, then make the spec be a outbound
                # source using the FK.  Otherwise, just rename the column in the spec if needed.
                if fkey_name and fkey_name in column_map:
                    return {'source':
                        [
                            {'outbound': (column_map[fkey_name].referenced_table.schema_name,
                                          column_map[fkey_name].name)}, 'RID'
                        ]
                    }
                elif self.column_name in column_map:
                    return {'source': column_map[self.column_name]}
                else:
                    self.logger.debug('mapping source , to %s ', {**self.spec, **{'source': column_map[self.source].name}})
                    return {**self.spec, **{'source': column_map[self.source].name}}
            else:
                return self.spec
        else:
            # We have a list of  inbound/outbound specs.  Go through the list and replace any names that are in the map.
            return {
                **self.spec,
                **{'source': [
                                 {next(iter(s)): column_map[next(iter(s.values()))].name} if next(iter(s)) in column_map
                                 else s
                                 for s in self.source[:-1]] + self.source[-1:]
                   }
            }

    def _referenced_columns(self):
        """
        Return the column name that is referenced in the source spec.
        If the spec is a a path then return the value pseudo_column.  If it is a single
        This will require us to look up the column behind an outbound foreign key reference. If
        :return:
        """

        if type(self.source) is str:
            return self.source
        elif len(self.source) == 2 and 'outbound' in self.source[0]:
                t = self.source[0]['outbound'][1]
                fk_cols = self.table.foreign_key(t).columns
                self.logger.debug('fk_cols %s %s', fk_cols, list(fk_cols)[0])
                return list(fk_cols)[0] if len(fk_cols) == 1 else None
        else:
            return 'pseudo_column'

    def make_outbound(self, validate=True):
        col_name = self.table.foreign_key(self.source).name
        self.spec.update(self._normalize_source_spec([self.table.schema_name, col_name]))
        print(self.source)
        if validate:
            self.validate()
        return self

    def make_column(self, validate=True):
        """
        Convert a outbound spec on a foreign key to a column spec.
        :param validate:
        :return:
        """
        # Get the fk_name from the spec and then change spec to be the key column.
        fk_name = self.source[0]['outbound'][1]
        self.spec.update(self._normalize_source_spec(
            next(iter(self.table.foreign_keys[fk_name].columns)).name
        )
        )

        if validate:
            self.validate()
        return self

class DerivaColumn(DerivaCore):
    class _DerivaColumnDef(DerivaLogging):
        def __init__(self, name, type, nullok=True, default=None, fill=None, comment=None, acls={},
                     acl_bindings={}, annotations={}):
            super().__init__()

            self.name = name
            self.type = type if isinstance(type, em.Type) else em.builtin_types[type]
            self.nullok = nullok
            self.default = default
            self.fill = fill
            self.comment = comment
            self.acls = acls
            self.acl_bindings = acl_bindings
            self.annotations = annotations

        def definition(self):
            return em.Column.define(
                self.name,
                self.type,
                nullok=self.nullok,
                default=self.default,
                comment=self.comment,
                acls=self.acls,
                acl_bindings=self.acl_bindings,
                annotations=self.annotations
            )

    def __init__(self, table, name, type=type,
                 nullok=True, default=None, fill=None, comment=None,
                 acls={}, acl_bindings={},
                 annotations={}, define=False
                 ):
        """
        The
        Column name already exists in table. If so the existing definition is used.
        Column name doesn't exist, if so other arguments are used to initialize the columns def.
        :param table: DerivaTable object, or None if the table is being defined along with the class.
        :param name: Name of the column.  If a em.Column is passed in as a name, then its name is used.
        """

        super().__init__(table.catalog if table else None)

        self.logger.debug('table: %s name: %s type: %s', table.name if table else "None", name, type)

        if isinstance(name, em.Column):  # We are providing a em.Column as the name argument.
            name = name.name

        self.column = None
        self.table = table

        if table:
            self.schema = self.catalog[table.schema_name]
            with DerivaModel(self.catalog) as m:
                self.column = m.column_exists(table, name)

        if self.column:
            if define:
                raise DerivaCatalogError(self, 'Column already exists: {}'.format(name))
            else:
                return
        else:
            if define:
                self.column = DerivaColumn._DerivaColumnDef(name, type,
                                                            nullok=nullok, default=default, fill=fill,
                                                            comment=comment,
                                                            acls=acls, acl_bindings=acl_bindings,
                                                            annotations=annotations)
            else:
                raise DerivaCatalogError(self, 'Column does not exist: {}'.format(name)
                )

    @classmethod
    def define(cls, name, type,
               nullok=True, default=None, fill=None,
               comment=None,
               acls={}, acl_bindings={},
               annotations={}):
        return DerivaColumn(None, name, type, nullok=nullok, default=default, fill=fill, comment=comment,
                            acls=acls, acl_bindings=acl_bindings, annotations=annotations, define=True)

    @staticmethod
    def convert_def(table, column_def):
        return DerivaColumn(table,
                            column_def.name, column_def.type,
                            column_def.nullok, column_def.default,
                            comment=column_def.comment,
                            acls=column_def.acls, acl_bindings=column_def.acl_bindings,
                            annotations=column_def.annotations
                            )

    @property
    def name(self):
        return self.column.name

    @property
    def type(self):
        return self.column.type

    @type.setter
    def type(self, type_value):
        if isinstance(self.column, DerivaColumn._DerivaColumnDef):
            self.column.type = em.builtin_types[type_value]
        else:
            raise DerivaCatalogError(self, 'Cannot alter defined column type')

    @property
    def nullok(self):
        return self.column.nullok

    @nullok.setter
    def nullok(self, nullok):
        if isinstance(self.column, DerivaColumn._DerivaColumnDef):
            self.column.nullok = nullok
        else:
            raise DerivaCatalogError(self, 'Cannot alter defined column type')

    @property
    def default(self):
        return self.column.default

    @property
    def fill(self):
        return self.column.fill if isinstance(self.column, DerivaColumn._DerivaColumnDef) else None

    @property
    def comment(self):
        return self.column.comment

    @comment.setter
    def comment(self, comment):
        if isinstance(self.column, DerivaColumn._DerivaColumnDef):
            self.column.comment = comment
        else:
            raise DerivaCatalogError(self, 'Cannot alter defined column type')

    @property
    def acls(self):
        return self.column.acls

    @acls.setter
    def acls(self, acls):
        with DerivaModel(self.table.catalog) as m:
            m.column_model(self).acls.update(acls)

    @property
    def acl_bindings(self):
        return self.column.acl_bindings

    @acl_bindings.setter
    def acl_bindings(self, item):
        with DerivaModel(self.catalog) as m:
            m.column_model(self).acl_bindings.update(item)

    def __str__(self):
        return '\n'.join(
            [
                '{}: {}'.format(self.name, self.type.typename),
                '\tnullok: {} default: {}'.format(self.nullok, self.default),
                '\tcomment: {}'.format(self.comment),
                '\tacls: {}'.format(self.acls),
                '\tacl_bindings: {}'.format(self.acl_bindings)
            ]
        )

    def definition(self):
        # Column will either be a DerivaColumn or an ermrest column.
        try:
            return self.column.definition()
        except AttributeError:
            return self.column

    def update_table(self, table):
        if self.table:
            return
        self.catalog = table.catalog
        self.table = table
        self.schema = self.catalog[table.schema_name]

    def create(self):
        with DerivaModel(self.table.catalog) as m:
            self.column = m.table_model(self.table).create_column(self.catalog.ermrest_catalog,
                                                                  self.column.definition())

    def delete(self):
        # TODO Need to check to make sure column is not in compound key or refered to by a FK.
        self.table._check_composite_keys([self.name])
        self.table.visible_columns.delete_visible_source(self.name)
        with DerivaModel(self.table.catalog) as m:
            m.column_model(self).delete(self.catalog.ermrest_catalog, m.table_model(self.table))
            self.column = None



class DerivaKey(DerivaCore):
    class _DerivaKeyDef(DerivaLogging):
        def __init__(self,
                     table,
                     columns,
                     name=None,
                     comment=None,
                     annotations={}):

            super().__init__()

            self.unique_columns = columns
            self.name = name
            self.comment = comment
            self.annotations = annotations

            self.update_name(table)

        def definition(self, key):
            return em.Key.define(
                self.unique_columns,
                constraint_names=[(key.table.schema_name, self.name)],
                comment=self.comment,
                annotations=self.annotations
            )

        def update_name(self, table):
            if not self.name and table:
                self.name = '{}_'.format(table.name) + '_'.join([i for i in self.unique_columns] + ['key'])

    def __init__(self, table, columns, name=None, comment=None, annotations={}, define=False):
        """

        :param table: Table in which this key exists
        :param name: Either the name of the key or the unique columns in the key.
        """
        super().__init__(table.catalog if table else None)
        self.logger.debug('table %s columns %s', table.name if table else "none", columns)

        if isinstance(name, em.Key):  # We are providing a em.Key as the name argument.
            name = name.names[0]
        if isinstance(columns, em.Key):  # We are providing a em.Key as the columns argument.
            name = columns.names[0]

        # Get just the name part of the potential (schema,name) pair
        if isinstance(name, tuple) and len(name) == 2 and name[0] == table.schema_name:
            name = name[1]

        self.key = None
        self.table = table

        if table:
            self.schema = self.catalog[table.schema_name]
            with DerivaModel(self.catalog) as m:
                self.key = m.key_exists(table, name if name else columns)

        # If we are defining a Key and it already exists, then we have an error, otherwise, we are done.
        if self.key:
            if define:
                raise DerivaCatalogError(self, 'Key already exists: {}'.format(columns))
            else:
                return
        else:
            if define:
                # We are creating a new key, so create a DerivaKeyDef
                self.key = DerivaKey._DerivaKeyDef(table, columns,
                                                   name=name, comment=comment, annotations=annotations)
            else:
                raise DerivaCatalogError(self, "Key doesn't exist {}".format(name))

    @staticmethod
    def define(columns, name=None, comment=None, annotations={}):
        return DerivaKey(None, columns, name, comment, annotations, define=True)

    @staticmethod
    def convert_def(table, key_def):
        if not isinstance(key_def, em.Key):
            raise DerivaCatalogError(table.catalog, 'convert_def must have em.Key as an argument')
        return DerivaKey(table,
                         key_def.unique_columns,
                         name=key_def.names[0],
                         comment=key_def.comment, annotations=key_def.annotations)

    def update_table(self, table):
        if self.table:
            return
        self.catalog = table.catalog
        self.table = table
        self.schema = self.catalog[table.schema_name]
        self.key.update_name(table)

    @property
    def name(self):
        try:
            return self.key.name
        except AttributeError:
            return self.key.names[0][1]

    def _key_column(self, column_name):
        return DerivaColumn(self.table, column_name)

    @property
    def columns(self):
        with DerivaModel(self.catalog) as m:
            return ElementList(self._key_column, m.key_model(self).unique_columns)

    @property
    def comment(self):
        return self.key.comment

    @comment.setter
    def comment(self, comment):
        if isinstance(self.key, DerivaKey._DerivaKeyDef):
            self.key.comment = comment
        else:
            raise DerivaCatalogError(self, 'Cannot alter defined key type')

    def __str__(self):
        return '{name}:{columns}\n\tcomment: {comment}\n\tannotations: {annotations}'.format(
            name=self.name, columns=[i.name for i in self.columns],
            comment=self.comment, annotations=[a for a in self.annotations])

    def create(self):
        with DerivaModel(self.table.catalog) as m:
            self.key = m.table_model(self.table).create_key(self.catalog.ermrest_catalog, self.key.definition(self))

    def delete(self):
        with DerivaModel(self.table.catalog) as m:
            m.key_model(self).delete(self.catalog.ermrest_catalog, m.table_model(self.table))
        self.table = None
        self.key = None

    def definition(self):
        # Key will either be a DerivaKey or an ermrest key.
        return self.key.definition(self)
        try:
            return self.key.definition(self)
        except AttributeError:
            return self.key


class DerivaForeignKey(DerivaCore):
    class _DerivaForeignKeyDef:
        def __init__(self,
                     table, columns,
                     referenced_table, referenced_columns,
                     name=None,
                     comment=None,
                     on_update='NO ACTION',
                     on_delete='NO ACTION',
                     acls={},
                     acl_bindings={},
                     annotations={}):

            self.name = name
            self.columns = columns
            self.referenced_table = referenced_table
            self.referenced_columns = referenced_columns
            self.comment = comment
            self.on_update = on_update
            self.on_delete = on_delete
            self.acls = acls
            self.acl_bindings = acl_bindings
            self.annotations = annotations

            self.update_name(table)
            fk_ops = ['CASCADE', 'DELETE', 'RESTRICT', 'NO ACTION', 'SET NULL']

            if on_update not in fk_ops or on_delete not in fk_ops:
                raise ValueError('Invalid value for on_update/on_delete {} {}'.format(on_update, on_delete))

        def definition(self, fkey):
            return em.ForeignKey.define(
                self.columns,
                self.referenced_table.schema_name, self.referenced_table.name, self.referenced_columns,
                constraint_names=[(fkey.table.schema_name, self.name)],
                comment=self.comment,
                on_update=self.on_update,
                on_delete=self.on_delete,
                acls=self.acls,
                acl_bindings=self.acl_bindings,
                annotations=self.annotations
            )

        def update_name(self, table):
            if not self.name and table:
                self.name = '{}_'.format(table.name) + '_'.join([i for i in self.columns] + ['fkey'])

    def __init__(self, table, columns=None,
                 dest_table=None, dest_columns=None,
                 name=None,
                 comment=None,
                 on_update='NO ACTION',
                 on_delete='NO ACTION',
                 acls={},
                 acl_bindings={},
                 annotations={},
                 define=False):
        """"
        Create a DerivaForeignKey object from an existing ERMrest FKey, or initalize an object for a key to be created
            at some point in the future.

        :param table: Table in which this key exists
        :param: dest_table: A DerivaTable that is the target of this FK.
        :param name: Either the name of the key, an existing ERMrest FK or the unique columns in the key.
        :param define:
        """
        super().__init__(table.catalog if table else None)
        self.logger.debug('%s %s %s', table.name if table else None, columns, define)

        if isinstance(name, em.ForeignKey):  # We are providing a em.ForeignKey as the name argument.
            name = name.names[0]

        if isinstance(columns, em.ForeignKey):  # We are providing a em.ForeignKey as the columns argument.
            name = columns.names[0]

        # Get just the name part of the potential (schema,name) pair
        if isinstance(name, (tuple,list)) and len(name) == 2 and name[0] == table.schema_name:
            name = name[1]

        self.fkey = None
        self.table = table

        # We may not have a table yet for this FK if we are creating it as part of creating a table.
        if table:
            self.schema = self.catalog[table.schema_name]
            with DerivaModel(self.catalog) as m:
                self.fkey = m.foreign_key_exists(table, name if name else columns)

        # If we are defining a FK and it already exists, then we have an error, otherwise, we are done.
        if self.fkey:
            if define:
                raise DerivaCatalogError(self, 'Foreign Key already exists'.format(columns))
            else:
                return

        # We are defining a new FK, so create a DerivaForeignKeyDef to hold the info until the key is created.
        logging.debug('creating fkey def {}', define)
        if define:
            self.fkey = DerivaForeignKey._DerivaForeignKeyDef(table, columns,
                                                              dest_table, dest_columns,
                                                              name=name, comment=comment,
                                                              on_update=on_update, on_delete=on_delete,
                                                              acls=acls, acl_bindings=acl_bindings,
                                                              annotations=annotations)
        else:
            raise DerivaCatalogError(self, "Foreign key doesn't exist".format(name))
        self.logger.debug('fkey def %s', self.fkey)

    @staticmethod
    def define(
            columns,
            dest_table, dest_columns,
            name=None,
            comment=None,
            on_update='NO ACTION',
            on_delete='NO ACTION',
            acls={},
            acl_bindings={},
            annotations={}
    ):
        return DerivaForeignKey(None, columns, dest_table, dest_columns,
                                name=name,
                                comment=comment,
                                on_update=on_update, on_delete=on_delete,
                                acls=acls, acl_bindings=acl_bindings,
                                annotations=annotations,
                                define=True)

    @staticmethod
    def convert_def(table, fkey_def: em.ForeignKey):
        if not isinstance(fkey_def, em.Key):
            raise DerivaCatalogError(table.catalog, 'convert_def must have em.ForeignKey as an argument')
        c = fkey_def.referenced_columns[0]
        dest_table = table.catalog[c['schema_name']][c['table_name']]
        return DerivaForeignKey(table, fkey_def.foreign_key_columns,
                                dest_table, dest_columns=fkey_def.referenced_columns,
                                name=fkey_def.names[0][1],
                                comment=fkey_def.comment,
                                on_update=fkey_def.on_update, on_delete=fkey_def.on_delete,
                                acls=fkey_def.acls, acl_bindings=fkey_def.acl_bindings,
                                annotations=fkey_def.annotations)

    def update_table(self, table):
        if self.table:
            return
        self.catalog = table.catalog
        self.table = table
        self.schema = self.catalog[table.schema_name]
        self.fkey.update_name(table)

    @property
    def name(self):
        try:
            return self.fkey.name
        except AttributeError:
            return self.fkey.names[0][1]

    def _key_column(self, column_name):
        self.logger.debug('column_name %s', column_name)
        # May have a emrest column name, or just the name of the column
        return DerivaColumn(
            self.table,
            column_name if isinstance(column_name, str) else column_name['column_name'])

    @property
    def columns(self):
        with DerivaModel(self.catalog) as m:
            self.logger.debug('%s %s', m.foreign_key_model(self), m.foreign_key_model(self).foreign_key_columns)
            return ElementList(self._key_column, m.foreign_key_model(self).foreign_key_columns)

    @property
    def referenced_table(self):
        try:
            return self.fkey.referenced_table
        except AttributeError:
            src_schema = self.fkey.referenced_columns[0]['schema_name']
            src_table = self.fkey.referenced_columns[0]['table_name']
            return self.table.catalog[src_schema][src_table]

    def _referenced_column(self, column_name):
        # May have a emrest column name, or just the name of the column
        return DerivaColumn(
            self.referenced_table,
            column_name if isinstance(column_name, str) else column_name['column_name'])

    @property
    def referenced_columns(self):
        with DerivaModel(self.catalog) as m:
            return ElementList(self._referenced_column, m.foreign_key_model(self).referenced_columns)
            return ElementList(self._referenced_column, m.foreign_key_model(self).referenced_columns)

    @property
    def acls(self):
        return self.fkey.acls

    @acls.setter
    def acls(self, acls):
        with DerivaModel(self.table.catalog) as m:
            m.foreign_key_model(self).acls.update(acls)

    @property
    def acl_bindings(self):
        return self.fkey.acl_bindings

    @acl_bindings.setter
    def acl_bindings(self, item):
        with DerivaModel(self.catalog) as m:
            m.foreign_key_model(self).acl_bindings.update(item)

    @property
    def comment(self):
        return self.fkey.comment

    @comment.setter
    def comment(self, comment):
        if isinstance(self.fkey, DerivaKey):
            self.fkey.comment = comment
        else:
            raise DerivaCatalogError(self, 'Cannot alter defined key type')

    def __str__(self):
        return '\n'.join([
            '{}: {}'.format(self.name, self.columns),
            '\tcomment: {}'.format(self.comment),
            '\tannotations: {}'.format(self.annotations)
        ])

    def create(self):
        with DerivaModel(self.table.catalog) as m:
            self.logger.debug('creating fkey %s', self.definition)
            self.fkey = m.table_model(self.table).create_fkey(self.catalog.ermrest_catalog, self.definition())

    def delete(self):
        referenced_table = self.referenced_table
        column = next(iter(self.columns)) if len(self.columns) == 1 else False
        self.logger.debug('demoting visible column %s', column)

        referenced_table.visible_foreign_keys.delete_visible_source(self.name)
        del (referenced_table.referenced_by[self.name])

        with DerivaModel(self.table.catalog) as m:
            m.foreign_key_exists(self.table, self.name).delete(self.catalog.ermrest_catalog, m.table_model(self.table))

        if column:
            self.table.visible_columns = self.table.visible_columns.make_column(column.name, validate=False)

        self.table = None
        self.fkey = None

    def definition(self):
        # Key will either be a DerivaForeignKey or an ermrest fkey.
        try:
            return self.fkey.definition(self)
        except AttributeError:
            return self.fkey


class DerivaTable(DerivaCore):
    def __init__(self, catalog, schema_name, table_name):
        super().__init__(catalog)
        self.schema = catalog[schema_name]
        self.table = self
        self.schema_name = schema_name
        self._table_name = table_name
        self.deleted = False

    def __getitem__(self, column_name):
        return self.column(column_name)

    def __iter__(self):
        return self.columns.__iter__()

    def __str__(self):
        return '\n'.join([
            'Table {}'.format(self.name),
            tabulate.tabulate(
                [[i.name, i.type.typename, i.nullok, i.default] for i in self.columns],
                headers=['Name', 'Type', 'NullOK', 'Default']
            ),
            '\n',
            'Keys:',
            tabulate.tabulate([[i.name, [c.name for c in i.columns]] for i in self.keys],
                              headers=['Name', 'Columns']),
            '\n',
            'Foreign Keys:',
            tabulate.tabulate(
                [[i.name, [c.name for c in i.columns], '->',
                  i.referenced_table.name, [c.name for c in i.referenced_columns]]
                 for i in self.foreign_keys],
                headers=['Name', 'Columns', '', 'Referenced Table', 'Referenced Columns']),
            '\n\n',
            'Referenced By:',
            tabulate.tabulate(
                [
                    [i.name,
                     [c.name for i in self.referenced_by for c in i.referenced_columns],
                     '<-',
                     '{}:{}:'.format(i.referenced_table.schema_name,
                                     i.referenced_table.name),
                     [c.name for c in i.columns]
                     ]
                    for i in self.referenced_by],
                headers=['Name', 'Columns', '', '', 'Referenced Columns'])
        ]
        )

    @property
    def name(self):
        return self._table_name

    @property
    def comment(self):
        with DerivaModel(self.catalog) as m:
            return m.table_model(self).comment

    @comment.setter
    def comment(self, value):
        with DerivaModel(self.catalog) as m:
            m.table_model(self).comment = value

    @property
    def acls(self):
        with DerivaModel(self.catalog) as m:
            return DerivaDictValue(m.table_model(self).acls)

    @property
    def acl_bindings(self):
        with DerivaModel(self.catalog) as m:
            m.schema_model(self)
            return DerivaDictValue(m.table_model(self).acl_bindings)

    @property
    def visible_columns(self):
        return DerivaVisibleSources(self, chaise_tags.visible_columns)

    @visible_columns.setter
    def visible_columns(self, vcs):
        self.annotations[chaise_tags.visible_columns] = vcs

    @property
    def visible_foreign_keys(self):
        return DerivaVisibleSources(self, chaise_tags.visible_foreign_keys)

    @visible_foreign_keys.setter
    def visible_foreign_keys(self, keys):
        self.annotations[chaise_tags.visible_foreign_keys] = keys

    @property
    def columns(self):
        with DerivaModel(self.catalog) as m:
            return ElementList(self.column, m.table_model(self).column_definitions)

    @property
    def keys(self):
        with DerivaModel(self.catalog) as m:
            return ElementList(self.key, m.table_model(self).keys)

    def foreign_key(self, fkey_name):
        self.logger.debug('%s', fkey_name)
        return DerivaForeignKey(self.catalog[self.schema_name][self.name], name=fkey_name)

    @property
    def foreign_keys(self):
        with DerivaModel(self.catalog) as m:
            return ElementList(self.foreign_key, m.table_model(self).foreign_keys)

    @property
    def referenced_by(self):
        with DerivaModel(self.catalog) as m:
            return ElementList(lambda x: self._referenced(x, m.table_model(self).referenced_by),
                               m.table_model(self).referenced_by)

    def _referenced(self, fkey_id, referenced_by):
        """
        Return the DerivaForeignKey associated with fk_id.  The Referenced_by list is different in that it is
        keys ih other tables which that the current table as the target.  We will name the FK we are interested
        in by providing eather 1) the name of that FK, 2) the columns in the current table that are being referenced
        or 3) a ERMrest Foreign Key.  In order to reassociate the key with the source table, we need to look into
        the list of referenced_by keys and search for the schema so we get the table that the key is in.
        :param fk:
        :return:
        """
        self.logger.debug('fkey_id: %s referenced_by: %s', fkey_id, referenced_by)
        fkey = None
        if isinstance(fkey_id, em.ForeignKey):
            fkey = fkey_id
        else:
            # We have either a constraint name or a column name.
            try:
                # See if we already have akey name
                fkey = referenced_by[tuple(fkey_id)]
            except (KeyError, TypeError):
                for schema in self.catalog:
                    try:
                        fkey = referenced_by[(schema.name, fkey_id)]
                    except (TypeError, KeyError):
                        continue
        if not fkey:
            # Only remaining possiblity is that we have been given columns names
            cols = set([fkey_id] if isinstance(fkey_id, str) else fkey_id)
            for fk in referenced_by:
                if (self.schema_name == fk.referenced_columns[0]['schema_name'] and
                        self.name == fk.referenced_columns[0]['table_name'] and
                        cols == {i['column_name'] for i in fk.referenced_columns}):
                    fkey = fk
                    break
        if not fkey:
            raise DerivaCatalogError(self, 'referenced by requires name or key type: {}'.format(fkey_id))

        with DerivaModel(self.table.catalog) as m:
            # Now find the schema and table of the referring table
            src_schema = fkey.foreign_key_columns[0]['schema_name']
            src_table = fkey.foreign_key_columns[0]['table_name']
            self.logger.debug('creating fkey... %s', fkey.names[0])
            return DerivaForeignKey(self.table.catalog[src_schema][src_table], fkey)

    def key(self, key_name):
        return DerivaKey(self.catalog[self.schema_name][self.name], key_name)

    def datapath(self):
        return self.catalog.getPathBuilder().schemas[self.schema_name].tables[self.name]

    def _column_names(self):
        return [i.name for i in self.columns]

    def create_key(self, columns, name=None, comment=None, annotations={}):
        key = DerivaKey(self, columns, name, comment, annotations, define=True)
        self.logger.debug('creating key....')
        key.create()

    def column(self, column_name):
        return DerivaColumn(self.catalog[self.schema_name][self.name], column_name)

    def validate(self):
        self.visible_columns().validate()

    def _column_map(self, column_map, dest_table):
        return DerivaColumnMap(self, column_map, dest_table)

    def chaise_uri(self):
        p = urlparse(self.catalog.model.get_server_uri())
        catalog_id = p.path.split('/')[-1]
        print('{}://{}/chaise/recordset/#{}/{}:{}'.format(
            p.scheme, p.hostname, catalog_id, self.schema_name, self.name)
        )

    def entities(self, *attributes, **renamed_attributes):
        return self.datapath().entities(*attributes, **renamed_attributes)

    def create_foreign_key(self,
                           columns, referenced_table, child_columns,
                           name=None,
                           comment=None,
                           on_update='NO ACTION',
                           on_delete='NO ACTION',
                           acls={},
                           acl_bindings={},
                           annotations={},
                           position={}):
        self.logger.debug('columns: %s', columns)
        try:
            fkey = DerivaForeignKey(self, columns,
                                    referenced_table,
                                    child_columns,
                                    comment=comment,
                                    acls=acls,
                                    acl_bindings=acl_bindings,
                                    name=name,
                                    on_update=on_update,
                                    on_delete=on_delete,
                                    annotations=annotations,
                                    define=True)
        except DerivaCatalogError:
            raise DerivaCatalogError(self, 'Invalid arguments for foreign key')
        fkey.create()

        with DerivaModel(self.catalog) as m:
            # Add foreign key to appropriate referenced_by list
            m.table_model(fkey.referenced_table).referenced_by.append((m.foreign_key_model(fkey)))
            _, _, inbound_sources = referenced_table.sources(filter=[fkey.name])
            # Pick out the source for this key:

            self.logger.debug('inbound sources %s', inbound_sources)
            referenced_table.visible_foreign_keys.insert_sources(inbound_sources, position)

            if len(columns) == 1:
                self.visible_columns = self.visible_columns.make_outbound(columns[0])
                self.logger.debug('new vc %s', self.visible_columns)

    def sources(self, merge_outbound=False, filter=None):
        """
        Create source lists from table columns.

        Go through the columns and keys in the current table and create a list of DerivaSourceSpecs for each of them.
        If filter is provided, only the column or key names in the list are examined.
        If merge_outbound is true and a column is used in a simple foreign key, used return an outbound source rather
        then the column source.

        :param merge_outbound: If True and the column is in a simple foreign_key s
        :param filter: List of column or key names to include in the returned source lists.
        :return: A triple of DerivaSourceSpec lists for columns, foreign_keys and incoming foreign_keys.
        """
        def full_key_name(k):
            return (k.table.schema_name, k.name)

        with DerivaModel(self.catalog) as m:
            table = m.table_model(self)

            # Go through the list of foreign keys and create a list of key columns in simple foreign keys
            fkey_names = {
                [c.name for c in fk.columns][0]: fk
                for fk in self.foreign_keys if len(fk.columns) == 1
            }

            # TODO We should check to see if target is vocabulary and if so use ID rather then RID
            column_sources = [
                DerivaSourceSpec(self,
                                 {'source': (
                                     [{'outbound': full_key_name(fkey_names[col.name])}, 'RID']
                                     if col.name in fkey_names and merge_outbound
                                     else col.name
                                 )}
                                 )
                for col in self.table.columns if not filter or col.name in filter
            ]

            outbound_sources = [
                DerivaSourceSpec(self,
                                 {'source': [{'outbound': full_key_name(i)}, 'RID']}) for i in self.table.foreign_keys
                if not filter or i.name in filter]

            inbound_sources = [
                DerivaSourceSpec(self,
                                 {'source': [{'inbound': full_key_name(i)}, 'RID']}) for i in self.table.referenced_by
                if not filter or i.name in filter
            ]

            return column_sources, outbound_sources, inbound_sources

    @staticmethod
    def _rename_columns_in_display(dval, column_map):
        def rename_markdown_pattern(pattern):
            # Look for column names {{columnname}} in the templace and update.
            for k, v in column_map.get_names(column_map):
                pattern = pattern.replace('{{{}}}'.format(k), '{{{}}}'.format(v))
            return pattern

        return {
            k: rename_markdown_pattern(v) if k == 'markdown_name' else v
            for k, v in dval.items()
        }

    def _rename_columns_in_annotations(self, column_map, skip_annotations=[]):
        new_annotations = {}
        for k, v in self.annotations().items():
            if k in skip_annotations:
                renamed = v
            elif k == chaise_tags.display:
                renamed = self._rename_columns_in_display(v, column_map)
            elif k == chaise_tags.visible_columns:
                renamed = self.visible_columns.rename_columns(column_map)
            else:
                renamed = v
            new_annotations[k] = renamed
        return new_annotations

    def _rename_columns_in_acl_bindings(self, _column_map):
        with DerivaModel(self.catalog) as m:
            table = m.table_model(self)
            return table.acl_bindings

    def _rename_columns_in_column_annotations(self, annotation, column_map):
        return annotation

    def _key_in_columns(self, columns, key_columns, rename=False):
        """
        Given a set of columns and a key, return true if the key is in that column set.  If we are simply renaming
        columns, rather then moving them to a new table, not all of the columns in a composite key have to be present.
        :param columns:  List of columns in a table that are being altered
        :param key_columns: list of columns in the key

        :return: True if the key is contained within columns.
        """

        overlap = set(columns).intersection({k.name for k in key_columns})
        # Determine if we are moving the column within the same table, or between tables.
        self.logger.debug('columns %s key_columns %s overlap %s', columns, {k.name for k in key_columns}, overlap)
        if len(overlap) == 0:
            return False
        if not rename and len(overlap) < len(key_columns):
            raise DerivaCatalogError(self, msg='Cannot rename part of compound key {}'.format(key_columns))
        return True

    def _check_composite_keys(self, columns, rename=False):
        """
        Go over all of the keys, incoming and outgoing foreign keys and check to make sure that renaming the set of
        columns colulumns won't break up composite keys if they are renamed.
        :param columns:
        :param rename:
        :return:
        """
        columns = set(columns)

        for i in self.keys:
            self._key_in_columns(columns, i.columns, rename)

        for fk in self.foreign_keys:
            self._key_in_columns(columns, fk.columns, rename)

        for fk in self.referenced_by:
            self._key_in_columns(columns, fk.referenced_columns, rename)

    def _update_key_name(self, name, column_map, dest_table):
        # Helper function that creates a new constraint name by replacing table and column names.
        name = name[1].replace('{}_'.format(self.name), '{}_'.format(dest_table.name))

        for k, v in self._column_map(column_map, 'name').items():
            # Value can be either a column or key name which would have a schema component.
            name = name.replace(k if type(k) is str else k[1], (v if type(v) is str else v[1]))
        return dest_table.schema_name, name

    def _copy_keys(self, column_map):
        """
        Copy over the keys from the current table to the destination table, renaming columns.
        :param column_map:
        :return:
        """

        key_map = column_map.get_keys()
        fkey_map = column_map.get_foreign_keys()

        with DerivaModel(self.catalog):
            for k, key_def in key_map.items():
                key_def.table_model.create_key(key_def)

            for k, fkey_def in fkey_map.items():
                fkey_def.table_model.create_fkey(fkey_def)

    def _delete_columns_in_display(self, annotation, columns):
        raise DerivaCatalogError(self, 'Cannot delete column from display annotation')

    def _delete_columns_from_annotations(self, columns):
        for k, v in self.annotations.items():
            if k == chaise_tags.display:
                self._delete_columns_in_display(v, columns)
            elif k == chaise_tags.visible_columns or k == chaise_tags.visible_foreign_keys:
                DerivaVisibleSources(self, k).delete_visible_source(columns)

    def create_keys(self, keys):
        """
        Create a new column in the table.
        :param keys: A list of DerivaKey.
        :return:
        """
        keys = keys if type(keys) is list else [keys]

        for key in keys:
            key.create()

    def delete_columns(self, columns):
        """
        Drop a column from a table, cleaning up visible columns and keys.
        :param columns:
        :return:
        """
        with DerivaModel(self.catalog) as m:
            table = m.table_model(self)

            self._check_composite_keys(columns, self)
            columns = set(columns)

            # Remove keys...
            for i in table.keys:
                if self._key_in_columns(columns, i.unique_columns, self):
                    i.delete(self.catalog.ermrest_catalog, table)

            for fk in table.foreign_keys:
                fk_columns = [i['column_name'] for i in fk.foreign_key_columns]
                if self._key_in_columns(columns, fk_columns, self):  # We are renaming one of the foreign key columns
                    self.delete_fkeys(fk)

            for column in columns:
                self._delete_columns_from_annotations([column])
                table.column_definitions[column].delete(self.catalog.ermrest_catalog, table)
        return

    def copy_columns(self, column_map, dest_table=None):
        """
        Copy a set of columns, updating visible columns list and keys to mirror source column. The columns to copy
        are specified by a column map.  Column map can be a dictionary with entries SrcCol: DerviaColumnSpec or
        SrcCol:TargetCol.

        :param column_map: a column_map that describes the list of columns.
        :param dest_table: Table name of destination table
        :param column_map: A dictionary that specifies column name mapping
        :return:
        """
        dest_table = dest_table if dest_table else self
        column_map = self._column_map(column_map, dest_table)

        columns = column_map.get_columns()
        column_names = [k for k in column_map.get_columns().keys()]

        with DerivaModel(self.catalog):
            # TODO we need to figure out what to do about ACL binding

            # Make sure that we can rename the columns
            overlap = {v.name for v in columns.values()}.intersection(set(dest_table._column_names()))
            if len(overlap) != 0:
                raise ValueError('Column {} already exists.'.format(overlap))

            self._check_composite_keys(column_names, dest_table)

            # Update visible column spec, putting copied column right next to the source column.
            if dest_table is self:
                positions = {col: [column_map[col].name] for col in column_map.get_columns()}

            self.create_columns([i for i in columns.values()], positions)

            # Copy over the old values
            from_path = self.datapath()
            to_path = dest_table.datapath()

            # Get the values of the columns, and remap the old column names to the new names.
            rows = from_path.entities(
                **{
                    **{val.name: getattr(from_path, col) for col, val in column_map.get_columns().items()},
                    **{'RID': from_path.RID}
                }
            )
            to_path.update(rows)

            # Copy over the keys.
            self._copy_keys(column_map)
        return

    def create_columns(self, columns, positions={}, visible=True):
        """
        Create a new column in the table.
        :param columns: A list of DerivaColumn.
        :param positions:  Where the column should be added into the visible columns spec.
        :param visible: Include this column in the visible columns spec.
        :return:
        """
        column_names = []
        columns = columns if type(columns) is list else [columns]

        for column in columns:
            column.update_table(self)
            column.create()
            column_names.append(column.name)

        if visible:
            sources, _, _ = self.sources(filter=column_names)
            self.visible_columns.insert_sources(sources, positions)

    def rename_column(self, from_column, to_column, default=None, nullok=None):
        """
        Rename a column by copying it and then deleting the origional column.
        :param from_column:
        :param to_column:
        :param default:
        :param nullok:
        :return:
        """
        column_map = {from_column: DerivaColumn(table=self, name=to_column, type=from_column.type, nullok=nullok,
                                                   default=default)}
        self.rename_columns(column_map=column_map)
        return

    def rename_columns(self, column_map, dest_table=None, delete=True):
        """
        Rename a column by copying it and then deleting the origional column.
        :param dest_table:
        :param column_map:
        :param delete:
        :return:
        """
        dest_table = dest_table if dest_table else self
        column_map = self._column_map(column_map, dest_table)

        with DerivaModel(self.catalog) as m:
            self.copy_columns(column_map, dest_table)
            # Update column name in ACL bindings....
            # self._rename_columns_in_acl_bindings(column_map)

            # Update annotations where the old spec was being used. We have already moved over
            # the visible columns, so skip the visible columns annotation.
            m.table_model(self).annotations.update(
                self._rename_columns_in_annotations(column_map, skip_annotations=[chaise_tags.visible_columns])
            )
            if delete:
                columns = [k for k in column_map.get_columns().keys()]
                self.delete_columns(columns)
        return

    def delete(self):
        with DerivaModel(self.catalog) as m:
            model = m.catalog_model()
            table = m.table_model(self)

            if table.referenced_by:
                DerivaCatalogError(self, 'Attept to delete catalog with incoming foreign keys')
            # Now we can delete the table.
            table.delete(self.catalog.ermrest_catalog, schema=model.schemas[self.schema_name])
            self.deleted = True

    def _relink_columns(self, dest_table, column_map):
        with DerivaModel(self.catalog) as m:
            table = m.table_model(self)
            for fkey in table.referenced_by[:]:
                fk_columns = [i['column_name'] for i in fkey.foreign_key_columns]
                referenced_columns = [i['column_name'] for i in fkey.referenced_columns]
                column_name_map = column_map.get_names()
                child_table = self.catalog.schema_model(fkey.foreign_key_columns[0]['schema_name']). \
                    table_model(fkey.foreign_key_columns[0]['table_name'])

                if self._key_in_columns(column_name_map.keys(), referenced_columns, dest_table):
                    child_table.delete_fkeys([fkey])
                    child_table.create_fkey(DerivaForeignKey(
                        child_table,
                        fk_columns,
                        dest_table,
                        [column_name_map.get(i, i) for i in referenced_columns],
                        comment=fkey.comment,
                        acls=fkey.acls,
                        acl_bindings=fkey.acl_bindings,
                        annotations=fkey.annotations
                    ))
            self.catalog.update_referenced_by()

    def copy_table(self, schema_name, table_name, column_map={}, clone=False,
                   key_defs=[],
                   fkey_defs=[],
                   comment=None,
                   acls={},
                   acl_bindings={},
                   annotations={}
                   ):
        """
        Copy the current table to the specified target schema and table. All annotations and keys are modified to
        capture the new schema and table name. Columns can be renamed in the target table by providing a column mapping.
        Key and foreign key definitions can be augmented or overwritten by providing appropriate arguments. Lastly
        if the clone argument is set to true, the RIDs of the source table are reused, so that the equivalent of a
        move operation can be obtained.
        :param schema_name: Target schema name
        :param table_name:  Target table name
        :param column_map: A dictionary that is used to rename columns in the target table.
        :param clone:
        :param key_defs:
        :param fkey_defs:
        :param comment:
        :param acls:
        :param acl_bindings:
        :param annotations:
        :return:
        """
        proto_table = namedtuple('ProtoTable', ['catalog', 'schema_name', 'name'],
                                 (self.catalog, schema_name, table_name))

        with DerivaModel(self.catalog) as m:
            model = m.model()
            table = model.schemas[self.schema_name].tables[self.name]

            # Augment the column_map with entries for columns in the table, but not in the map.
            new_map = {i.name: column_map.get(i.name, i.name) for i in table.column_definitions}
            new_map.update(column_map)
            # Add keys to column map. We need to create a dummy destination table for this call.
            column_map = self._column_map(new_map, proto_table)

            # new_columns = [c['name'] for c in column_defs]

            new_table = self.catalog.schema_model(self.schema_name).create_table(
                table_name,
                # Use column_map to change the name of columns in the new table.
                column_defs=column_map.get_columns().values(),
                key_defs=[i for i in column_map.get_keys().values()] + key_defs,
                fkey_defs=[i for i in column_map.get_foreign_keys().values()] + fkey_defs,
                comment=comment if comment else table.comment,
                acls={**table.acls, **acls},
                acl_bindings={**table.acl_bindings, **acl_bindings},
                annotations=self._rename_columns_in_annotations(column_map)
            )

            # Create new table
            new_table.table_model = table_name
            new_table.schema_model = schema_name

            # Copy over values from original to the new one, mapping column names where required. Use the column_fill
            # argument to provide values for non-null columns.
            pb = self.catalog.getPathBuilder()
            from_path = pb.schemas[self.schema_name].tables[self.name]
            to_path = pb.schemas[schema_name].tables[table_name]

            rows = map(
                lambda x: {**x, **{k: v.fill for k, v in column_map.get_columns().items() if v.fill}},
                from_path.entities(
                    **{column_map.get(i, i).name: getattr(from_path, i) for i in from_path.column_definitions})
            )
            to_path.insert(list(rows), **({'nondefaults': {'RID', 'RCT', 'RCB'}} if clone else {}))
        return new_table

    def move_table(self, schema_name, table_name,
                   delete=True,
                   column_map={},
                   key_defs=[],
                   fkey_defs=[],
                   comment=None,
                   acls={},
                   acl_bindings={},
                   annotations={}
                   ):
        with DerivaModel(self.catalog) as m:
            new_table = self.copy_table(schema_name, table_name, clone=True,
                                        column_map=column_map,
                                        key_defs=key_defs,
                                        fkey_defs=fkey_defs,
                                        comment=comment,
                                        acls=acls,
                                        acl_bindings=acl_bindings,
                                        annotations=annotations)

            # Augment the column_map with entries for columns in the table, but not in the map.
            new_map = {i.name: column_map.get(i.name, i.name) for i in m.table_model(self).column_definitions}
            new_map.update(column_map)
            column_map = self._column_map(new_map, new_table)

            self._relink_columns(new_table, column_map)
            if delete:
                self.delete()
            return new_table

    def create_asset_table(self, key_column,
                           extensions=[],
                           file_pattern='.*',
                           column_defs=[], key_defs=[], fkey_defs=[],
                           comment=None, acls={},
                           acl_bindings={},
                           annotations={},
                           set_policy=True):
        """
        Create a basic asset table and configures the bulk upload annotation to load the table along with a table of
        associated metadata. This routine assumes that the metadata table has already been defined, and there is a key
        associated metadata. This routine assumes that the metadata table has already been defined, and there is a key
        column the metadata table that can be used to associate the asset with a row in the table. The default
        configuration assumes that the assets are in a directory named with the table name for the metadata and that
        they either are in a subdirectory named by the key value, or that they are in a file whose name starts with the
        key value.

        :param key_column: The column in the metadata table to be used to correlate assets with entries. Assets will be
                           named using the key column.
        :param extensions: List file extensions to be matched. Default is to match any extension.
        :param file_pattern: Regex that identified the files to be considered for upload
        :param column_defs: a list of Column.define() results for extra or overridden column definitions
        :param key_defs: a list of Key.define() results for extra or overridden key constraint definitions
        :param fkey_defs: a list of ForeignKey.define() results for foreign key definitions
        :param comment: a comment string for the asset table
        :param acls: a dictionary of ACLs for specific access modes
        :param acl_bindings: a dictionary of dynamic ACL bindings
        :param annotations: a dictionary of annotations
        :param set_policy: If true, add ACLs for self serve policy to the asset table
        :return:
        """

        def create_asset_upload_spec():
            extension_pattern = '^.*[.](?P<file_ext>{})$'.format('|'.join(extensions if extensions else ['.*']))

            return [
                # Any metadata is in a file named /records/schema_name/tablename.[csv|json]
                {
                    'default_columns': ['RID', 'RCB', 'RMB', 'RCT', 'RMT'],
                    'ext_pattern': '^.*[.](?P<file_ext>json|csv)$',
                    'asset_type': 'table',
                    'file_pattern': '^((?!/assets/).)*/records/(?P<schema>%s?)/(?P<table>%s)[.]' %
                                    (self.schema_name, self.name),
                    'target_table': [self.schema_name, self.name],
                },
                # Assets are in format assets/schema_name/table_name/correlation_key/file.ext
                {
                    'checksum_types': ['md5'],
                    'column_map': {
                        'URL': '{URI}',
                        'Length': '{file_size}',
                        self.name: '{table_rid}',
                        'Filename': '{file_name}',
                        'MD5': '{md5}',
                    },
                    'dir_pattern': '^.*/(?P<schema>%s)/(?P<table>%s)/(?P<key_column>.*)/' %
                                   (self.schema_name, self.name),
                    'ext_pattern': extension_pattern,
                    'file_pattern': file_pattern,
                    'hatrac_templates': {'hatrac_uri': '/hatrac/{schema}/{table}/{md5}.{file_name}'},
                    'target_table': [self.schema_name, asset_table_name],
                    # Look for rows in the metadata table with matching key column values.
                    'metadata_query_templates': [
                        '/attribute/D:={schema}:{table}/%s={key_column}/table_rid:=D:RID' % key_column],
                    # Rows in the asset table should have a FK reference to the RID for the matching metadata row
                    'record_query_template':
                        '/entity/{schema}:{table}_Asset/{table}={table_rid}/MD5={md5}/URL={URI_urlencoded}',
                    'hatrac_options': {'versioned_uris': True},
                }
            ]

        asset_table_name = '{}_Asset'.format(self.name)

        if set_policy and chaise_tags.catalog_config not in self.catalog.model.annotations:
            raise DerivaCatalogError(self, msg='Attempting to configure table before catalog is configured')

        with DerivaModel(self.catalog) as m:
            model = m.model()
            if key_column not in self._column_names():
                raise DerivaCatalogError(self, msg='Key column not found in target table')

        column_defs = [
                          em.Column.define('{}'.format(self.name),
                                           em.builtin_types['text'],
                                           nullok=False,
                                           comment="The {} entry to which this asset is attached".format(
                                               self.name)),
                      ] + column_defs

        # Set up policy so that you can only add an asset to a record that you own.
        fkey_acls, fkey_acl_bindings = {}, {}
        if set_policy:
            groups = self.catalog.get_groups()

            fkey_acls = {
                "insert": [groups['curator']],
                "update": [groups['curator']],
            }
            fkey_acl_bindings = {
                "self_linkage_creator": {
                    "types": ["insert", "update"],
                    "projection": ["RCB"],
                    "projection_type": "acl",
                },
                "self_linkage_owner": {
                    "types": ["insert", "update"],
                    "projection": ["Owner"],
                    "projection_type": "acl",
                }
            }

        # Link asset table to metadata table with additional information about assets.
        asset_fkey_defs = [
                              em.ForeignKey.define(['{}'.format(self.name)],
                                                   self.schema_name, self.name, ['RID'],
                                                   acls=fkey_acls, acl_bindings=fkey_acl_bindings,
                                                   constraint_names=[
                                                       (self.schema_name,
                                                        '{}_{}_fkey'.format(asset_table_name, self.name))],
                                                   )
                          ] + fkey_defs
        comment = comment if comment else 'Asset table for {}'.format(self.name)

        if chaise_tags.table_display not in annotations:
            annotations[chaise_tags.table_display] = {'row_name': {'row_markdown_pattern': '{{{Filename}}}'}}

        table_def = em.Table.define_asset(self.schema_name, asset_table_name, fkey_defs=asset_fkey_defs,
                                          column_defs=column_defs, key_defs=key_defs, annotations=annotations,
                                          acls=acls, acl_bindings=acl_bindings,
                                          comment=comment)

        for i in table_def['column_definitions']:
            if i['name'] == 'URL':
                i[chaise_tags.column_display] = {'*': {'markdown_pattern': '[**{{URL}}**]({{{URL}}})'}}
            if i['name'] == 'Filename':
                i[chaise_tags.column_display] = {'*': {'markdown_pattern': '[**{{Filename}}**]({{{URL}}})'}}
            if i['name'] == 'Length' or i['name'] == 'MD5' or i['name'] == 'URL':
                i[chaise_tags.generated] = True

        with DerivaModel(self.catalog) as m:
            model = m.model()

            asset_table = self.catalog.schema_model(self.schema_name)._create_table(table_def)

            # The last thing we should do is update the upload spec to accomidate this new asset table.
            if chaise_tags.bulk_upload not in self.catalog.model.annotations:
                model.annotations.update({
                    chaise_tags.bulk_upload: {
                        'asset_mappings': [],
                        'version_update_url': 'https://github.com/informatics-isi-edu/deriva-qt/releases',
                        'version_compatibility': [['>=0.4.3', '<1.0.0']]
                    }
                })

            # Clean out any old upload specs if there are any and add the new specs.
            upload_annotations = model.annotations[chaise_tags.bulk_upload]
            upload_annotations['asset_mappings'] = \
                [i for i in upload_annotations['asset_mappings'] if
                 not (
                         i.get('target_table', []) == [self.schema_name, asset_table_name]
                         or
                         (
                                 i.get('target_table', []) == [self.schema_name, self.name]
                                 and
                                 i.get('asset_type', '') == 'table'
                         )
                 )
                 ] + create_asset_upload_spec()

        return asset_table

    def link_tables(self, column_name, target_table, target_column='RID'):
        """
        Create a foreign key link from the specified column to the target table and column.
        :param column_name: Column or list of columns in current table which will hold the FK
        :param target_table:
        :param target_column:
        :return:
        """

        with DerivaModel(self.catalog):
            if type(column_name) is str:
                column_name = [column_name]
            self.create_fkey(
                em.ForeignKey.define(column_name,
                                     target_table.schema_name, target_table.name,
                                     target_column if type(target_column) is list else [
                                         target_column],
                                     constraint_names=[(self.schema_name,
                                                        '_'.join([self.name] +
                                                                 column_name +
                                                                 ['fkey']))],
                                     )
            )
        return

    def link_vocabulary(self, column_name, term_table):
        """
        Set an existing column in the table to refer to an existing vocabulary table.
        :param column_name: Name of the column whose value is to be from the vocabular
        :param term_table: The term table.
        :return: None.
        """
        if not ({'ID', 'URI', 'Description', 'Name'} < set(term_table._column_names())):
            raise DerivaCatalogError(self, 'Attempt to link_vocabulary on a non-vocabulary table')

        self.link_tables(column_name, term_table, target_column='ID')
        return

    def disassociate_tables(self, target_table):
        association_table_name = '{}_{}'.format(self.name, target_table.name)
        try:
            self.catalog.schema_model(self.schema_name).table_model(association_table_name).delete()
        except KeyError:
            self.catalog.schema_model(target_table.schema_name).table_model(association_table_name).delete()

    def associate_tables(self, target_schema, target_table, table_column='RID', target_column='RID'):
        """
        Create a pure binary association table that connects rows in the table to rows in the target table.
        Assume that RIDs are used for linking. however, this can be overridder.
        :param target_schema: Schema of the table that is to be associated with current table
        :param target_table: Name of the table that is to be associated with the current table
        :param table_column: Name of the column in the current table that is used for the foreign key, defaults to RID
        :param target_column: Name of the column in the target table that is to be used for the foreign key, defaults
                              to RID
        :return: Association table.
        """

        association_table_name = '{}_{}'.format(self.name, target_table)

        column_defs = [
            em.Column.define('{}'.format(self.name), em.builtin_types['text'], nullok=False),
            em.Column.define('{}'.format(target_table), em.builtin_types['text'], nullok=False)
        ]

        key_defs = [
            em.Key.define([self.name, target_table],
                          constraint_names=[
                              (self.schema_name,
                               '{}_{}_{}_key'.format(association_table_name, self.name, target_table))],
                          )
        ]

        fkey_defs = [
            em.ForeignKey.define([self.name],
                                 self.schema_name, self.name, [table_column],
                                 constraint_names=[
                                     (self.schema_name, '{}_{}_fkey'.format(association_table_name, self.name))],
                                 ),
            em.ForeignKey.define([target_table],
                                 target_schema, target_table, [target_column],
                                 constraint_names=[
                                     (self.schema_name, '{}_{}_fkey'.format(association_table_name, target_table))])
        ]
        table_def = em.Table.define(association_table_name, column_defs=column_defs,
                                    key_defs=key_defs, fkey_defs=fkey_defs,
                                    comment='Association table for {}'.format(association_table_name))
        with DerivaModel(self.catalog) as m:
            association_table = m.schema_model(self.schema_name).create_table(self.catalog, table_def)
            self.catalog.update_referenced_by()
            return self.catalog.schema_model(association_table.sname).table_model(association_table.name)
