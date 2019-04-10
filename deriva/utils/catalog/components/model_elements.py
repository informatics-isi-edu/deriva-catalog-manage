import logging
from enum import Enum
from collections import namedtuple, OrderedDict
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import MultiKeyedList

from deriva.core.ermrest_config import tag as chaise_tags
from deriva.core import ErmrestCatalog, get_credential

from urllib.parse import urlparse

logger = logging.getLogger(__name__)

CATALOG_CONFIG__TAG = 'tag:isrd.isi.edu,2019:catalog-config'


class DerivaCatalogError(Exception):
    def __init__(self, msg):
        self.msg = msg


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


class DerivaModel:
    contexts = {i for i in DerivaContext if i is not DerivaContext("all")}

    def __init__(self, catalog):
        self.catalog = catalog

    def __enter__(self):
        self.catalog.nesting += 1
        logger.debug("Deriva model nesting %s" % self.catalog.nesting)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.catalog.nesting -= 1
        logger.debug("Deriva model nesting %s" % self.catalog.nesting)
        if self.catalog.nesting == 0:
            logger.debug('DerivaModel updated')
            self.catalog.apply()

    def model(self):
        return self.catalog.model

    def schema(self, schema):
        return self.catalog.model.schemas[schema]

    def table(self, table):
        return self.catalog.model.schemas[table.schema_name].tables[table.table_name]


class DerivaCatalog:
    def __init__(self, catalog_or_host, scheme='https', catalog_id=1):
        """
        Initialize a DerivaCatalog.  This can be done one of two ways: by passing in an Ermrestcatalog object, or
        specifying the host and catalog id of the desired catalog.
        :param catalog_or_host:
        :param scheme:
        :param catalog_id:
        """

        self.nesting = 0

        self.catalog = (
            ErmrestCatalog(scheme,
                           catalog_or_host,
                           catalog_id,
                           credentials=get_credential(catalog_or_host))
            if type(catalog_or_host) is str else catalog_or_host)

        self.model = self.catalog.getCatalogModel()
        self.schema_classes = {}

    def apply(self):
        self.model.apply(self.catalog)
        return self

    def refresh(self):
        assert (self.nesting == 0)
        logger.debug('Refreshing model')
        self.model.apply(self.catalog)
        self.model = self.catalog.getCatalogModel()

    def display(self):
        for i in self.model.schemas:
            print('{}'.format(i))

    def update_referenced_by(self):
        """Introspects the 'foreign_keys' and updates the 'referenced_by' properties on the 'Table' objects.
        """
        for schema in self.model.schemas.values():
            for table in schema.tables.values():
                table.referenced_by = MultiKeyedList([])
        self.model.update_referenced_by()

    def getPathBuilder(self):
        return self.catalog.getPathBuilder()

    def _make_schema_instance(self, schema_name):
        return DerivaSchema(self, schema_name)

    def schema(self, schema_name):
        if self.model.schemas[schema_name]:
            return self.schema_classes.setdefault(schema_name, self._make_schema_instance(schema_name))

    def create_schema(self, schema_name, comment=None, acls={}, annotations={}):
        self.model.create_schema(self.catalog,
                                 em.Schema.define(
                                     schema_name,
                                     comment=comment,
                                     acls=acls,
                                     annotations=annotations
                                 )
                                 )
        return self.schema(schema_name)

    def get_groups(self):
        if chaise_tags.catalog_config in self.model.annotations:
            return self.model.annotations[chaise_tags.catalog_config]['groups']
        else:
            raise DerivaCatalogError(msg='Attempting to configure table before catalog is configured')

    def __getitem__(self, schema_name):
        return self.schema(schema_name)

    def __iter__(self):
        def schema_iterator(schemas):
            for i in schemas:
                yield self[i]
        return schema_iterator(self.model.schemas)

class DerivaSchema:
    def __init__(self, catalog, schema_name):
        self.catalog = catalog
        self.schema_name = schema_name
        self.table_classes = {}

    def display(self):
        for t in self.catalog.model.schemas[self.schema_name].tables:
            print('{}'.format(t))

    @property
    def name(self):
        return self.schema_name

    @property
    def comment(self):
        with DerivaModel(self.catalog) as m:
            return m.schema(self.schema_name).comment

    @comment.setter
    def comment(self, value):
        with DerivaModel(self.catalog) as m:
            m.schema(self.schema_name).comment = value

    @property
    def acls(self):
        with DerivaModel(self.catalog) as m:
            return m.schema(self.schema_name).acls

    @acls.setter
    def acls(self, value):
        with DerivaModel(self.catalog) as m:
            m.schema(self.schema_name).acls.update(value)

    def _make_table_instance(self, schema_name, table_name):
        return DerivaTable(self.catalog, schema_name, table_name)

    def table(self, table_name):
        if self.catalog.model.schemas[self.schema_name].tables[table_name]:
            return self.table_classes.setdefault(table_name, self._make_table_instance(self.schema_name, table_name))

    def create_table(self, table_name, column_defs,
                     key_defs=[], fkey_defs=[],
                     comment=None,
                     acls={},
                     acl_bindings={},
                     annotations={}):
        column_defs = [col.definition() if type(col) is DerivaColumnDef else col for col in column_defs]
        key_defs = [key.definition() if type(key) is DerivaKey else key for key in key_defs]
        fkey_defs = [fkey.definition() if type(fkey) is DerivaForeignKey else fkey for fkey in fkey_defs]

        return self._create_table(em.Table.define(
            table_name, column_defs,
            key_defs=key_defs, fkey_defs=fkey_defs,
            comment=comment,
            acls=acls, acl_bindings=acl_bindings,
            annotations=annotations))

    def _create_table(self, table_def):
        with DerivaModel(self.catalog) as m:
            schema = m.schema(self.schema_name)
            schema.create_table(self.catalog.catalog, table_def)
            table = self.table(table_def['table_name'])
            table.deleted = False
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
        return self.table(table_name)

    def __iter__(self):
        def table_iterator(schemas):
            for i in schemas:
                yield self[i]
        return table_iterator(self.model.schemas[self.schema_name].tables)


class DerivaColumnMap(OrderedDict):
    def __init__(self, table, column_map, dest_table):
        self.table = table
        super().__init__(self._normalize_column_map(table, column_map, dest_table))

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
            :param v: Either the name of the new column or a dictionary of new column attributes.
            :return:
            """

            if isinstance(v, (DerivaColumnDef, DerivaKey, DerivaForeignKey)):
                return v

            try:
                # Get the existing column definition if it exists.
                col = table._column_definition(k).prejson()  # Get current definition for the column
                col['type'] = table._column_definition(k).type.typename

            except KeyError:
                # Column is new, so create a default definition for it. If value is a string, then its the type.
                return DerivaColumnDef(**{'name': k, 'table': dest_table, **({'type': v} if type(v) is str else v)})

            # We have a column remap in the form of col: new_name or col: dictionary
            # Create a proper dictionary spec for the value adding in a table entry in the case if needed.
            col['table'] = dest_table
            col.update({'name': v} if type(v) is str else v)
            return DerivaColumnDef(**col)

        # Go through the columns in order and add map entries, converting any map entries that are just column names
        # or dictionaries to DerivaColumnDefs
        column_map = {k: normalize_column(k, v) for k, v in column_map.items()}

        # Collect up all of the column name maps.
        column_name_map = {k: v.name for k, v in column_map.items()}

        with DerivaModel(self.table.catalog) as m:
            # Get new key and fkey definitions by mapping to new column names.
            column_map.update(
                {key.names[0]:
                    DerivaKey(
                        table=dest_table,
                        columns=[column_name_map.get(c, c) for c in key.unique_columns],
                        comment=key.comment,
                        annotations=key.annotations
                    )
                    for key in m.table(table).keys if
                    table._key_in_columns(column_name_map.keys(), key.unique_columns, dest_table)
                }
            )
            column_map.update(
                {
                    fkey.names[0]:
                        DerivaForeignKey(
                            table=dest_table,
                            columns=[column_name_map.get(c['column_name'], c['column_name']) for c in
                                     fkey.foreign_key_columns],
                            dest_table=table.catalog.schema(fkey.referenced_columns[0]['schema_name']).table(
                                fkey.referenced_columns[0]['table_name']),
                            dest_columns=[c['column_name'] for c in fkey.referenced_columns],
                            comment=fkey.comment,
                            acls=fkey.acls,
                            acl_bindings=fkey.acl_bindings
                        )
                    for fkey in m.table(table).foreign_keys
                    if table._key_in_columns(column_name_map.keys(),
                                             [i['column_name'] for i in fkey.foreign_key_columns],
                                             dest_table
                                             )
                }
            )
            return column_map

    def get_columns(self):
        return {k: v for k, v in self.items() if isinstance(v, DerivaColumnDef)}

    def get_keys(self):
        return {k: v for k, v in self.items() if isinstance(v, DerivaKey)}

    def get_foreign_keys(self):
        return {k: v for k, v in self.items() if isinstance(v, DerivaForeignKey)}

    def get_names(self):
        field = 'name'
        return {k: getattr(v, field) for k, v in self.items() if getattr(v, field)}


class DerivaVisibleSources:
    def __init__(self, table, tag):
        self.table = table
        self.tag = tag

    def __str__(self):
        ''.join(['{}\n{}'.format(k, v) for k, v in self.table.get_annotation(self.tag)])

    def validate(self):
        for c, l in self.table.get_annotation(self.tag).items():
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
            self.table.set_annotation(self.tag, new_vs)

    def display(self):
        for k, v in self.table.get_annotation(self.tag).items():
            print(k, v)

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

    def insert_context(self, context, sources=[]):
        context = DerivaContext(context)
        # Map over sources and make sure that they are all ok before we inster...
        sources = [DerivaSourceSpec(self.table, j).spec for j in sources]
        self.table.get_annotation(self.tag).update({context.value: sources})

    def insert_sources(self, source_list, positions={}):
        """
        Insert a set of columns into a source list.  If column is included in a foreign-key, make source an outgoing
        spec.
        :param source_list: A column map which will indicate the sources to be included.
        :param positions: where it insert the so
        :return:
        """

        positions = self._normalize_positions({'all'} if positions == {} else positions)

        with DerivaModel(self.table.catalog) as m:
            table = m.model().table(self.table.schema_name, self.table.table_name)

            # Identify any columns that are references to assets and collect up associated columns.
            skip_columns, assets = [], []
            for col in [DerivaSourceSpec(self.table, i).column_name for i in source_list]:
                if col == 'pseudo_column':
                    continue
                if chaise_tags.asset in table.column_definitions[col].annotations:
                    assets.append(col)
                    skip_columns.extend(table.column_definitions[col][chaise_tags.asset].values())

            sources = {}
            for context, context_list in self.table.get_annotation(self.tag).items():
                if DerivaContext(context) not in positions.keys():
                    continue

                # Get list of column names that are in the spec, mapping back simple FK references.
                source_names = [DerivaSourceSpec(self.table, i).column_name for i in context_list]
                new_context = context_list[:]

                for source in [DerivaSourceSpec(self.table, i) for i in source_list]:
                    if (context == 'entry' and source.column_name in skip_columns) or \
                            (source.column_name != 'pseudo_column' and source.column_name in source_names):
                        # Skip over asset columns in entry context and make sure we don't have repeat column specs.
                        continue

                    new_context.append(source.spec)
                    source_names.append(source.column_name)

                sources[context] = new_context
            sources = self._reorder_sources(sources, positions)

            # All is good, so update the visible columns annotation.
            self.table.set_annotation(self.tag, {**self.table.get_annotation(self.tag), **sources})

    def rename_columns(self, column_map):
        """
        Go through a list of visible specs and rename the spec, returning a new visible column spec.
        :param column_map:
        :return:
        """
        vc = {
            k: [
                j for i in v for j in (
                    [i] if (DerivaSourceSpec(self.table, i).rename_column(column_map) == i)
                    else [DerivaSourceSpec(self.table, i).rename_column(column_map)]
                )
            ] for k, v in self.table.get_annotation(self.tag).items()
        }
        return vc

    def copy_visible_source(self, from_context):
        pass

    def delete_visible_source(self, columns, contexts=[]):
        context_names = [i.value for i in (DerivaContext if contexts == [] else contexts)]
        for context, vc_list in self.table.get_annotation(self.tag).items():
            # Get list of column names that are in the spec, mapping back simple FK references.
            if context not in context_names:
                continue
            vc_names = [DerivaSourceSpec(self.table, i).column_name for i in vc_list]
            for col in columns:
                if col in vc_names:
                    del vc_list[vc_names.index(col)]
                    vc_names.remove(col)

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
            source_names = [DerivaSourceSpec(self.table, i).column_name for i in source_list]

            # Now build up a map that has the indexes of the reordered columns.  Include the columns in order
            # Unless they are in the column_list, in which case, insert them immediately after the key column.
            reordered_names = source_names[:]

            for key_col, column_list in positions[deriva_context].items():
                if not (set(column_list + [key_col]) <= set(source_names)):
                    raise DerivaCatalogError('Invalid position specification in reorder columns')
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


class DerivaSourceSpec:
    def __init__(self, table, spec):
        self.table = table
        self.spec = spec.spec if isinstance(spec, DerivaSourceSpec) else self.normalize_column_entry(spec)
        self.source = self.spec['source']
        self.column_name = self._referenced_columns()

    def source_type(self):
        if type(self.source) is str:
            return 'column'
        elif isinstance(self.source, (list, tuple)) and len(self.source) == 2:
            if 'inbound' in self.source[0]:
                return 'inbound'
            elif 'outbound' in self.source[0]:
                return 'outbound'
        return None

    def normalize_column_entry(self, spec):
        with DerivaModel(self.table.catalog) as m:
            table_m = m.table(self.table)
            if type(spec) is str:
                if spec not in table_m.column_definitions.elements:
                    raise DerivaCatalogError('Invalid source entry {}'.format(spec))
                return {'source': spec}
            if isinstance(spec, (tuple, list)) and len(spec) == 2:
                spec = tuple(spec)
                if spec in self.table.keys().elements:
                    return {'source': self.table.keys()[spec].unique_columns[0]}
                elif spec in self.table.foreign_keys().elements:
                    return {'source': [{'outbound': spec}, 'RID']}
                else:
                    raise DerivaCatalogError('Invalid source entry {}'.format(spec))
            else:
                return self.normalize_source_entry(spec)

    def normalize_source_entry(self, spec):
        with DerivaModel(self.table.catalog) as m:
            model = m.model()
            table_m = m.table(self.table)

            source_entry = spec['source']
            if type(source_entry) is str:
                if source_entry not in table_m.column_definitions.elements:
                    raise DerivaCatalogError('Invalid source entry {}'.format(spec))
                else:
                    return spec

            # We have a path of FKs so follow the path to make sure that all of the constraints line up.
            path_table = table_m

            for c in source_entry[0:-1]:
                if 'inbound' in c and len(c['inbound']) == 2:
                    k = tuple(c['inbound'])
                    target_schema = path_table.referenced_by[k].foreign_key_columns[0]['schema_name']
                    target_table = path_table.referenced_by[k].foreign_key_columns[0]['table_name']
                    path_table = model.schemas[target_schema].tables[target_table]
                elif 'outbound' in c and len(c['outbound']) == 2:
                    k = tuple(c['outbound'])
                    target_schema = path_table.foreign_keys[k].referenced_columns[0]['schema_name']
                    target_table = path_table.foreign_keys[k].referenced_columns[0]['table_name']
                    path_table = model.schemas[target_schema].tables[target_table]
                else:
                    raise DerivaCatalogError('Invalid source entry {}'.format(c))

            if source_entry[-1] not in path_table.column_definitions.elements:
                raise DerivaCatalogError('Invalid source entry {}'.format(source_entry[-1]))
        return spec

    def rename_column(self, column_map):
        if self.source_type() == 'column':
            # Get a dictionary keyed by column names in the child table.
            if self.source in column_map:
                fk_cols = {
                    fk.columns[0]:
                        (fk.name, fk.dest_columns[0])
                    for fk in column_map.get_foreign_keys().values() if len(fk.columns) == 1
                }
                # If we are renaming a column, and it is used in a foreign_key, then make the spec be a outbound
                # source using the FK.  Otherwise, just rename the column in the spec if needed.
                if column_map[self.source].name in fk_cols:
                    return {'source': [{'outbound': fk_cols[self.source][0]}, fk_cols[self.source][1]]}
                else:
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
        # Return the column name that is referenced in the source spec.
        # This will require us to look up the column behind an outbound foreign key reference.
        if type(self.source) is str:
            return self.source
        elif len(self.source) == 2 and 'outbound' in self.source[0]:
            t = tuple(self.source[0]['outbound'])
            fk_cols = self.table.foreign_keys()[t].foreign_key_columns
            return fk_cols[0]['column_name'] if len(fk_cols) == 1 else {'pseudo_column': self.source}
        return 'pseudo_column'


class DerivaColumnDef:
    def __init__(self, table, name, type, nullok=True, default=None, fill=None, comment=None, acls={},
                 acl_bindings={}, annotations={}):
        self.name = name
        self.type = type if isinstance(type, em.Type) else em.builtin_types[type]
        self.table = table
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

    @staticmethod
    def convert_def(table, column_def):
        return DerivaColumnDef(table, **column_def)


class DerivaKey:
    def __init__(self,
                 table,
                 columns,
                 name=None,
                 comment=None,
                 annotations={}):
        self.columns = columns
        self.table = table
        self.name = (name
                     if name
                     else (table.schema_name, '{}_'.format(table.table_name) + '_'.join([i for i in columns])))
        self.comment = comment
        self.annotations = annotations

    def definition(self):
        return em.Key.define(
            self.columns,
            constraint_names=[self.name],
            comment=self.comment,
            annotations=self.annotations
        )

    @staticmethod
    def convert_def(table, key_def):
        return DerivaKey(table, **key_def)


class DerivaForeignKey:
    def __init__(self,
                 table, columns,
                 dest_table, dest_columns,
                 name=None,
                 comment=None,
                 on_update='NO ACTION',
                 on_delete='NO ACTION',
                 acls={},
                 acl_bindings={},
                 annotations={}):
        self.name = (name
                     if name
                     else (table.schema_name, '{}_'.format(table.table_name) + '_'.join([i for i in columns])))
        self.type = type
        self.table = table
        self.columns = columns
        self.dest_table = dest_table
        self.dest_columns = dest_columns
        self.comment = comment
        self.on_update = on_update
        self.on_delete = on_delete
        self.acls = acls
        self.acl_bindings = acl_bindings
        self.annotations = annotations

        fk_ops = ['CASCADE', 'DELETE', 'RESTRICT', 'NO ACTION', 'SET NULL']

        if on_update not in fk_ops or on_delete not in fk_ops:
            raise ValueError('Invalid value for on_update/on_delete {} {}'.format(on_update, on_delete))

    def definition(self):
        return em.ForeignKey.define(
            self.columns,
            self.dest_table.schema_name, self.dest_table.table_name, self.dest_columns,
            constraint_names=[self.name],
            comment=self.comment,
            on_update=self.on_update,
            on_delete=self.on_delete,
            acls=self.acls,
            acl_bindings=self.acl_bindings,
            annotations=self.annotations
        )

    @staticmethod
    def convert_def(table, fkey_def):
        return DerivaForeignKey(table, **fkey_def)


class DerivaTable:
    def __init__(self, catalog, schema_name, table_name):
        self.catalog = catalog
        self.schema_name = schema_name
        self.table_name = table_name
        self.deleted = False

    def validate(self):
        self.visible_columns().validate()

    def _column_names(self):
        table = self.catalog.model.schemas[self.schema_name].tables[self.table_name]
        return [i.name for i in table.column_definitions]

    def _column_map(self, column_map, dest_table):
        return DerivaColumnMap(self, column_map, dest_table)

    def _column_definition(self, name):
        table = self.catalog.model.schemas[self.schema_name].tables[self.table_name]
        return table.column_definitions[name]

    def display(self):
        table = self.catalog.model.schemas[self.schema_name].tables[self.table_name]
        print('{}:{}'.format(self.schema_name, self.table_name))
        for i in table.column_definitions:
            print('{}\t{}\tnullok:{}\tdefault:{}'.format(i.name, i.type.typename, i.nullok, i.default))

        for i in table.keys:
            print('\t{}:{}\t{}'.format(i.names[0][0], i.names[0][1], i.unique_columns))

        for i in table.foreign_keys:
            print('    ', [c['column_name'] for c in i.foreign_key_columns],
                  '-> {}:{}:'.format(i.referenced_columns[0]['schema_name'], i.referenced_columns[0]['table_name']),
                  [c['column_name'] for c in i.referenced_columns])

        for i in table.referenced_by:
            print('    ', [c['column_name'] for c in i.referenced_columns],
                  '<- {}:{}:'.format(i.foreign_key_columns[0]['schema_name'], i.foreign_key_columns[0]['table_name']),
                  [c['column_name'] for c in i.foreign_key_columns])

    def chaise_uri(self):
        p = urlparse(self.catalog.catalog.get_server_uri())
        catalog_id = p.path.split('/')[-1]
        print('{}://{}/chaise/recordset/#{}/{}:{}'.format(
            p.scheme, p.hostname, catalog_id, self.schema_name, self.table_name)
        )

    def datapath(self):
        return self.catalog.getPathBuilder().schemas[self.schema_name].tables[self.table_name]

    def entities(self, *attributes, **renamed_attributes):
        return self.datapath().entities(*attributes, **renamed_attributes)

    def __getitem__(self, column_name):
        return DerivaColumnDef(self.schema(column_name))

    def __iter__(self):
        def schema_iterator(schemas):
            for i in schemas:
                yield self[i]
        return schema_iterator(self.model.schemas)

    @property
    def name(self):
        return self.table_name

    @property
    def comment(self):
        with DerivaModel(self.catalog) as m:
            return m.table(self).comment

    @comment.setter
    def comment(self, value):
        with DerivaModel(self.catalog) as m:
            m.table(self).comment = value

    @property
    def acls(self):
        with DerivaModel(self.catalog) as m:
            return m.table(self).acls

    @acls.setter
    def acls(self, value):
        with DerivaModel(self.catalog) as m:
            m.table(self).acls.update(value)

    @property
    def acl_bindings(self):
        with DerivaModel(self.catalog) as m:
            return m.table(self).acls

    @acl_bindings.setter
    def acl_bindings(self, value):
        with DerivaModel(self.catalog) as m:
            m.table(self).acl_bindings.update(value)

    def get_annotation(self, tag):
        with DerivaModel(self.catalog) as m:
            table = m.table(self)
            if tag not in table.annotations:
                table.annotations[tag] = {}
            return m.table(self).annotations[tag]

    def annotations(self):
        with DerivaModel(self.catalog) as m:
            return m.table(self).annotations

    def set_annotation(self, annotation, value):
        with DerivaModel(self.catalog) as m:
            m.table(self).annotations.update({annotation: value})

    def visible_columns(self):
        return DerivaVisibleSources(self, chaise_tags.visible_columns)

    def visible_foreign_keys(self):
        return DerivaVisibleSources(self, chaise_tags.visible_foreign_keys)

    def _visible_columns(self):
        with DerivaModel(self.catalog) as m:
            table = m.table(self)
            if chaise_tags.visible_columns not in table.annotations:
                table.annotations[chaise_tags.visible_columns] = {}
            return m.table(self).annotations[chaise_tags.visible_columns]

    def create_key(self, key_def):
        if isinstance(key_def, DerivaKey):
            key_def = key_def.definition()
        with DerivaModel(self.catalog) as m:
            m.table(self).create_key(self.catalog.catalog, key_def)

    def keys(self):
        with DerivaModel(self.catalog) as m:
            return m.table(self).keys

    def foreign_keys(self):
        with DerivaModel(self.catalog) as m:
            return m.table(self).foreign_keys

    def create_fkey(self, fkey_def,
                    columns, child_table, child_columns,
                    name=None,
                    comment=None,
                    on_update='NO_ACTION',
                    on_delete='NO_ACTION',
                    acls={},
                    acl_bindings={},
                    annotations={},
                    position={}):
        # TODO THis needs to be finished.
        fkey_def = DerivaForeignKey(self, columns, comment=comment, acls=acls, annotations=annotations, )
        if isinstance(fkey_def, DerivaForeignKey):
            fkey_def = fkey_def.definition()

        dest_table = self.catalog.schema(fkey_def['referenced_columns'][0]['schema_name']). \
            table(fkey_def['referenced_columns'][0]['table_name'])

        with DerivaModel(self.catalog) as m:
            fkey = m.table(self).create_fkey(self.catalog.catalog, fkey_def)
            # Add foreign key to appropriate referenced_by list
            m.table(dest_table).referenced_by.append(fkey)

            self.visible_foreign_keys().insert_sources(
                [DerivaSourceSpec(self, fkey.names[0])],
                position)
            # TODO Need to go over source list and update to change column spec to FK spec.

    def sources(self, merge_outbound=False):
        with DerivaModel(self.catalog) as m:
            table = m.table(self)

            # Go through the list of foreign keys and create a list of key columns and referenced columns.
            fkey_names = {}
            for fk in table.foreign_keys:
                ckey = [c['column_name'] for c in fk.foreign_key_columns]  # List of names in composite key.
                if len(ckey) == 1:
                    fkey_names[ckey[0]] = fk.names[0]

            # TODO We should check to see if target is vocabulary and if so use ID rather then RID
            column_sources = [
                {'source': (
                    [{'outbound': fkey_names[col.name]}, 'RID']
                    if col.name in fkey_names and merge_outbound
                    else col.name
                )
                }
                for col in table.column_definitions
            ]

            outbound_sources = [{'source': [{'outbound': i.names[0]}, 'RID']} for i in table.foreign_keys]
            inbound_sources = [{'source': [{'inbound': i.names[0]}, 'RID']} for i in table.referenced_by]
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
                renamed = self.visible_columns().rename_columns(column_map)
            else:
                renamed = v
            new_annotations[k] = renamed
        return new_annotations

    def _rename_columns_in_acl_bindings(self, _column_map):
        with DerivaModel(self.catalog) as m:
            table = m.table(self)
            return table.acl_bindings

    def _rename_columns_in_column_annotations(self, annotation, column_map):
        return annotation

    def _key_in_columns(self, columns, key_columns, dest_table):
        """
        Given a set of columns and a key, return true if the key is in that column set.  If we are simply renaming
        columns, rather then moving them to a new table, not all of the columns in a composite key have to be present.
        :param columns:  List of columns in a table that are being altered
        :param key_columns: list of columns in the key

        :return: True if the key is contained within columns.
        """

        overlap = set(columns).intersection(set(key_columns))
        # Determine if we are moving the column within the same table, or between tables.
        rename = self.schema_name == dest_table.schema_name and self.table_name == dest_table.table_name

        if len(overlap) == 0:
            return False
        if not rename and len(overlap) < len(key_columns):
            raise DerivaCatalogError(msg='Cannot rename part of compound key {}'.format(key_columns))
        return True

    def _check_composite_keys(self, columns, dest_table):
        """
        Go over all of the keys, incoming and outgoing foreign keys and check to make sure that renaming the set of
        columns colulumns won't break up composite keys if they are renamed.
        :param columns:
        :param dest_table:
        :return:
        """
        columns = set(columns)

        with DerivaModel(self.catalog) as m:
            table = m.model().schemas[self.schema_name].tables[self.table_name]
            for i in table.keys:
                self._key_in_columns(columns, i.unique_columns, dest_table)

            for fk in table.foreign_keys:
                self._key_in_columns(columns, [i['column_name'] for i in fk.foreign_key_columns], dest_table)

            for fk in table.referenced_by:
                self._key_in_columns(columns, [i['column_name'] for i in fk.referenced_columns], dest_table)

    def _update_key_name(self, name, column_map, dest_table):
        # Helper function that creates a new constraint name by replacing table and column names.
        name = name[1].replace('{}_'.format(self.table_name), '{}_'.format(dest_table.table_name))

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
                key_def.table.create_key(key_def)

            for k, fkey_def in fkey_map.items():
                fkey_def.table.create_fkey(fkey_def)

    def _delete_columns_in_display(self, annotation, columns):
        raise DerivaCatalogError('Cannot delete column from display annotation')

    def _delete_columns_from_annotations(self, columns):
        for k, v in self.annotations().items():
            if k == chaise_tags.display:
                self._delete_columns_in_display(v, columns)
            elif k == chaise_tags.visible_columns or k == chaise_tags.visible_foreign_keys:
                DerivaVisibleSources(self, k).delete_visible_source(columns)

    def delete_fkeys(self, fkeys):
        fkeys = fkeys if isinstance(fkeys, list) else [fkeys]
        with DerivaModel(self.catalog) as m:
            model = m.model()
            for fk in fkeys:
                fkey = fk.definition() if isinstance(fk, DerivaForeignKey) else fk
                referenced = model.schemas[
                    fkey.referenced_columns[0]['schema_name']
                ].tables[
                    fkey.referenced_columns[0]['table_name']
                ]
                self.foreign_keys()[fkey.names[0]].delete(self.catalog.catalog, m.table(self))
                del referenced.referenced_by[fkey.names[0]]

    def delete_keys(self, keys):
        keys = keys if isinstance(keys, list) else [keys]
        with DerivaModel(self.catalog) as m:
            for k in keys:
                key = k.definition() if isinstance(k, DerivaKey) else k
                self.keys()[key.names[0]].delete(self.catalog.catalog, m.table(self))

    def column_def(self, name, type,
                   nullok=True,
                   default=None,
                   fill=None,
                   comment=None,
                   acls={},
                   acl_bindings={},
                   annotations={}):
        return DerivaColumnDef(self, name, type, nullok=nullok, default=default, fill=fill, comment=comment, acls=acls,
                               acl_bindings=acl_bindings, annotations=annotations)

    def delete_columns(self, columns):
        """
        Drop a column from a table, cleaning up visible columns and keys.
        :param columns:
        :return:
        """
        with DerivaModel(self.catalog) as m:
            table = m.table(self)

            self._check_composite_keys(columns, self)
            columns = set(columns)

            # Remove keys...
            for i in table.keys:
                if self._key_in_columns(columns, i.unique_columns, self):
                    i.delete(self.catalog.catalog, table)

            for fk in table.foreign_keys:
                fk_columns = [i['column_name'] for i in fk.foreign_key_columns]
                if self._key_in_columns(columns, fk_columns, self):  # We are renaming one of the foreign key columns
                    self.delete_fkeys(fk)

            for column in columns:
                self._delete_columns_from_annotations([column])
                table.column_definitions[column].delete(self.catalog.catalog, table)
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
        :param columns: Either a list of DerivaColumnDef, or a ERMrest column defiiniton.
        :param positions:  Where the column should be added into the visible columns spec.
        :param visible: Include this column in the visible columns spec.
        :return:
        """
        column_names = []
        columns = columns if type(columns) is list else [columns]

        for column in columns:
            try:
                # column is a DerivaColumnDef
                column_def = column.definition()
            except AttributeError:
                # Got a column definition already.
                column_def = column

            column_names.append(column_def['name'])

            with DerivaModel(self.catalog) as m:
                table = m.table(self)
                table.create_column(m.catalog.catalog, column_def)

        if visible:
            self.visible_columns().insert_sources(column_names, positions)

    def rename_column(self, from_column, to_column, default=None, nullok=None):
        """
        Rename a column by copying it and then deleting the origional column.
        :param from_column:
        :param to_column:
        :param default:
        :param nullok:
        :return:
        """
        column_map = {from_column: DerivaColumnDef(table=self, name=to_column, type=from_column.type, nullok=nullok,
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
            m.table(self).annotations.update(
                self._rename_columns_in_annotations(column_map, skip_annotations=[chaise_tags.visible_columns])
            )
            if delete:
                columns = [k for k in column_map.get_columns().keys()]
                self.delete_columns(columns)
        return

    def delete(self):
        with DerivaModel(self.catalog) as m:
            model = m.model()
            table = m.table(self)

            if table.referenced_by:
                DerivaCatalogError('Attept to delete catalog with incoming foreign keys')
            # Now we can delete the table.
            table.delete(self.catalog.catalog, schema=model.schemas[self.schema_name])
            self.deleted = True

    def _relink_columns(self, dest_table, column_map):
        with DerivaModel(self.catalog) as m:
            table = m.table(self)
            for fkey in table.referenced_by[:]:
                fk_columns = [i['column_name'] for i in fkey.foreign_key_columns]
                referenced_columns = [i['column_name'] for i in fkey.referenced_columns]
                column_name_map = column_map.get_names()
                child_table = self.catalog.schema(fkey.foreign_key_columns[0]['schema_name']). \
                    table(fkey.foreign_key_columns[0]['table_name'])

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
        proto_table = namedtuple('ProtoTable', ['schema_name', 'table_name'])(schema_name, table_name)

        with DerivaModel(self.catalog) as m:
            model = m.model()
            table = model.schemas[self.schema_name].tables[self.table_name]

            # Augment the column_map with entries for columns in the table, but not in the map.
            new_map = {i.name: column_map.get(i.name, i.name) for i in table.column_definitions}
            new_map.update(column_map)
            # Add keys to column map. We need to create a dummy destination table for this call.
            column_map = self._column_map(new_map, proto_table)

            # new_columns = [c['name'] for c in column_defs]

            new_table = self.catalog.schema(self.schema_name).create_table(
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
            new_table.table = table_name
            new_table.schema = schema_name

            # Copy over values from original to the new one, mapping column names where required. Use the column_fill
            # argument to provide values for non-null columns.
            pb = self.catalog.getPathBuilder()
            from_path = pb.schemas[self.schema_name].tables[self.table_name]
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
            new_map = {i.name: column_map.get(i.name, i.name) for i in m.table(self).column_definitions}
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
                                    (self.schema_name, self.table_name),
                    'target_table': [self.schema_name, self.table_name],
                },
                # Assets are in format assets/schema_name/table_name/correlation_key/file.ext
                {
                    'checksum_types': ['md5'],
                    'column_map': {
                        'URL': '{URI}',
                        'Length': '{file_size}',
                        self.table_name: '{table_rid}',
                        'Filename': '{file_name}',
                        'MD5': '{md5}',
                    },
                    'dir_pattern': '^.*/(?P<schema>%s)/(?P<table>%s)/(?P<key_column>.*)/' %
                                   (self.schema_name, self.table_name),
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

        asset_table_name = '{}_Asset'.format(self.table_name)

        if set_policy and chaise_tags.catalog_config not in self.catalog.model.annotations:
            raise DerivaCatalogError(msg='Attempting to configure table before catalog is configured')

        with DerivaModel(self.catalog) as m:
            model = m.model()
            if key_column not in self._column_names():
                raise DerivaCatalogError(msg='Key column not found in target table')

        column_defs = [
                          em.Column.define('{}'.format(self.table_name),
                                           em.builtin_types['text'],
                                           nullok=False,
                                           comment="The {} entry to which this asset is attached".format(
                                               self.table_name)),
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
                              em.ForeignKey.define(['{}'.format(self.table_name)],
                                                   self.schema_name, self.table_name, ['RID'],
                                                   acls=fkey_acls, acl_bindings=fkey_acl_bindings,
                                                   constraint_names=[
                                                       (self.schema_name,
                                                        '{}_{}_fkey'.format(asset_table_name, self.table_name))],
                                                   )
                          ] + fkey_defs
        comment = comment if comment else 'Asset table for {}'.format(self.table_name)

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

            asset_table = self.catalog.schema(self.schema_name)._create_table(table_def)

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
                                 i.get('target_table', []) == [self.schema_name, self.table_name]
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
                                     target_table.schema_name, target_table.table_name,
                                     target_column if type(target_column) is list else [
                                         target_column],
                                     constraint_names=[(self.schema_name,
                                                        '_'.join([self.table_name] +
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
            raise DerivaCatalogError('Attempt to link_vocabulary on a non-vocabulary table')

        self.link_tables(column_name, term_table, target_column='ID')
        return

    def disassociate_tables(self, target_table):
        association_table_name = '{}_{}'.format(self.table_name, target_table.table_name)
        try:
            self.catalog.schema(self.schema_name).table(association_table_name).delete()
        except KeyError:
            self.catalog.schema(target_table.schema_name).table(association_table_name).delete()

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

        association_table_name = '{}_{}'.format(self.table_name, target_table)

        column_defs = [
            em.Column.define('{}'.format(self.table_name), em.builtin_types['text'], nullok=False),
            em.Column.define('{}'.format(target_table), em.builtin_types['text'], nullok=False)
        ]

        key_defs = [
            em.Key.define([self.table_name, target_table],
                          constraint_names=[
                              (self.schema_name,
                               '{}_{}_{}_key'.format(association_table_name, self.table_name, target_table))],
                          )
        ]

        fkey_defs = [
            em.ForeignKey.define([self.table_name],
                                 self.schema_name, self.table_name, [table_column],
                                 constraint_names=[
                                     (self.schema_name, '{}_{}_fkey'.format(association_table_name, self.table_name))],
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
            association_table = m.schema(self.schema_name).create_table(self.catalog.catalog, table_def)
            self.catalog.update_referenced_by()
            return self.catalog.schema(association_table.sname).table(association_table.name)
