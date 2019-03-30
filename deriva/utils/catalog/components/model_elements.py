import logging
from enum import Enum
from sortedcollections import OrderedDict
from collections import namedtuple
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


class DerivaSchema:
    def __init__(self, catalog, schema_name):
        self.catalog = catalog
        self.schema_name = schema_name
        self.table_classes = {}

    def display(self):
        for t in self.catalog.model.schemas[self.schema_name].tables:
            print('{}'.format(t))

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


class DerivaVisibleSources:
    def __init__(self, table, tag):
        self.table = table
        self.tag = tag

    def __str__(self):
        ''.join([f'{k}\n{v}' for k, v in self.table.get_annotation(self.tag)])

    def validate(self):
        for c, l in self.table.get_annotation(self.tag).items():
            for j in l:
                DerivaSourceSpec(self.table, j)

    def display(self):
        for k,v in self.table.get_annotation(self.tag).items():
            print(k, v)

    def insert_sources(self, source_list, position={}, contexts={}, create=False):
        """
        Create a general visible columns annotation spec that would be consistant with what chaise does by default.
        This spec can then be added to a table and edited for user preference.
        :return:
        """

        # Get the contexts list into a standard form...
        if type(contexts) is str:
            contexts = {DerivaContext(contexts)}
        elif type(contexts) is DerivaContext:
            contexts = {contexts}
        elif contexts == {} or contexts == DerivaContext.all:
            contexts = DerivaModel.contexts

        with DerivaModel(self.table.catalog) as m:
            table = m.model().table(self.table.schema_name, self.table.table_name)

            # Create any missing contexts
            if create:
                sources = self.table.get_annotation(self.tag)
                for i in contexts:
                    if i.value not in sources.keys():
                        sources[i.value] = []

            # Identify any columns that are references to assets and collect up associated columns.
            skip_columns, assets = [], []
            for col in [DerivaSourceSpec(self.table, i).column_name for i in source_list]:
                if col == 'pseudo_column':
                    continue
                if chaise_tags.asset in table.column_definitions[col].annotations:
                    assets.append(col)
                    skip_columns.extend(table.column_definitions[col][chaise_tags.asset].values())

            # Go through the list of foreign keys and create a list of key columns and referenced columns.
            fkey_names, fkey_cols = {}, {}
            for fk in table.foreign_keys:
                ckey = [c['column_name'] for c in fk.foreign_key_columns]  # List of names in composite key.
                if len(ckey) == 1:
                    fkey_names[ckey[0]] = fk.names[0]
                    fkey_cols[fk.names[0][1]] = ckey[0]

            sources = {}
            for context, context_list in self.table.get_annotation(self.tag).items():
                if DerivaContext(context) not in contexts:
                    continue

                # Get list of column names that are in the spec, mapping back simple FK references.
                source_names = [DerivaSourceSpec(self.table, i).column_name for i in context_list]
                new_context = context_list[:]

                for source in source_list:
                    col_name = DerivaSourceSpec(self.table, source).column_name
                    if (context == 'entry' and col_name in skip_columns) or \
                            (col_name != 'pseudo_column' and col_name in source_names):
                        # Skip over asset columns in entry context and make sure we don't have repeat column specs.
                        continue
                    new_context.append(source)
                    source_names.append(col_name)

                sources[context] = new_context
            sources = self._reorder_sources(sources, position)

            # All is good, so update the visible columns annotation.
            self.table.set_annotation(self.tag, {**self.table.get_annotation(self.tag), **sources})

    def rename_columns(self, column_map, dest_table):
        vc = {
            k: [
                j for i in v for j in (
                    [i] if (DerivaSourceSpec(self.table, i).rename_column(column_map, dest_table) == i)
                    else [DerivaSourceSpec(self.table, i).rename_column(column_map, dest_table)]
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
        vc = self._reorder_sources(self.table.annotation(self.tag), positions)
        self.table.set_annotation(self.tag, {**self.table.annotation(self.tag), **vc})

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
        positions = OrderedDict(positions) if positions.keys() in DerivaContext \
            else OrderedDict({DerivaContext.all: positions})
        position_contexts = \
            {
                i.value for i in
                (DerivaContext if positions.keys() == {} or positions.keys() == {DerivaContext.all}
                 else positions.keys())
            }

        new_sources = {}
        for context, source_list in sources.items():
            if context not in position_contexts:
                continue

            # Get the list of column names for the spec.
            source_names = [DerivaSourceSpec(self.table, i).column_name for i in source_list]

            # Now build up a map that has the indexes of the reordered columns.  Include the columns in order
            # Unless they are in the column_list, in which case, insert them immediately after the key column.
            reordered_names = source_names[:]

            for key_col, column_list in positions.get(context, positions[DerivaContext.all]).items():
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
    def __init__(self, catalog):
        super().__init__(catalog, chaise_tags.visible_column)


class DerivaVisibleForeignKeys(DerivaVisibleSources):
    def __init__(self, catalog):
        super().__init__(catalog, chaise_tags.visible_foreign_keys)


class DerivaSourceSpec:
    def __init__(self, table, spec):
        self.table = table
        self.spec = spec
        self.source = self.normalize_column_entry(spec)['source']
        self.column_name = self._referenced_columns()

    def source_type(self):
        if type(self.source) is str:
            return 'column'
        elif isinstance(self.source, (list,tuple)) and len(self.source) ==2:
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
                    raise DerivaCatalogError(f'Invalid source entry {spec}')
                return {'source': spec}
            if isinstance(spec, (tuple, list)) and len(spec) == 2:
                spec = tuple(spec)
                if spec in self.table.keys().elements:
                    return {'source': self.table.keys()[spec].unique_columns[0]}
                elif spec in self.table.foreign_keys().elements:
                    return {'source': [{'outbound': spec}, 'RID']}
                else:
                    raise DerivaCatalogError(f'Invalid source entry {spec}')
            else:
                return self.normalize_source_entry(spec)

    def normalize_source_entry(self, spec):
        with DerivaModel(self.table.catalog) as m:
            model = m.model()
            table_m = m.table(self.table)

            source_entry = spec['source']
            if type(source_entry) is str:
                if source_entry not in table_m.column_definitions.elements:
                    raise DerivaCatalogError(f'Invalid source entry {spec}')
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
                    raise DerivaCatalogError(f'Invalid source entry {c}')

            if source_entry[-1] not in path_table.column_definitions.elements:
                raise DerivaCatalogError(f'Invalid source entry {source_entry[-1]}')
        return spec

    def rename_column(self, column_map, dest_table):
        if self.source_type() == 'column':
            return {**self.spec,
                    **{'source': DerivaTable._column_map(column_map, 'name').get(self.source, self.source)}
                    }
        elif self.source_type() == 'outbound':
        # We have a FK list....
            return {
                **self.spec,
                **{'source':
                    (
                        {
                            'outbound': self.table._update_key_name(self.source[0]['outbound'], column_map, dest_table)
                        },
                        self.source[1]
                    )}
            }
        elif self.source_type() == 'inbound':
            return self.spec

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

    def from_foreign_key(self, fkey):
        if fkey.table == self.table.table_name:
            return


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
        return {i.name for i in table.column_definitions}

    def display(self):
        table = self.catalog.model.schemas[self.schema_name].tables[self.table_name]
        for i in table.column_definitions:
            print('{}\t{}\tnullok:{}\tdefault:{}'.format(i.name, i.type.typename, i.nullok, i.default))

        for i in table.keys:
            print(f'\t{i.names[0][0]}:{i.names[0][1]}\t{i.unique_columns}')

        for i in table.foreign_keys:
            print('    ', [c['column_name'] for c in i.foreign_key_columns],
                  '-> {}:{}:'.format(i.referenced_columns[0]['schema_name'], i.referenced_columns[0]['table_name']),
                  [c['column_name'] for c in i.referenced_columns])

        for i in table.referenced_by:
            print('    ', [c['column_name'] for c in i.referenced_columns],
                  '<- {}:{}:'.format(i.foreign_key_columns[0]['schema_name'], i.foreign_key_columns[0]['table_name']),
                  [c['column_name'] for c in i.foreign_key_columns])

    def uri(self):
        p = urlparse(self.catalog.catalog.get_server_uri())
        catalog_id = p.path.split('/')[-1]
        print(f'{p.scheme}://{p.hostname}/chaise/recordset/#{catalog_id}/{self.schema_name}:{self.table_name}')

    def datapath(self):
        return self.catalog.getPathBuilder().schemas[self.schema_name].tables[self.table_name]

    def entities(self, *attributes, **renamed_attributes):
        return self.datapath().entities(*attributes, **renamed_attributes)

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
        with DerivaModel(self.catalog) as m:
            m.table(self).create_key(self.catalog.catalog, key_def)

    def keys(self):
        with DerivaModel(self.catalog) as m:
            return m.table(self).keys

    def foreign_keys(self):
        with DerivaModel(self.catalog) as m:
            return m.table(self).foreign_keys

    def create_fkey(self, fkey_def):
        with DerivaModel(self.catalog) as m:
            target_schema = fkey_def['referenced_columns'][0]['schema_name']
            target_table = fkey_def['referenced_columns'][0]['table_name']
            fkey = m.table(self).create_fkey(self.catalog.catalog, fkey_def)
            m.model().schemas[target_schema].tables[target_table].referenced_by.append(fkey)

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
            for k, v in DerivaTable._column_map(column_map, 'name'):
                pattern = pattern.replace('{{{}}}'.format(k), '{{{}}}'.format(v))
            return pattern

        return {
            k: rename_markdown_pattern(v) if k == 'markdown_name' else v
            for k, v in dval.items()
        }

    def _rename_columns_in_annotations(self, column_map, dest_table):
        new_annotations = {}
        for k, v in self.annotations().items():
            if k == chaise_tags.display:
                renamed = self._rename_columns_in_display(v, column_map)
            elif k == chaise_tags.visible_columns:
                renamed = self.visible_columns().rename_columns(column_map, dest_table)
            elif k == chaise_tags.visible_foreign_keys:
                renamed = self.visible_foreign_keys().rename_columns(column_map, dest_table)
            else:
                renamed = v
            new_annotations[k] = renamed
        return new_annotations

    def _rename_column_in_fkey(self, fk, columns, column_map, dest_table, incoming=False):
        """
        Given an existing FK, create a new FK that reflects column renaming caused by changing the column name, or
        by moving the column to a new table and/or schema.
        :param fk: The existing fkey that is being renamed.
        :param columns: List of columns in this tablethat are being renamed.  Used to determine if FK is being impacted.
        :param column_map: dictionary that indicates column name remapping.
        :param dest_table: new table for the column
        :param incoming: True if we are renaming an incoming FK definition.
        :return:
        """

        # Determine if we are moving the column within the same table, or between tables.
        column_rename = self.schema_name == dest_table.schema_name and self.table_name == dest_table.table_name

        fk_columns = [i['column_name'] for i in fk.foreign_key_columns]
        referenced_columns = [i['column_name'] for i in fk.referenced_columns]
        column_name_map = self._column_map(column_map, 'name')

        # Rename the columns that appear in foreign keys...
        if incoming:
            fk_colnames = fk_columns
            pk_sname = dest_table.schema_name
            pk_tname = dest_table.table_name
            pk_colnames = [column_name_map.get(i, i) for i in referenced_columns]
            names = fk.names
            map_key = self._key_in_columns(columns, referenced_columns, column_rename)
        else:
            fk_colnames = [column_name_map.get(i, i) for i in fk_columns]
            pk_sname = fk.referenced_columns[0]['schema_name']
            pk_tname = fk.referenced_columns[0]['table_name']
            pk_colnames = referenced_columns
            names = [self._update_key_name(n, column_name_map, dest_table) for n in fk.names]
            map_key = self._key_in_columns(columns, fk_columns, column_rename)

        return em.ForeignKey.define(
            fk_colnames,
            pk_sname, pk_tname, pk_colnames,
            constraint_names=names,
            comment=fk.comment,
            acls=fk.acls,
            acl_bindings=fk.acl_bindings,
            annotations=fk.annotations
        ) if map_key else fk

    def _rename_column_in_key(self, key, columns, column_map, dest_table):
        """
        Create a new key def
        :param key:
        :param columns:
        :param column_map:
        :param dest_table:
        :return:
        """
        column_rename = self.schema_name == dest_table.schema_name and self.table_name == dest_table.table_name

        return em.Key.define(
            [self._column_map(column_map, 'name').get(c, c) for c in key.unique_columns],
            constraint_names=[self._update_key_name(n, column_map, dest_table) for n in key.names],
            comment=key.comment,
            annotations=key.annotations
        ) if self._key_in_columns(columns, key.unique_columns, column_rename) else key

        # Now look through incoming foreign keys to make sure none of them changed.

    def _rename_columns_in_acl_bindings(self, column_map):
        with DerivaModel(self.catalog) as m:
            table = m.table(self)
            return table.acl_bindings

    def _rename_columns_in_column_annotations(self, annotation, column_map):
        return annotation

    @staticmethod
    def _key_in_columns(columns, key_columns, rename):
        """
        Given a set of columns and a key, return true if the key is in that column set.  If we are simply renaming
        columns, rather then moving them to a new table, not all of the columns in a composite key have to be present.
        :param columns:  List of columns in a table that are being altered
        :param key_columns: list of columns in the key
        :param rename: True if we are just renaming columns within a table, False if we are moving columns between
                       tables.
        :return: True if the key is contained within columns.
        """
        overlap = set(columns).intersection(set(key_columns))

        if len(overlap) == 0:
            return False
        if not rename and len(overlap) < len(key_columns):
            raise DerivaCatalogError(msg='Cannot rename part of compound key')
        return True

    def _check_composite_keys(self, columns, dest_table, rename=None):
        """
        Go over all of the keys, incoming and outgoing foreign keys and check to make sure that renaming the set of
        columns colulumns won't break up composite keys if they are renamed.
        :param columns:
        :param dest_table:
        :param rename:
        :return:
        """
        local_rename = (
            rename if rename is not None
            else (self.schema_name == dest_table.schema_name and self.table_name == dest_table.table_name)
        )
        columns = set(columns)

        with DerivaModel(self.catalog) as m:
            table = m.model().schemas[self.schema_name].tables[self.table_name]
            for i in table.keys:
                self._key_in_columns(columns, i.unique_columns, local_rename)

            for fk in table.foreign_keys:
                self._key_in_columns(columns, [i['column_name'] for i in fk.foreign_key_columns], local_rename)

            for fk in table.referenced_by:
                self._key_in_columns(columns, [i['column_name'] for i in fk.referenced_columns], local_rename)

    def _update_key_name(self, name, column_map, dest_table):
        # Helper function that creates a new constraint name by replacing table and column names.
        name = name[1].replace('{}_'.format(self.table_name), '{}_'.format(dest_table.table_name))

        for k, v in column_map.items():
            name = name.replace(k, v)
        return dest_table.schema_name, name

    def _copy_keys(self, columns, column_name_map, dest_table):
        """
        Copy over the keys from the current table to the destination table, renaming columns.
        :param columns:
        :param column_name_map:
        :param dest_table:
        :return:
        """

        columns = set(columns)
        with DerivaModel(self.catalog) as m:
            target_table = m.table(dest_table)
            table = m.table(self)

            for i in table.keys:
                if i.unique_columns == ['RID']:
                    continue  # RID Key constraint is already put in place by ERMRest.
                key_def = self._rename_column_in_key(i, columns, column_name_map, dest_table)
                if key_def != i:
                    dest_table.create_key(key_def)

            # Rename the columns that appear in foreign keys...
            for fk in table.foreign_keys:
                fk_def = self._rename_column_in_fkey(fk, columns, column_name_map, dest_table)
                if fk_def != fk:
                    target_table.create_fkey(self.catalog.catalog, fk_def)

    def _add_fkeys(self, fkeys):
        with DerivaModel(self.catalog) as m:
            model = m.model()
            for fkey in fkeys:
                referenced = model.schemas[
                    fkey.referenced_columns[0]['schema_name']
                ].tables[
                    fkey.referenced_columns[0]['table_name']
                ]
                referenced.referenced_by.append(fkey)

    def _delete_columns_in_display(self, annotation, columns):
        raise DerivaCatalogError('Cannot delete column from diaplay annotation')

    def _delete_column_from_annotations(self, columns):
        for k, v in self.annotations().items():
            if k == chaise_tags.display:
                self._delete_columns_in_display(v, columns)
            elif k == chaise_tags.visible_columns or k == chaise_tags.visible_foreign_keys:
                DerivaVisibleSources(self, k).delete_visible_source(columns)

    def _delete_fkeys(self, fkeys):
        with DerivaModel(self.catalog) as m:
            model = m.model()
            for fkey in fkeys:
                referenced = model.schemas[
                    fkey.referenced_columns[0]['schema_name']
                ].tables[
                    fkey.referenced_columns[0]['table_name']
                ]
                del referenced.referenced_by[fkey]

    @staticmethod
    def _column_map(column_map, field):
        sub_map = {}
        for k, v in column_map.items():
            if type(v) is str and field == 'name':
                sub_map[k] = v
            elif field in v:
                sub_map[k] = v
        return sub_map

    def delete_columns(self, columns):
        """
        Drop a column from a table, cleaning up visible columns and keys.
        :param columns:
        :return:
        """
        with DerivaModel(self.catalog) as m:
            model = m.model()
            table = model.schemas[self.schema_name].tables[self.table_name]

            self._check_composite_keys(columns, self, rename=False)
            columns = set(columns)

            # Remove keys...
            for i in table.keys:
                if self._key_in_columns(columns, i.unique_columns, False):
                    i.delete(self.catalog.catalog, table)

            for fk in table.foreign_keys:
                fk_columns = [i['column_name'] for i in fk.foreign_key_columns]
                if self._key_in_columns(columns, fk_columns, False):  # We are renaming one of the foreign key columns
                    fk.delete(self.catalog.catalog, table)

            for fk in table.referenced_by:
                referenced_columns = [i['column_name'] for i in fk.referenced_columns]
                if self._key_in_columns(columns, referenced_columns,
                                        False):  # We are renaming one of the referenced columns.
                    referring_table = model.schemas[fk.sname].tables[fk.tname]
                    fk.delete(self.catalog.catalog, referring_table)

            for column in columns:
                self._delete_column_from_annotations(column)
                table.column_definitions[column].delete(self.catalog.catalog, table)
        return

    def _copy_columns(self, columns, dest_table, column_map={}):
        """
        Copy a set of columns, updating visible columns list and keys to mirror source column.
        :param columns: a list of columns
        :param dest_table: Table name of destination table
        :param column_map: A dictionary that specifies column name mapping
        :return:
        """

        column_name_map = self._column_map(column_map, 'name')
        nullok = self._column_map(column_map, 'nullok')
        default = self._column_map(column_map, 'default')
        comment = self._column_map(column_map, 'comment')

        with DerivaModel(self.catalog) as m:
            model = m.model()
            table = m.table(self)

            # TODO we need to figure out what to do about ACL binding
            target_table = m.table(dest_table)

            # Make sure that we can rename the columns
            overlap = {column_name_map.get(i, i) for i in columns}.intersection(dest_table._column_names())
            if len(overlap) != 0:
                raise ValueError('Column {} already exists.'.format(overlap))

            self._check_composite_keys(columns, dest_table)

            # Create a new column_spec from the existing spec.
            for from_column in columns:
                from_def = table.column_definitions[from_column]
                target_table.create_column(self.catalog.catalog,
                                           em.Column.define(
                                               column_name_map.get(from_column, from_column),
                                               from_def.type,
                                               nullok=nullok.get(from_column, from_def.nullok),
                                               default=default.get(from_column, from_def.default),
                                               comment=comment.get(from_column, from_def.comment),
                                               acls=from_def.acls,
                                               acl_bindings=from_def.acl_bindings,
                                               annotations=from_def.annotations
                                           ))

            # Copy over the old values
            from_path = self.datapath()
            to_path = dest_table.datapath()
            rows = from_path.entities(**{column_name_map.get(i, i): getattr(from_path, i) for i in columns + ['RID']})
            to_path.update(rows)

            # Copy over the keys.
            self._copy_keys(columns, column_name_map, dest_table)
        return

    def rename_column(self, from_column, to_column, default=None, nullok=None):
        """
        Rename a column by copying it and then deleting the origional column.
        :param from_column:
        :param to_column:
        :param default:
        :param nullok:
        :return:
        """
        column_map = {from_column: {'name': to_column}}
        if default:
            column_map['default'] = default
        if nullok:
            column_map['nullok'] = nullok

        self.rename_columns([from_column], self, column_map={from_column: to_column})
        return

    def rename_columns(self, columns, dest_table, column_map, delete=True):
        """
        Rename a column by copying it and then deleting the origional column.
        :param columns:
        :param dest_table:
        :param column_map:
        :param delete:
        :return:
        """
        with DerivaModel(self.catalog) as m:
            table = m.table(self)
            self._copy_columns(columns, dest_table, column_map=column_map)
            # Update column name in ACL bindings....
            self._rename_columns_in_acl_bindings(column_map)

            # Update annotations where the old spec was being used
            table.annotations.update(self._rename_columns_in_annotations(column_map, dest_table))
            if delete:
                self.delete_columns(columns)
        return

    def delete_table(self):
        with DerivaModel(self.catalog) as m:
            model = m.model()
            table = m.table(self)

            if table.referenced_by:
                DerivaCatalogError('Attept to delete catalog with incoming foreign keys')
            # Now we can delete the table.
            table.delete(self.catalog.catalog, schema=model.schemas[self.schema_name])
            self.deleted = True

    def _relink_table(self, dest_table, column_name_map={}):
        with DerivaModel(self.catalog) as m:
            model = m.model()
            table = model.table(self)

            for fk in table.referenced_by:
                fk_def = self._rename_column_in_fkey(
                    fk, self._column_names(),
                    column_name_map,
                    dest_table,
                    incoming=True)
                if fk_def != fk:
                    referring_table = model.schemas[fk.sname].tables[fk.tname]
                    sname = fk.sname
                    tname = fk.tname
                    fk.delete(self.catalog.catalog, referring_table)
                    self.catalog.schema(sname).table(tname).create_fkey(fk_def)

            self.catalog.update_referenced_by()

    def copy_table(self, schema_name, table_name, column_map={}, clone=False, column_fill={},
                   column_defs=[],
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
        :param column_fill:
        :param column_defs:
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

            # Get new key and fkey definitions by mapping to new column names.
            keys = [
                self._rename_column_in_key(k, self._column_names(), column_map, proto_table)
                for k in table.keys
            ]
            fkeys = [
                self._rename_column_in_fkey(fk, self._column_names(), column_map, proto_table)
                for fk in table.foreign_keys
            ]

            # Add foreign key name mappings...
            column_map.update({i[0]: i[1] for i in
                               zip([i.names[0][1] for i in table.foreign_keys],
                                   [i['names'][0][1] for i in fkeys])})

            new_columns = [c['name'] for c in column_defs]

            # Create new table
            new_table_def = em.Table.define(
                table_name,

                # Use column_map to change the name of columns in the new table.
                column_defs=[
                                em.Column.define(
                                    column_map.get(i.name, i.name),
                                    i.type,
                                    nullok=i.nullok,
                                    default=i.default,
                                    comment=i.comment,
                                    acls={**i.acls, **acls}, acl_bindings={**i.acl_bindings, **acl_bindings},
                                    annotations=self._rename_columns_in_column_annotations(i.annotations, column_map)
                                )
                                for i in table.column_definitions if i.name not in new_columns
                            ] + column_defs,
                key_defs=keys + key_defs,
                fkey_defs=fkeys + fkey_defs,
                comment=comment if comment else table.comment,
                acls=table.acls,
                acl_bindings=table.acl_bindings,
                annotations=self._rename_columns_in_annotations(column_map, proto_table)
            )

            # Create new table
            new_table = self.catalog.schema(schema_name)._create_table(new_table_def)
            new_table.table = table_name
            new_table.schema = schema_name
            new_table.visible_columns().insert_sources(new_columns)

            # Copy over values from original to the new one, mapping column names where required. Use the column_fill
            # argument to provide values for non-null columns.
            pb = self.catalog.getPathBuilder()
            from_path = pb.schemas[self.schema_name].tables[self.table_name]
            to_path = pb.schemas[schema_name].tables[table_name]
            rows = map(lambda x: {**x, **column_fill},
                       from_path.entities(
                           **{column_map.get(i, i): getattr(from_path, i) for i in from_path.column_definitions})
                       )
            to_path.insert(rows, **({'nondefaults': {'RID', 'RCT', 'RCB'}} if clone else {}))
        return new_table

    def move_table(self, schema_name, table_name, column_fill={},
                   delete_table=True,
                   column_map={},
                   column_defs=[],
                   key_defs=[],
                   fkey_defs=[],
                   comment=None,
                   acls={},
                   acl_bindings={},
                   annotations={}
                   ):
        with DerivaModel(self.catalog) as m:
            new_table = self.copy_table(schema_name, table_name, clone=True, column_fill=column_fill,
                                        column_map=column_map,
                                        column_defs=column_defs,
                                        key_defs=key_defs,
                                        fkey_defs=fkey_defs,
                                        comment=comment,
                                        acls=acls,
                                        acl_bindings=acl_bindings,
                                        annotations=annotations)

        # Redirect incoming FKs to the new table...
        self._relink_table(new_table, column_map)
        if delete_table:
            self.delete_table()
        self.schema_name = schema_name
        self.table_name = table_name

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
            table = model.schemas[self.schema_name].tables[self.table_name]
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

        with DerivaModel(self.catalog) as m:
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
        if not ({'ID', 'URI', 'Description', 'Name'} < term_table._column_names()):
            raise DerivaCatalogError('Attempt to link_vocabulary on a non-vocabulary table')

        self.link_tables(column_name, term_table, target_column='ID')
        return

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
