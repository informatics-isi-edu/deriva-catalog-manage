import sys
import logging
import traceback
import requests
from requests import HTTPError
from enum import Enum
from sortedcollections import OrderedDict
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import MultiKeyedList

from deriva.core.base_cli import BaseCLI
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.core import ErmrestCatalog, get_credential, format_exception
from deriva.core.utils import eprint

from deriva.utils.catalog.version import __version__ as VERSION

IS_PY2 = (sys.version_info[0] == 2)
IS_PY3 = (sys.version_info[0] == 3)

logger = logging.getLogger(__name__)

CATALOG_CONFIG__TAG = 'tag:isrd.isi.edu,2019:catalog-config'


class DerivaConfigError(Exception):
    def __init__(self, msg):
        self.msg = msg


class DerivaModel:
    class Context(Enum):
        compact="compact"
        compact_brief = "compact/brief"
        compact_select = "compact/select"
        detailed = "detailed"
        entry = "entry"
        entry_edit = "entry/edit"
        entry_create = "entry/create"
        filter = "filter"
        row_name="row_name"
        row_name_title="row_name/title"
        row_name_compact="row_name/compact"
        row_name_detailed="row_name/detailed"
        star="*"
        all="all"

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

    def schema(self, schema_name):
        return self.catalog.model.schemas[schema_name]

    def table(self, schema_name, table_name):
        return self.catalog.model.schemas[schema_name].tables[table_name]


class DerivaCatalog():
    def __init__(self, catalog_or_host, scheme='https', catalog_id=1):
        """
        Initialize a DerivaCatalog.  This can be done one of two ways: by passing in an Ermrestcatalog object, or
        specifying the host and catalog id of the desired catalog.
        :param catalog_or_host:
        :param scheme:
        :param catalog_id:
        """

        self.nesting = 0

        self.catalog = ErmrestCatalog(scheme, catalog_or_host, catalog_id, credentials=get_credential(catalog_or_host)) \
            if type(catalog_or_host) is str else catalog_or_host

        self.model = self.catalog.getCatalogModel()
        self.schema_classes = {}


    def apply(self):
        self.model.apply(self.catalog)
        return self

    def refresh(self):
        self.model.apply(self.catalog)
        self.model = self.catalog.getCatalogModel()

    def display(self):
        for i in self.model.schemas:
            print('{}'.format(i))

    def getPathBuilder(self):
        return self.catalog.getPathBuilder()

    def _make_schema_instance(self, schema_name):
        return DerivaSchema(self, schema_name)

    def schema(self, schema_name):
        if self.model.schemas[schema_name]:
            return self.schema_classes.setdefault(schema_name, self._make_schema_instance(schema_name))

    def update_referenced_by(self):
        """Introspects the 'foreign_keys' and updates the 'referenced_by' properties on the 'Table' objects.
        :param model: an ERMrest model object
        """
        for schema in self.model.schemas.values():
            for table in schema.tables.values():
                table.referenced_by = MultiKeyedList([])
        self.model.update_referenced_by()


    def create_schema(self, schema_name, comment=None, acls={}, annotations={}):
        schema = self.model.create_schema(self.catalog,
                                          em.Schema.define(
                                              schema_name,
                                              comment=comment,
                                              acls=acls,
                                              annotations=annotations
                                          )
                                          )
        return self.schema(schema_name)

    def refresh(self):
        assert(self.nesting == 0)
        logger.debug('Refreshing model')
        self.model.apply(self.catalog)
        self.model = self.catalog.getCatalogModel()

    def get_groups(self):
        if chaise_tags.catalog_config in self.model.annotations:
            return self.model.annotations[chaise_tags.catalog_config]['groups']
        else:
            raise DerivaConfigError(msg='Attempting to configure table before catalog is configured')


class DerivaSchema:
    def __init__(self, catalog, schema_name):
        self.catalog = catalog
        self.schema_name = schema_name
        self.table_classes = {}

    def display(self):
        for t in self.catalog.model.schemas[self.schema_name].tables:
            print('{}'.format(t))


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
        schema = self.catalog.model.schemas[self.schema_name]
        schema.create_table(self.catalog.catalog, table_def)
        return self.table(table_def['table_name'])

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


class DerivaVisibleColumns:
    def __init__(self, table):
        self.table = table

    def insert_visible_columns(self, column_names, position={}, contexts={}, create=False):
        """
        Create a general visible columns annotation spec that would be consistant with what chaise does by default.
        This spec can then be added to a table and edited for user preference.
        :return:
        """

        # Get the contexts list into a standard form...
        if type(contexts) is str:
            contexts = {DerivaModel.Context(contexts)}
        elif type(contexts) is DerivaModel.Context:
            contexts = {contexts}
        elif contexts == {} or contexts == DerivaModel.Context.all:
            contexts = {i for i in DerivaModel.Context}

        with DerivaModel(self.table.catalog) as m:
            table = m.model().table(self.table.schema_name, self.table.table_name)

            # Create any missing contexts
            if create:
                vcs = self.table._visible_columns()
                for i in contexts:
                    if i.value not in vcs.keys():
                        vcs[i.value] = []

            # Identify any columns that are references to assets and collect up associated columns.
            skip_columns , assets = [], []
            for col in column_names:
                if chaise_tags.asset in table.column_definitions[col].annotations:
                    assets.append(col)
                    skip_columns.extend(table.column_definitions[col][chaise_tags.asset].values())

            # Go through the list of foreign keys and create a list of key columns and referenced columns.
            fkey_names, fkey_cols = {},{}
            for fk in table.foreign_keys:
                ckey = [c['column_name'] for c in fk.foreign_key_columns] # List of names in composite key.
                if len(ckey) == 1:
                    fkey_names[ckey[0]] = fk.names[0]
                    fkey_cols[fk.names[0][1]] = ckey[0]

            vcs = {}
            for context, vc_list in self.table._visible_columns().items():
                # Get list of column names that are in the spec, mapping back simple FK references.
                if DerivaModel.Context(context) not in contexts:
                    continue

                vc_names = [DerivaSourceSpec(self.table, i)._referenced_columns() for i in vc_list]

                new_vc = vc_list[:]
                for col in column_names:
                    if (context == 'entry' and col in skip_columns) or col in vc_names:
                        # Skip over asset columns in entry context and make sure we don't have repeat column specs.
                        continue
                    # TODO this should check to see if target is a vocabulary and ID should be used instead of RID
                    new_spec = {'source': [{'outbound': fkey_names[col]}, 'RID'] if col in fkey_names else col}
                    new_vc.append(new_spec)
                    vc_names.append(col)

                vcs[context] = new_vc
            vcs= self._reorder_visible_columns(vcs, position)

            # All is good, so update the visible columns annotation.
            self.table.set_annotation(chaise_tags.visible_columns, {**self.table._visible_columns(), **vcs})

    def rename_columns_in_visible_columns(self, column_name_map, dest_sname, dest_tname):
        vc = {
            k: [
                j for i in v for j in (
                    [i] if (DerivaSourceSpec(self.table, i).rename_column(column_name_map, dest_sname, dest_tname) == i)
                    else [DerivaSourceSpec(self.table, i).rename_column(column_name_map, dest_sname, dest_tname)]
                )
            ] for k, v in self.table.annotations()[chaise_tags.visible_columns].items()
        }
        return vc

    def delete_visible_columns(self, columns, contexts=[]):
        context_names = [i.value for i in (DerivaModel.Context if contexts == [] else contexts)]

        for context, vc_list in self.table._visible_columns().items():
            # Get list of column names that are in the spec, mapping back simple FK references.
            if context not in context_names:
                continue
            vc_names = [self._spec_to_column(i) for i in vc_list]
            for col in columns:
                if col in vc_names:
                    del vc_list[vc_names.index(col)]
                    vc_names.remove(col)
        return

    def copy_visible_columns(self, from_context, ):
        pass

    def reorder_visible_columns(self, positions):
        vc = self._reorder_visible_columns(self.table._visible_columns(), positions)
        self.table.set_annotation(chaise_tags.visible_columns, {**self.table._visible_columns(), **vc})

    def _reorder_visible_columns(self, visible_columns, positions):
        """
        Reorder the columns in a visible columns specification.  Order is determined by the positions argument. The
        form of this is a dictionary whose elements are:
            context: {key_column: column_list, key_column:column_list}
        The columns in the specified context are then reorded so that the columns in the column list follow the column
        in order.  Key column specs are processed in order specified. The context name 'all' can be used to indicate
        that the order should be applied to all contexts currently in the visible_columns annotation.  The context name
        can also be omitted an positions can be in the form of {key_column: columnlist, ...} and the context all is
        implied.

        :param positions:
        :param context:
        :return:
        """
        if positions == {}:
            return visible_columns

        # Set up positions to apply to all contexts if you have {key_column: column_list} form.
        positions = OrderedDict(positions) if positions.keys() in DerivaModel.Context \
            else OrderedDict({DerivaModel.Context.all: positions})
        position_contexts = \
            {
                i.value for i in \
                (DerivaModel.Context if positions.keys() == {} or positions.keys() == {DerivaModel.Context.all} \
                 else positions.keys())
            }

        new_vc = {}
        for context, vc_list in visible_columns.items():
            if context not in position_contexts:
                continue

            # Get the list of column names for the spec.
            vc_names = [DerivaSourceSpec(self.table, i).column_name for i in vc_list]

            # Now build up a map that has the indexes of the reordered columns.  Include the columns in order
            # Unless they are in the column_list, in which case, insert them immediately after the key column.
            reordered_names = vc_names[:]
            for key_col, column_list in positions.get(context, positions[DerivaModel.Context.all]).items():
                if  not (set(column_list + [key_col]) <= set(vc_names)):
                    raise DerivaConfigError('Invalid position specificaion in reorder columns')
                mapped_list = [j for i in reordered_names if i not in column_list
                            for j in [i] + (column_list if i == key_col else [])
                ]
                reordered_names = mapped_list

            new_vc[context] = [vc_list[vc_names.index(i)] for i in reordered_names]
        return {**visible_columns, **new_vc}

    def validate(self):
        for c, l in self.table._visible_columns().items():
            for j in l:
                DerivaSourceSpec(self.table, j)

    def display(self):
        print(self.table._visible_columns())


class DerivaSourceSpec:
    def __init__(self, table, spec):
        self.table = table
        self.spec = spec
        self.source = self.normalize_column_entry(spec)['source']
        self.column_name = self._referenced_columns()

    def normalize_column_entry(self, spec):
        with DerivaModel(self.table.catalog) as m:
            table_m = m.table(self.table.schema_name, self.table.table_name)
            if type(spec) is str:
                table_m.column_definitions[spec]
                return {'source': spec}
            if isinstance(spec, (tuple,list)) and len(spec) == 2:
                spec = tuple(spec)
                if spec in self.table.keys().elements:
                    return {'source': self.table.keys()[spec].unique_columns[0]}
                elif spec in self.table.foreign_keys().elements:
                    return {'source': [{'outbound': spec}, 'RID']}
                else:
                    raise DerivaConfigError(f'Invalid source entry {spec}')
            else:
                 return self.normalize_source_entry(spec)

    def normalize_source_entry(self, spec):
        with DerivaModel(self.table.catalog) as m:
            table_m = m.table(self.table.schema_name, self.table.table_name)

            source_entry = spec['source']
            if type(source_entry) is str:
                table_m.column_definitions[source_entry]
                return spec

            # We have a path of FKs so follow the path to make sure that all of the constraints line up.
            path_table = table_m

            for c in source_entry[0:-1]:
                if 'inbound' in c and len(c['inbound']) == 2:
                    k = tuple(c['inbound'])
                    target_schema = path_table.referenced_by[k].foreign_key_columns[0]['schema_name']
                    target_table = path_table.referenced_by[k].foreign_key_columns[0]['table_name']
                    path_table = m.table(target_schema, target_table)
                elif 'outbound' in c and len(c['outbound']) == 2:
                    k = tuple(c['outbound'])
                    target_schema = path_table.foreign_keys[k].referenced_columns[0]['schema_name']
                    target_table = path_table.foreign_keys[k].referenced_columns[0]['table_name']
                    path_table = m.table(target_schema, target_table)
                else:
                   raise DerivaConfigError(f'Invalid source entry {c}')

            if source_entry[-1] not in path_table.column_definitions.elements:
                raise DerivaConfigError(f'Invalid source entry {source_entry[-1]}')
        return spec

    def rename_column(self, column_map, dest_sname, dest_tname):
        if type(self.source) is str:
            return {**self.spec, **{'source': column_map.get(self.source, self.source)}}
        # We have a FK list....
        if type(self.source) is list and len(self.source) == 2:
            return {
                **self.spec,
                **{'source':
                       (
                        {
                            'outbound': self.table._update_key_name(self.source[0]['outbound'], column_map, dest_sname, dest_tname)
                        },
                        self.source[1]
                       )}
            }
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
        return {'pseudo_column': self.source}


class DerivaTable:
    def __init__(self, catalog, schema_name, table_name):
        self.catalog =  catalog
        self.schema_name = schema_name
        self.table_name = table_name

    def annotations(self):
        with DerivaModel(self.catalog) as m:
            return m.table(self.schema_name, self.table_name).annotations

    def set_annotation(self, annotation, value):
        with DerivaModel(self.catalog) as m:
            m.table(self.schema_name, self.table_name).annotations.update({annotation:value})

    def visible_columns(self):
        return DerivaVisibleColumns(self)

    def _visible_columns(self):
        with DerivaModel(self.catalog) as m:
            table = m.table(self.schema_name, self.table_name)
            if chaise_tags.visible_columns not in table.annotations:
                table.annotations[chaise_tags.visible_columns] = {}
            return m.table(self.schema_name, self.table_name).annotations[chaise_tags.visible_columns]

    def create_key(self, key_def):
        with DerivaModel(self.catalog) as m:
            m.table(self.schema_name, self.table_name).create_key(self.catalog.catalog, key_def)

    def keys(self):
        with DerivaModel(self.catalog) as m:
            return m.table(self.schema_name, self.table_name).keys

    def foreign_keys(self):
        with DerivaModel(self.catalog) as m:
            return m.table(self.schema_name, self.table_name).foreign_keys

    def create_fkey(self, fkey_def):
        with DerivaModel(self.catalog) as m:
            target_schema = fkey_def['referenced_columns'][0]['schema_name']
            target_table = fkey_def['referenced_columns'][0]['table_name']
            fkey = m.table(self.schema_name, self.table_name).create_fkey(self.catalog.catalog, fkey_def)
            m.model().schemas[target_schema].tables[target_table].referenced_by.append(fkey)

    def link_tables(self, column_name, target_schema, target_table, target_column='RID'):
        """
        Create a foreign key link from the specified column to the target table and column.
        :param column_name: Column or list of columns in current table which will hold the FK
        :param target_schema:
        :param target_table:
        :param target_column:
        :return:
        """

        with DerivaModel(self.catalog) as m:
            if type(column_name) is str:
                column_name = [column_name]
            table = m.model().schemas[self.schema_name].tables[self.table_name]
            self.create_fkey(
                em.ForeignKey.define(column_name,
                                     target_schema, target_table,
                                     target_column if type(target_column) is list else [
                                         target_column],
                                     constraint_names=[(self.schema_name,
                                                        '_'.join([self.table_name] +
                                                                 column_name +
                                                                 ['fkey']))],
                                     )
            )
        return

    def link_vocabulary(self, column_name, term_schema, term_table):
        """
        Set an existing column in the table to refer to an existing vocabulary table.
        :param column_name: Name of the column whose value is to be from the vocabular
        :param term_schema: Schema name of the term table
        :param term_table: Name of the term table.
        :return: None.
        """
        self.link_tables(column_name, term_schema, term_table, target_column='ID')
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

    @staticmethod
    def _map_columns_in_display(dval, column_name_map):
        def rename_markdown_pattern(pattern):
            # Look for column names {{columnname}} in the templace and update.
            for k, v in column_name_map:
                pattern = pattern.replace('{{{}}}'.format(k), '{{{}}}'.format(v))
            return pattern

        return {
            k: rename_markdown_pattern(v) if k == 'markdown_name' else v
            for k, v in dval.items()
        }

    def _update_columns_in_visible_columns(self, column_map, new_columns):
        vc = self.visible_columns()
        vc.rename_columns_in_visible_columns(column_map)
        vc.insert_visible_columns(new_columns)

    def _rename_columns_in_annotations(self, column_name_map, dest_sname, dest_tname):
        return {
            k: self._update_columns_in_display(v, column_name_map) if k == chaise_tags.display else
               self.visible_columns().rename_columns_in_visible_columns(column_name_map, dest_sname, dest_tname)
               if k == chaise_tags.visible_columns else
            v
            for k, v in self.annotations().items()
        }

    def _delete_column_from_annotations(self, column_name):
        with DerivaModel(self.catalog) as m:
            model = m.model()
            table = model.schemas[self.schema_name].tables[self.table_name]
            return {
                k: self._delete_from_visible_columns(v, column_name) if k == chaise_tags._visible_columns else v
                for k, v in table.annotations.items()
            }

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
            raise DerivaConfigError(msg='Cannot rename part of compound key')
        return True

    def _check_composite_keys(self, columns, dest_sname, dest_tname, rename=None):
        """
        Go over all of the keys, incoming and outgoing foreign keys and check to make sure that renaming the set of
        columns colulumns won't break up composite keys if they are renamed.
        :param columns:
        :param dest_sname:
        :param dest_tname:
        :param rename:
        :return:
        """
        local_rename = rename if rename is not None else \
            (self.schema_name == dest_sname and self.table_name == dest_tname)
        columns = set(columns)

        with DerivaModel(self.catalog) as m:
            table = m.model().schemas[self.schema_name].tables[self.table_name]
            for i in table.keys:
                self._key_in_columns(columns, i.unique_columns, local_rename)

            for fk in table.foreign_keys:
                self._key_in_columns(columns, [i['column_name'] for i in fk.foreign_key_columns], local_rename)

            for fk in table.referenced_by:
                self._key_in_columns(columns, [i['column_name'] for i in fk.referenced_columns], local_rename)

    def _update_key_name(self, name, column_name_map, dest_sname, dest_tname):
            # Helper function that creates a new constraint name by replacing table and column names.
            name = name[1].replace('{}_'.format(self.table_name), '{}_'.format(dest_tname))
            for k, v in column_name_map.items():
                name = name.replace(k, v)
            return dest_sname, name

    def _rename_column_in_fkey(self, fk, columns, column_name_map, dest_sname, dest_tname, incoming=False):
        """
        Given an existing FK, create a new FK that reflects column renaming.
        :param fk:
        :param columns:
        :param column_name_map:
        :param dest_sname:
        :param dest_tname:
        :return:
        """
        column_rename = self.schema_name == dest_sname and self.table_name == dest_tname

        # Rename the columns that appear in foreign keys...
        fk_columns = [i['column_name'] for i in fk.foreign_key_columns]
        referenced_columns = [i['column_name'] for i in fk.referenced_columns]

        return em.ForeignKey.define(
            [column_name_map.get(i, i) for i in fk_columns] if incoming==False else fk_columns,
            fk.referenced_columns[0]['schema_name'],
            fk.referenced_columns[0]['table_name'],
            [column_name_map.get(i, i) for i in referenced_columns] if incoming==True else referenced_columns,
            constraint_names=[self._update_key_name(n, column_name_map, dest_sname, dest_tname) for n in fk.names],
            comment=fk.comment,
            acls=fk.acls,
            acl_bindings=fk.acl_bindings,
            annotations=fk.annotations
        ) if self._key_in_columns(columns, fk_columns, column_rename) else fk

    def _rename_column_in_key(self, key, columns, column_name_map, dest_sname, dest_tname):
        column_rename = self.schema_name == dest_sname and self.table_name == dest_tname

        return em.Key.define(
            [column_name_map.get(c, c) for c in key.unique_columns],
            constraint_names=[self._update_key_name(n, column_name_map, dest_sname, dest_tname) for n in key.names],
            comment=key.comment,
            annotations=key.annotations
        ) if self._key_in_columns(columns, key.unique_columns, column_rename) else key

        # Now look through incoming foreign keys to make sure none of them changed.

    def _relink_table(self, columns, column_name_map, dest_sname, dest_tname):
        columns = set(columns)
        with DerivaModel(self.catalog) as m:
            model = m.model()
            dest_table = model.schemas[dest_sname].tables[dest_tname]
            table = model.schemas[self.schema_name].tables[self.table_name]
            for fk in table.referenced_by:
                fk_def = self._rename_column_in_fkey(fk, columns, column_name_map, dest_sname, dest_tname, incoming=True)
                if fk_def != fk:
                    referring_table = model.schemas[fk.sname].tables[fk.tname]
                    fk.delete(self.catalog.catalog, referring_table)
                    self.catalog.schema(fk.sname).table(fk.tname).create_fkey(fk_def)
            self.catalog.update_referenced_by()

    def _copy_keys(self, columns, column_name_map, dest_sname, dest_tname):
        """
        Copy over the keys from the current table to the destination table, renaming columns.
        :param columns:
        :param column_name_map:
        :param dest_sname:
        :param dest_tname:
        :return:
        """

        columns = set(columns)
        with DerivaModel(self.catalog) as m:
            model = m.model()
            dest_table = model.schemas[dest_sname].tables[dest_tname]
            table = model.schemas[self.schema_name].tables[self.table_name]

            for i in table.keys:
                if i.unique_columns == ['RID']:
                    continue  # RID Key constraint is already put in place by ERMRest.
                key_def = self._rename_column_in_key(i, columns, column_name_map, dest_sname, dest_tname)
                if key_def == i:
                    self.catalog.schema(dest_sname).table(dest_tname).create_key(key_def)

            # Rename the columns that appear in foreign keys...
            for fk in table.foreign_keys:
                fk_columns = [i['column_name'] for i in fk.foreign_key_columns]
                fk_def = self._rename_column_in_fkey(fk, columns, column_name_map, dest_sname, dest_tname)
                if fk_def != fk:
                    dest_table.create_fkey(self.catalog.catalog, fk_def)

    def _rename_columns_in_acl_bindings(self, column_name_map):
        with DerivaModel(self.catalog) as m:
            table = m.model().schemas[self.schema_name].tables[self.table_name]
            return table.acl_bindings

    def _rename_columns_in_column_annotations(self, annotation, column_name_map):
        return annotation

    def _add_fkeys(self, fkeys):
        with DerivaModel(self.catalog) as m:
            model = m.model()
            table = model.schemas[self.schema_name].tables[self.table_name]
            for fkey in fkeys:
                referenced = model.schemas[
                    fkey.referenced_columns[0]['schema_name']
                ].tables[
                    fkey.referenced_columns[0]['table_name']
                ]
                referenced.referenced_by.append(fkey)

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

    def delete_columns(self, columns):
        """
        Drop a column from a table, cleaning up visible columns and keys.
        :param columns:
        :return:
        """
        with DerivaModel(self.catalog) as m:
            model = m.model()
            table = model.schemas[self.schema_name].tables[self.table_name]

            self._check_composite_keys(columns, self.schema_name, self.table_name, rename=False)
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
                table.annotations = self._delete_column_from_annotations(column)
                table.column_definitions[column].delete(self.catalog.catalog, table)
        return

    def _copy_columns(self, columns, dest_sname, dest_tname, column_map={}):
        """
        Copy a set of columns, updating visible columns list and keys to mirror source column.
        :param columns: a list of columns
        :param dest_sname: Schema name of destination table
        :param dest_tname: Table name of destination table
        :param column_map: A dictionary that specifies column name mapping
        :return:
        """

        column_name_map = {k: v['name'] for k, v in column_map.items() if 'name' in v}
        nullok = {k: v['nullok'] for k, v in column_map.items() if 'nullok' in v}
        default = {k: v['default'] for k, v in column_map.items() if 'default' in v}
        comment = {k: v['comment'] for k, v in column_map.items() if 'comment' in v}

        with DerivaModel(self.catalog) as m:
            model = m.model()
            table = model.table(self.schema_name, self.table_name)

            # TODO we need to figure out what to do about ACL binding
            target_table = model.table(dest_sname, dest_tname)

            # Make sure that we can rename the columns
            overlap = {column_name_map.get(i, i) for i in columns}.intersection(
                {i.name for i in target_table.column_definitions})
            if len(overlap) != 0:
                raise ValueError('Column {} already exists.'.format(overlap))

            self._check_composite_keys(columns, dest_sname, dest_tname)

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
            pb = self.catalog.getPathBuilder()
            from_path = pb.schemas[self.schema_name].tables[self.table_name]
            to_path = pb.schemas[dest_sname].tables[dest_tname]
            rows = from_path.entities(**{column_name_map.get(i, i): getattr(from_path, i) for i in columns + ['RID']})
            to_path.update(rows)

            # Copy over the keys.
            self._copy_keys(columns, column_name_map, dest_sname, dest_tname)
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
        self.rename_columns([from_column], self.schema_name, self.table_name,
                            column_map={
                                from_column:
                                    {k: v for k, v in
                                     {'name': to_column,
                                      'nullok': nullok,
                                      'default': default
                                      }.items()
                                     if v is not None}}
                            )
        return

    def rename_columns(self, columns, dest_schema, dest_table, column_map, delete=True):
        """
        Rename a column by copying it and then deleting the origional column.
        :param columns:
        :param dest_schema:
        :param dest_table:
        :param column_map:
        :param delete:
        :return:
        """
        with DerivaModel(self.catalog) as m:
            table = m.model().table(self.schema_name, self.table_name)
            self._copy_columns(columns, dest_schema, dest_table, column_map=column_map)
            # Update column name in ACL bindings....
            self._rename_columns_in_acl_bindings(column_map)

            # Update annotations where the old spec was being used
            table.annotations.update(self._rename_columns_in_annotations(column_map))
            if delete:
                self.delete_columns(columns)
        return

    def delete_table(self):
        with DerivaModel(self.catalog) as m:
            model = m.model()
            table = m.table(self.schema_name, self.table_name)

            # Delete all of the incoming FKs
            columns = {i.name for i in table.column_definitions}
            for fk in table.referenced_by:
                referenced_columns = [i['column_name'] for i in fk.referenced_columns]
                if self._key_in_columns(columns, referenced_columns,
                                        False):  # We are renaming one of the referenced columns.
                    referring_table = model.schemas[fk.sname].tables[fk.tname]
                    fk.delete(self.catalog, referring_table)

            # Now we can delete the table.
            table.delete(self.catalog.catalog, schema=model.schemas[self.schema_name])
            self.table_name = None
            self.schema_name = None

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
        Key and foreign key definitions can be augmented or overwritten by providing approporiate arguements. Lastly
        if the clone arguement is set to true, the RIDs of the source table are reused, so that the equivelant of a
        move operation can be obtained.
        :param schema_name: Target schema name
        :param table_name:  Target table name
        :param column_map: A dictionary that is used to rename columns in the target table.
        :param clone:
        :param column_defs:
        :param key_defs:
        :param fkey_defs:
        :param comment:
        :param acls:
        :param acl_bindings:
        :param annotations:
        :return:
        """

        with DerivaModel(self.catalog) as m:
            model = m.model()
            table = model.schemas[self.schema_name].tables[self.table_name]

            # Get new key and fkey definitions by mapping to new column names.
            columns = [i.name for i in table.column_definitions]
            keys = [
                       self._rename_column_in_key(k, columns, column_map, schema_name, table_name)
                           for k in table.keys
                   ]
            fkeys = [
                        self._rename_column_in_fkey(fk, columns, column_map, schema_name, table_name)
                            for fk in table.foreign_keys
                    ]

            # Add foreign key name mappings...
            column_map.update({i[0]: i[1] for i in
                                zip([i.names[0][1] for i in table.foreign_keys],
                                    [i['names'][0][1] for i in fkeys])})

            annotations= {}

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
                                    acls=i.acls, acl_bindings=i.acl_bindings,
                                    annotations=self._rename_columns_in_column_annotations(i.annotations, column_map)
                                )
                                for i in table.column_definitions if i.name not in new_columns
                            ] + column_defs,
                key_defs=keys + key_defs,
                fkey_defs=fkeys + fkey_defs,
                comment=comment if comment else table.comment,
                acls=table.acls,
                acl_bindings=table.acl_bindings,
                annotations=self._rename_columns_in_annotations(column_map, schema_name, table_name)
            )

            # Create new table
            new_table = self.catalog.schema(schema_name)._create_table(new_table_def)
            new_table.visible_columns().insert_visible_columns(new_columns)

            # Copy over values from original to the new one, mapping column names where required. Use the column_fill
            # argument to provide values for non-null columns.
            pb = self.catalog.getPathBuilder()
            from_path = pb.schemas[self.schema_name].tables[self.table_name]
            to_path = pb.schemas[schema_name].tables[table_name]
            rows = map(lambda x:  {**x, **column_fill},
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
            model = m.model()
            table = m.table(self.schema_name, self.table_name)

            new_table = self.copy_table(schema_name, table_name, clone=True,column_fill=column_fill,
                            column_map=column_map,
                            column_defs=column_defs,
                            key_defs=key_defs,
                            fkey_defs=fkey_defs,
                            comment=comment,
                            acls=acls,
                            acl_bindings=acl_bindings,
                            annotations=annotations)

            # Redirect incoming FKs to the new table...
            self._relink_table([i.name for i in table.column_definitions],
                                      column_map, schema_name, table_name)
            if delete_table:
                self.delete_table()
            self.schema_name = schema_name
            self.table_name = table_name

        return

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
                        self.table_name : '{table_rid}',
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
            raise DerivaConfigError(msg='Attempting to configure table before catalog is configured')

        with DerivaModel(self.catalog) as m:
            model = m.model()
            table = model.schemas[self.schema_name].tables[self.table_name]
            if key_column not in [i.name for i in table.column_definitions]:
                raise DerivaConfigError(msg='Key column not found in target table')

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

    def datapath(self):
        return self.catalog.getPathBuilder().schemas[self.schema_name].tables[self.table_name]

    def entities(self, *attributes, **renamed_attributes):
        return self.datapath().entities(*attributes, **renamed_attributes)


class DerivaModelElementsCLI(BaseCLI):

    def __init__(self, description, epilog):
        """Initializes the CLI.
        """
        super(DerivaModelElementsCLI, self).__init__(description, epilog, VERSION, hostname_required=True)

        # initialized after argument parsing
        self.args = None
        self.host = None

        # parent arg parser
        parser = self.parser
        parser.add_argument('table', default=None, metavar='SCHEMA_NAME:TABLE_NAME',
                            help='Name of table to be configured')
        parser.add_argument('--catalog', default=1, help="ID number of desired catalog (Default:1)")
        parser.add_argument('--asset-table', default=None, metavar='KEY_COLUMN',
                            help='Create an asset table linked to table on key_column')
        parser.add_argument('--visible-columns', action='store_true',
                            help='Create a default visible columns annotation')
        parser.add_argument('--replace', action='store_true', help='Overwrite existing value')

    @staticmethod
    def _get_credential(host_name, token=None):
        if token:
            return {"cookie": "webauthn={t}".format(t=token)}
        else:
            return get_credential(host_name)

    def main(self):
        """Main routine of the CLI.
        """
        args = self.parse_cli()

        try:
            catalog = ErmrestCatalog('https', args.host, args.catalog, credentials=self._get_credential(args.host))
            [schema_name, table_name] = args.table.split(':')
            table = DerivaTable(catalog, schema_name, table_name)
            if args.asset_table:
                table.create_asset_table(args.asset_table)
            if args._visible_columns:
                table.create_default_visible_columns(really=args.replace)

        except HTTPError as e:
            if e.response.status_code == requests.codes.unauthorized:
                msg = 'Authentication required'
            elif e.response.status_code == requests.codes.forbidden:
                msg = 'Permission denied'
            else:
                msg = e
            logging.debug(format_exception(e))
            eprint(msg)
        except RuntimeError as e:
            sys.stderr.write(str(e))
            return 1
        except:
            traceback.print_exc()
            return 1
        finally:
            sys.stderr.write("\n\n")
        return 0


def main():
    DESC = "DERIVA Model Elements Command-Line Interface"
    INFO = "For more information see: https://github.com/informatics-isi-edu/deriva-catalog-manage"
    return DerivaModelElementsCLI(DESC, INFO).main()


if __name__ == '__main__':
    sys.exit(main())
