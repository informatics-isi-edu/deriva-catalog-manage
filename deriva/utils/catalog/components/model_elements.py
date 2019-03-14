import sys
import logging
import traceback
import requests
from requests import HTTPError
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


class DerivaTable:
    def __init__(self, catalog, schema_name, table_name):
        self.catalog =  catalog
        self.schema_name = schema_name
        self.table_name = table_name

    def set_annotation(self, annotation, value):
        with DerivaModel(self.catalog) as m:
            m.table(self.schema_name, self.table_name).annotations.update({annotation:value})

    def create_key(self, key_def):
        with DerivaModel(self.catalog) as m:
            m.table(self.schema_name, self.table_name).create_key(self.catalog.catalog, key_def)

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
    def _delete_from_visible_columns(vcols, column_name):
        def column_match(spec):
            # Helper function that determines if column is in the spec.
            if type(spec) is str and spec == column_name:
                return True
            if type(spec) is list and len(spec) == 2 and spec[1] == column_name:
                return True
            if type(spec) is dict:
                return column_match(spec['source'])
            else:
                return False

        return {
            k: [i for i in v if not column_match(i)]
            for k, v in vcols.items()
        }

    @staticmethod
    def _rename_columns_in_visible_columns(vcols, column_name_map):
        def map_column_spec(spec):
            if type(spec) is str and spec in column_name_map:
                return column_name_map[spec]
            if type(spec) is list and len(spec) == 2 and spec[1] in column_name_map:
                return [spec[0], column_name_map[spec[1]]]
            if type(spec) is dict:
                return {k: map_column_spec(v) if k == 'source' else v for k, v, in spec.items()}
            else:
                return spec

        return {
            k: [
                j for i in v for j in (
                    [i] if (map_column_spec(i) == i)
                    else [map_column_spec(i)]
                )
            ] for k, v in vcols.items()
        }

    @staticmethod
    def _insert_column_in_visible_columns(vcols, column_name):
        return {
            k: v.append({'source': column_name})
            for k, v in vcols.items()
        }

    @staticmethod
    def _rename_columns_in_display(dval, column_name_map):
        def rename_markdown_pattern(pattern):
            # Look for column names {{columnname}} in the templace and update.
            for k, v in column_name_map:
                pattern = pattern.replace('{{{}}}'.format(k), '{{{}}}'.format(v))
            return pattern

        return {
            k: rename_markdown_pattern(v) if k == 'markdown_name' else v
            for k, v in dval.items()
        }

    def _rename_columns_in_annotations(self, column_name_map):
        with DerivaModel(self.catalog) as m:
            model = m.model()
            table = model.schemas[self.schema_name].tables[self.table_name]
            return {
                k:
                    self._rename_columns_in_visible_columns(v, column_name_map) if k == chaise_tags.visible_columns else
                    self._rename_columns_in_display(v, column_name_map) if k == chaise_tags.display else
                    v
                for k, v in table.annotations.items()
            }

    def _delete_column_from_annotations(self, column_name):
        with DerivaModel(self.catalog) as m:
            model = m.model()
            table = model.schemas[self.schema_name].tables[self.table_name]
            return {
                k: self._delete_from_visible_columns(v, column_name) if k == chaise_tags.visible_columns else v
                for k, v in table.annotations.items()
            }

    @staticmethod
    def _key_match(columns, key_columns, rename):
        overlap = columns.intersection(set(key_columns))
        if len(overlap) == 0:
            return False
        if not rename and len(overlap) < len(key_columns):
            raise DerivaConfigError(msg='Cannot rename part of compound key')
        return True

    def _check_composite_keys(self, columns, dest_sname, dest_tname, rename=None):
        """
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
                self._key_match(columns, i.unique_columns, local_rename)

            for fk in table.foreign_keys:
                self._key_match(columns, [i['column_name'] for i in fk.foreign_key_columns], local_rename)

            for fk in table.referenced_by:
                self._key_match(columns, [i['column_name'] for i in fk.referenced_columns], local_rename)

    def _rename_columns_in_keys(self, columns, column_name_map, dest_sname, dest_tname):
        """
        Rename incoming foreign keys to this table so that they still refer to the same columns after columns have
        been renamed according to column_name_map and move to a new schema and table.
        :param columns:
        :param column_name_map:
        :param dest_sname:
        :param dest_tname:
        :return:
        """

        def update_key_name(name):
            # Helper function that creates a new constraint name by replacing table and column names.
            name = name[1].replace('{}_'.format(self.table_name), '{}_'.format(dest_tname))
            for k, v in column_name_map.items():
                name = name.replace(k, v)
            return dest_sname, name

        def def_fkey(fk, fk_columns, sname, tname, referenced_columns, names):
            return em.ForeignKey.define(
                fk_columns,
                sname, tname, referenced_columns,
                on_update=fk.on_update, on_delete=fk.on_delete,
                constraint_names=names,
                comment=fk.comment,
                acls=fk.acls,
                acl_bindings=fk.acl_bindings,
                annotations=fk.annotations
            )

        column_rename = self.schema_name == dest_sname and self.table_name == dest_tname
        columns = set(columns)
        with DerivaModel(self.catalog) as m:
            model = m.model()
            dest_table = model.schemas[dest_sname].tables[dest_tname]
            table = model.schemas[self.schema_name].tables[self.table_name]

            for i in table.keys:
                if i.unique_columns == ['RID']:
                    continue  # RID Key constraint is already put in place by ERMRest.
                if self._key_match(columns, i.unique_columns, column_rename):
                    self.catalog.schema(dest_sname).table(dest_tname).create_key(
                                          em.Key.define(
                                              [column_name_map.get(c, c) for c in i.unique_columns],
                                              constraint_names=[update_key_name(n) for n in i.names],
                                              comment=i.comment,
                                              annotations=i.annotations
                                          )
                                          )
                    i.delete(self.catalog.catalog, table)

            # Rename the columns that appear in foreign keys...
            for fk in table.foreign_keys:
                fk_columns = [i['column_name'] for i in fk.foreign_key_columns]
                if self._key_match(columns, fk_columns,
                                   column_rename):  # We are renaming one of the foreign key columns
                    referenced_schema = fk.referenced_columns[0]['schema_name']
                    referenced_table = fk.referenced_columns[0]['table_name']
                    fk_def = def_fkey(fk,
                                      [column_name_map.get(i, i) for i in fk_columns],
                                      referenced_schema,
                                      referenced_table,
                                      [i['column_name'] for i in fk.referenced_columns],
                                      [update_key_name(n) for n in fk.names]
                                      )
                    new_fkey = dest_table.create_fkey(self.catalog.catalog, fk_def)
                    fk.delete(self.catalog.catalog, table)

            # Now look through incoming foreign keys to make sure none of them changed.
            for fk in table.referenced_by:
                referenced_columns = [i['column_name'] for i in fk.referenced_columns]
                if self._key_match(columns, referenced_columns,
                                   column_rename):  # We are renaming one of the referenced columns.
                    fk_def = def_fkey(fk,
                                      [i['column_name'] for i in fk.foreign_key_columns],
                                      dest_sname, dest_tname, [column_name_map.get(i, i) for i in referenced_columns],
                                      fk.names)
                    referring_table = model.schemas[fk.sname].tables[fk.tname]
                    fk.delete(self.catalog.catalog, referring_table)
                    self.catalog.schema(fk.sname).table(fk.tname).create_fkey(fk_def)
            self.catalog.update_referenced_by()

    def _rename_columns_in_acl_bindings(self, column_name_map):
        with DerivaModel(self.catalog) as m:
            table = m.model().schemas[self.schema_name].tables[self.table_name]
            return table.acl_bindings

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
                if self._key_match(columns, i.unique_columns, False):
                    i.delete(self.catalog.catalog, table)

            for fk in table.foreign_keys:
                fk_columns = [i['column_name'] for i in fk.foreign_key_columns]
                if self._key_match(columns, fk_columns, False):  # We are renaming one of the foreign key columns
                    fk.delete(self.catalog.catalog, table)

            for fk in table.referenced_by:
                referenced_columns = [i['column_name'] for i in fk.referenced_columns]
                if self._key_match(columns, referenced_columns,
                                   False):  # We are renaming one of the referenced columns.
                    referring_table = model.schemas[fk.sname].tables[fk.tname]
                    fk.delete(self.catalog.catalog, referring_table)

            for column in columns:
                table.annotations = self._delete_column_from_annotations(column)
                table.column_definitions[column].delete(self.catalog.catalog, table)
        return

    def _rename_columns(self, columns, dest_sname, dest_tname, column_map={}):
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
            table = model.schemas[self.schema_name].tables[self.table_name]

            # TODO we need to figure out what to do about ACL binding
            target_table = model.schemas[dest_sname].tables[dest_tname]
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
            self._rename_columns_in_keys(columns, column_name_map, dest_sname, dest_tname)

            # Update column name in ACL bindings....
            table.annotations['acl_bindings'] = self._rename_columns_in_acl_bindings(column_name_map)

            # Update annotations where the old spec was being used
            table.annotations = self._rename_columns_in_annotations(column_name_map)

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
        self._rename_columns(columns, dest_schema, dest_table, column_map=column_map)
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
                if self._key_match(columns, referenced_columns,
                                   False):  # We are renaming one of the referenced columns.
                    referring_table = model.schemas[fk.sname].tables[fk.tname]
                    fk.delete(self.catalog, referring_table)

            # Now we can delete the table.
            table.delete(self.catalog.catalog, schema=model.schemas[self.schema_name])
            self.table_name = None
            self.schema_name = None

    def copy_table(self, schema_name, table_name, column_map={}, clone=False,
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
                                    annotations=i.annotations
                                )
                                for i in table.column_definitions if i.name not in {c['name']: c for c in column_defs}
                            ] + column_defs,
                key_defs=key_defs,
                fkey_defs=fkey_defs,

                comment=comment if comment else table.comment,
                acls=table.acls,
                acl_bindings=table.acl_bindings,

                # Update visible columns to account for column_map
                annotations=self._rename_columns_in_annotations(column_map)
            )

            # Create new table
            new_table = self.catalog.schema(schema_name)._create_table(new_table_def)

            # Copy over values from original to the new one, mapping column names where required.
            pb = self.catalog.getPathBuilder()
            from_path = pb.schemas[self.schema_name].tables[self.table_name]
            to_path = pb.schemas[schema_name].tables[table_name]
            rows = from_path.entities(
                **{column_map.get(i, i): getattr(from_path, i) for i in from_path.column_definitions})
            to_path.insert(rows, **({'nondefaults': {'RID', 'RCT', 'RCB'}} if clone else {}))
        return new_table

    def move_table(self, schema_name, table_name,
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
            table = model.schemas[self.schema_name].tables[self.table_name]

            self.copy_table(schema_name, table_name, clone=True,
                            column_map=column_map,
                            column_defs=column_defs,
                            key_defs=key_defs,
                            fkey_defs=fkey_defs,
                            comment=comment,
                            acls=acls,
                            acl_bindings=acl_bindings,
                            annotations=annotations)

            self._rename_columns_in_keys([i.name for i in table.column_definitions],
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

    def entities(self):
        self.datapath.entities()


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
            if args.visible_columns:
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
