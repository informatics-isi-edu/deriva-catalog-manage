import sys
import logging
import traceback
import requests
from requests import HTTPError
import deriva.core.ermrest_model as em
from deriva.core.base_cli import BaseCLI
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.core import ErmrestCatalog, get_credential, format_exception
from deriva.core.utils import eprint
from deriva.utils.catalog.manage.configure_catalog import DerivaTableConfigure
from deriva.utils.catalog.version import __version__ as VERSION

IS_PY2 = (sys.version_info[0] == 2)
IS_PY3 = (sys.version_info[0] == 3)

logger = logging.getLogger(__name__)

CATALOG_CONFIG__TAG = 'tag:isrd.isi.edu,2019:catalog-config'


class DerivaConfigError(Exception):
    def __init__(self, msg):
        self.msg = msg


class DerivaTable(DerivaTableConfigure):
    def __init__(self, catalog, schema_name, table_name, model=None):
        super(DerivaTable, self).__init__(catalog, schema_name, table_name, model=model)

    def link_tables(self, column_name, target_schema, target_table, target_column='RID'):
        """
        Create a foreign key link from the specified column to the target table and column.
        :param column_name: Column or list of columns in current table which will hold the FK
        :param target_schema:
        :param target_table:
        :param target_column:
        :return:
        """

        if type(column_name) is str:
            column_name = [column_name]
        self.table.create_fkey(self.catalog,
                               em.ForeignKey.define(column_name,
                                                    target_schema, target_table,
                                                    target_column if type(target_column) is list else [target_column],
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
        association_table = self.model.schemas[self.schema_name].create_table(self.catalog, table_def)
        return DerivaTable(self.catalog, association_table.sname, association_table.name, model=self.model)

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
    def _rename_columns_in_visible_columns(vcols, column_map):
        def map_column_spec(spec):
            if type(spec) is str and spec in column_map:
                return column_map[spec]
            if type(spec) is list and len(spec) == 2 and spec[1] in column_map:
                return [spec[0], column_map[spec[1]]]
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
    def _rename_columns_in_display(dval, column_map):
        def rename_markdown_pattern(pattern):
            # Look for column names {{columnname}} in the templace and update.
            for k, v in column_map:
                pattern = pattern.replace('{{{}}}'.format(k), '{{{}}}'.format(v))
            return pattern

        return {
            k: rename_markdown_pattern(v) if k == 'markdown_name' else v
            for k, v in dval.items()
        }

    def _rename_columns_in_annotations(self, column_map):
        return {
            k:
                self._rename_columns_in_visible_columns(v, column_map) if k == chaise_tags.visible_columns else
                self._rename_columns_in_display(v, column_map) if k == chaise_tags.display else
                v
            for k, v in self.table.annotations.items()
        }

    def _delete_column_from_annotations(self, column_name):
        return {
            k: self._delete_from_visible_columns(v, column_name) if k == chaise_tags.visible_columns else v
            for k, v in self.table.annotations.items()
        }

    def _rename_columns_keys(self, columns, column_map, target_schema, target_table):
        """
        Rename incoming foreign keys to this table so that they still refer to the same columns after columns have
        been renamed according to column_map and move to a new schema and table.
        :param columns:
        :param column_map:
        :param target_schema:
        :param target_table:
        :return:
        """
        def update_key_name(name):
            # Helper function that creates a new constraint name by replacing table and column names.
            name = name[1].replace('{}_'.format(self.table_name), '{}_'.format(table_name))
            for k, v in column_map.items():
                    name = name.replace(k, v)
            return target_schema, name

        def column_match(key_columns):
            overlap = columns.intersection(key_columns)
            if len(overlap) == 0:
                return False
            if not column_rename and len(overlap) < len(key_columns):
                raise DerivaConfigError(msg='Cannot rename part of compound key')
            return True
        
        def fkey_def(fk, fk_columns, referenced_columns, names):
            return em.ForeignKey.define(
                [i['column_name'] for i in fk.foreign_key_columns],
                target_schema, target_table,
                [column_map.get(i, i) for i in referenced_columns],
                on_update=fk.on_update, on_delete=fk.on_delete,
                constraint_names=[update_key_name(n) for n in fk.names],
                comment=fk.comment,
                acls=fk.acls,
                acl_bindings=fk.acl_bindings,
                annotations=fk.annotations
            )

        column_rename = self.schema_name == target_schema and self.table_name == target_table
        columns = set(columns)
        
        for i in self.table.keys:
            if column_match(i.unique_columns):
                self.table.create_key(
                    em.Key.define(
                        [column_map.get(c, c) for c in i.unique_columns],
                        constraint_names=[update_key_name(n) for n in i.names],
                        comment=i.comment,
                        annotations=i.annotations
                    )
                )
                
        for fk in self.table.foreign_keys:
            fk_columns = [i['column_name'] for i in fk.foreign_key_columns]
            if column_match(fk_columns):    # We are renaming one of the foreign key columns
                fk_def = fk_def(fk,
                                [column_map.get(i, i) for i in fk_columns],
                                [i['column_name'] for i in fk.referenced_columns],
                                [update_key_name(n) for n in fk.names])

        for fk in self.table.referenced_by:
            referenced_columns = [i['column_name'] for i in fk.referenced_columns],
            if column_match(referenced_columns):    # We are renaming one of the referenced columns.
                fk_def = fk_def(fk,
                                [i['column_name'] for i in fk.foreign_key_columns],
                                [column_map.get(i,i) for i in referenced_columns],
                                fk.names)

                referring_table = self.model.schemas[fk.sname].tables[fk.tname]
                referring_table.create_fkey(self.catalog, fk_def)
                fk.delete(self.catalog, referring_table)

    def delete_column(self, column_name):
        """
        Drop a column from a table, cleaning up visible columns and keys.
        :param column_name:
        :return:
        """
        # Remove keys...
        for i in self.table.keys:
            if column_name in i.unique_columns and len(i.unique_columns) != 1:
                raise DerivaConfigError(msg='Cannot delete column that is part of composite key')
        for i in self.table.keys:
            if column_name in i.unique_columns:
                i.delete(self.catalog, self.table)

        # TODO Need to check acl_bindings and display annotations

        self.table.annotations = self._delete_column_from_annotations(column_name)
        self.table.column_definitions[column_name].delete(self.catalog, self.table)
        return

    def _rename_column(self, from_column, to_column,
                       nullok=None, default=None,
                       comment=None,
                       acls = {},
                       acl_bindings = {},
                       annotations = {}):
        """
        Copy a column, updating visible columns list and keys to mirror source column.
        :param from_column:
        :param to_column:
        :return:
        """
        
        # TODO we need to figure out what to do about ACL binding

        # Create a new column_spec from the existing spec.
        from_def = self.table.column_definitions[from_column]
        self.table.create_column(self.catalog,
                                 em.Column.define(
                                     to_column,
                                     from_def.type,
                                     nullok=from_def.nullok if nullok is None else nullok,
                                     default=from_def.default if default is None else default,
                                     comment= comment if comment else from_def.comment,
                                     acls=from_def.acls,
                                     acl_bindings=from_def.acl_bindings,
                                     annotations=from_def.annotations
                                 ))

        # Copy over the old values
        table_path = self.datapat()
        table_path.update(table_path.entities(table_path.RID, 
                                              **{to_column: table_path.column_definitions[from_column]}))
        
        # Copy over the keys.
        for i in self.table.keys:
            if from_column in i.unique_columns:
                self.table.create_key(
                    em.Key.define(
                        [to_column if c == from_column else c for c in i.unique_columns],
                        constraint_names=[
                            i.constraint_name.replace('_{}_'.format(from_column), '_{}_'.format(to_column))
                        ],
                        comment=i.comment,
                        annotations=i.annotations
                    )
                )

        # Copy over fkeys
        
        self._rename_columns_in_incoming_fkeys()
        # Update annotations where the old spec was being used
        self.table.annotations = self._rename_columns_in_annotations({from_column: to_column})
        return

    def rename_column(self, from_column, to_column, default=None, nullok=None):
        """
        Rename a column by copying it and then deleting the origional column.
        :param from_column:
        :param to_column:
        :return:
        """
        self._rename_column(from_column, to_column, default=default, nullok=nullok)
        self.delete_column(from_column)
        return

    def delete_table(self):
        pass

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
        def update_key_name(name):
            # Helper function that creates a new constraint name by replacing table and column names.
            name = name[1].replace('{}_'.format(self.table_name), '{}_'.format(table_name))
            for k, v in column_map.items():
                    name = name.replace(k, v)
            return schema_name, name

        def ktup(k):
            return tuple(k.unique_columns)

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
                for i in self.table.column_definitions if i.name not in {c['name']: c for c in column_defs}
            ] + column_defs,

            # Update the keys using the new column names.
            key_defs=[
                em.Key.define(
                    [column_map.get(c, c) for c in i.unique_columns],
                    constraint_names=[update_key_name(n) for n in i.names],
                    comment=i.comment,
                    annotations=i.annotations
                )
                for i in self.table.keys if ktup(i) not in {ktup(kdef): kdef for kdef in key_defs}
            ] + key_defs,

            fkey_defs=[
                em.ForeignKey.define(
                    [column_map.get(c['column_name'], c['column_name']) for c in i.foreign_key_columns],
                    i.referenced_columns[0]['schema_name'],
                    i.referenced_columns[0]['table_name'],
                    [c['column_name'] for c in i.referenced_columns],
                    constraint_names=[update_key_name(n) for n in i.names],
                    comment=i.comment,
                    on_update=i.on_update, on_delete=i.on_delete,
                    acls=i.acls, acl_bindings=i.acl_bindings,
                    annotations=i.annotations
                )
                for i in self.table.foreign_keys
            ] + fkey_defs,

            comment=comment if comment else self.table.comment,
            acls=self.table.acls,
            acl_bindings=self.table.acl_bindings,

            # Update visible columns to account for column_map
            annotations=self._rename_columns_in_annotations(column_map)
        )

        # Create new table
        new_table = self.model.schemas[schema_name].create_table(self.catalog, new_table_def)

        # Copy over values from original to the new one, mapping column names where required.
        pb = self.catalog.getPathBuilder()
        from_path = pb.schemas[self.schema_name].tables[self.table_name]
        to_path = pb.schemas[schema_name].tables[table_name]
        rows = from_path.entities(**{column_map.get(i, i): getattr(from_path, i) for i in from_path.column_definitions})
        to_path.insert(rows, **({'nodefaults': 'RID,RCT,RCB'} if clone else {}))

        return new_table

    def move_table(self, schema_name, table_name,
                   delete_table=False,
                   column_map={},
                   column_defs=[],
                   key_defs=[],
                   fkey_defs=[],
                   comment=None,
                   acls={},
                   acl_bindings={},
                   annotations={}
                   ):

        new_table = self.copy_table(schema_name, table_name, clone=True,
                                    column_map=column_map,
                                    column_defs=column_defs,
                                    key_defs=key_defs,
                                    fkey_defs=fkey_defs,
                                    comment=comment,
                                    acls=acls,
                                    acl_bindings=acl_bindings,
                                    annotations=annotations)

        self._rename_columns_in_keys([i.name for i in self.table.column_definitions], 
                                     column_map, schema_name, table_name)

        if delete_table:
            self.table.delete(self.catalog, schema=self.model.schemas[schema_name])
        self.table = new_table
        self.schema_name = schema_name
        self.table_name = table_name
        return

    
    def refresh(self):
        self.model = self.catalog.getCatalogModel()

    def display(self):
        for i in self.table.column_definitions:
            print('{}\t{}'.format(i.name, i.type.typename))

    def datapath(self):
        return self.catalog.getPathBuilder().schemas[self.schema_name].tables[self.table_name]


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
