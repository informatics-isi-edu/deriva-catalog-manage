import argparse
from attrdict import AttrDict
from deriva.core import ErmrestCatalog, get_credential, DerivaPathError
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

table_name = 'Foo'

schema_name = 'TestSchema'

column_annotations = {}

column_comment = {}

column_acls = {}

column_acl_bindings = {}

column_defs = [
    em.Column.define('Id', em.builtin_types['int4'], nullok=False,
                     ),
    em.Column.define('Field_0', em.builtin_types['int4'],
                     ),
    em.Column.define('Field_1', em.builtin_types['int4'],
                     ),
    em.Column.define('Field_2', em.builtin_types['boolean'],
                     ),
    em.Column.define('Field_3', em.builtin_types['float8'],
                     ),
    em.Column.define('Field_4', em.builtin_types['date'],
                     ),
    em.Column.define('Field_5', em.builtin_types['text'],
                     ),
]

table_annotations = {}
table_comment = None
table_acls = {}
table_acl_bindings = {}

key_defs = [
    em.Key.define(['RID'], constraint_names=[],
                  ),
    em.Key.define(['Id'], constraint_names=[('TestSchema', 'Foo_Id_key)')],
                  ),
]

fkey_defs = []

table_def = em.Table.define(
    table_name,
    column_defs=column_defs,
    key_defs=key_defs,
    fkey_defs=fkey_defs,
    annotations=table_annotations,
    acls=table_acls,
    acl_bindings=table_acl_bindings,
    comment=table_comment,
    provide_system=True
)


def main(catalog, mode, replace=False):
    updater = CatalogUpdater(catalog)
    updater.update_table(mode, schema_name, table_def, replace=replace)


if __name__ == "__main__":
    server = 'host.local'
    catalog_id = 1
    mode, replace, server, catalog_id = parse_args(server, catalog_id, is_table=True)
    credential = get_credential(server)
    catalog = ErmrestCatalog('https', server, catalog_id, credentials=credential)
    main(catalog, mode, replace)

