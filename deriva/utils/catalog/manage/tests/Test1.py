import argparse
from attrdict import AttrDict
from deriva.core import ErmrestCatalog, get_credential, DerivaPathError
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater

table_name = 'Test1'

schema_name = 'TestSchema'

groups = {}

column_annotations = {}

column_comment = {}

column_acls = {}

column_acl_bindings = {}

column_defs = [
    em.Column.define('RID', em.builtin_types['ermrest_rid'], nullok=False,
                     ),
    em.Column.define('RCT', em.builtin_types['ermrest_rct'], nullok=False,
                     ),
    em.Column.define('RMT', em.builtin_types['ermrest_rmt'], nullok=False,
                     ),
    em.Column.define('RCB', em.builtin_types['ermrest_rcb'],
                     ),
    em.Column.define('RMB', em.builtin_types['ermrest_rmb'],
                     ),
    em.Column.define('A', em.builtin_types['text'],
                     ),
    em.Column.define('B', em.builtin_types['float8'],
                     ),
    em.Column.define('C', em.builtin_types['boolean'],
                     ),
]

table_annotations = {}
table_comment = None
table_acls = {}
table_acl_bindings = {}

key_defs = [
    em.Key.define(['RID'], constraint_names=[],
                  ),
    em.Key.define(['RID'], constraint_names=[('TestSchema', 'Test1_RID_Key')],
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


def main(mode, replace=False, catalog=None):
    server = 'host.local'
    catalog_id = (1, )

    if catalog is None:
        mode, replace, server, catalog_id = update_catalog.parse_args(
            server, catalog_id, is_table=True
        )

    updater = CatalogUpdater(server, catalog_id, catalog=catalog)
    updater.update_table(mode, schema_name, table_def, replace=replace)


if __name__ == "__main__":
    mode, replace, server, catalog_id = update_catalog.parse_args(
        server, catalog_id, is_catalog=True
    )
    main(mode, replace)

