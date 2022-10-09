import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential
import deriva.core.ermrest_model as em
from deriva.core import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

groups = {
    'core-writer': 'https://auth.globus.org/72bdb36c-9503-11e8-8c03-0e847f194132',
    'DERIVA Forecast Demo Creator': 'https://auth.globus.org/a060fd0b-3f13-11eb-85ca-0aecec13621f'
}

table_name = 'ERMrest_Client'

schema_name = 'public'

column_annotations = {}

column_comment = {}

column_acls = {}

column_acl_bindings = {}

column_defs = [
    em.Column.define('ID', em.builtin_types['text'], nullok=False,
                     ),
    em.Column.define('Display_Name', em.builtin_types['text'],
                     ),
    em.Column.define('Full_Name', em.builtin_types['text'],
                     ),
    em.Column.define('Email', em.builtin_types['text'],
                     ),
    em.Column.define('Client_Object', em.builtin_types['jsonb'], nullok=False,
                     ),
]

table_annotations = {}

table_comment = None

table_acls = {'delete': [], 'insert': [], 'select': [], 'update': [], 'enumerate': []}

table_acl_bindings = {}

key_defs = [
    em.Key.define(['RID'], constraint_names=[['public', 'ERMrest_Client_pkey']],
                  ),
    em.Key.define(['ID'], constraint_names=[['public', 'ERMrest_Client_ID_key']],
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


def main(catalog, mode, replace=False, really=False):
    updater = CatalogUpdater(catalog)
    table_def['column_annotations'] = column_annotations
    table_def['column_comment'] = column_comment
    updater.update_table(mode, schema_name, table_def, replace=replace, really=really)


if __name__ == "__main__":
    host = 'forecast.derivacloud.org'
    catalog_id = 1
    mode, replace, host, catalog_id = parse_args(host, catalog_id, is_table=True)
    catalog = ErmrestCatalog('https', host, catalog_id=catalog_id, credentials=get_credential(host))
    main(catalog, mode, replace)
