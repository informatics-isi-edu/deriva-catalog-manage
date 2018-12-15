table_file_template="""
import argparse
from attrdict import AttrDict
from deriva.core import ErmrestCatalog, get_credential, DerivaPathError
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

table_name = '{table_name}'

schema_name = '{schema_name}'

{variables}

{column_annotations}

{column_defs}

{table_annotations}

{key_defs}

{fkey_defs}

{table_def}


def main(catalog, mode, replace=False):
    updater = CatalogUpdater(catalog)
    updater.update_table(mode, schema_name, table_def,replace=replace)


if __name__ == "__main__":
    server = {server!r}
    catalog_id = {catalog_id}
    mode, replace, server, catalog_id = parse_args(server, catalog_id, is_table=True)
    credential = get_credential(server)
    catalog = ErmrestCatalog('https', server, catalog_id, credentials=credential)
    main(catalog, mode, replace)
"""


schema_file_template = """
import argparse
from attrdict import AttrDict
from deriva.core import ErmrestCatalog, get_credential, DerivaPathError
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args

schema_name = '{schema_name}'

{table_names}

{variables}

{annotations}

{acls}

{comments}

schema_def = em.Schema.define(
        '{schema_name}',
        comment=comment,
        acls=acls,
        annotations=annotations,
    )

def main(catalog, mode, replace=False):
    updater = CatalogUpdater(catalog)
    updater.update_catalog.update_schema(mode, schema_name, schema_def, replace=replace)


if __name__ == "__main__":
    server = {server!r}
    catalog_id = {catalog_id}
    mode, replace, server, catalog_id = parse_args(server, catalog_id, is_catalog=True)
    credential = get_credential(server)
    catalog = ErmrestCatalog('https', server, catalog_id, credentials=credential)
    main(catalog, mode, replace)
"""

catalog_file_template = """
import argparse
from attrdict import AttrDict
from deriva.core import ErmrestCatalog, get_credential, DerivaPathError
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args
from deriva.core.ermrest_config import tag as chaise_tags
import deriva.core.ermrest_model as em


{variables}


{tag_variables}


{annotations}


{acls}

def main(catalog, mode, replace=False):
    updater = CatalogUpdater(catalog)
    updater.update_catalog(mode, annotations, acls, replace=replace)


if __name__ == "__main__":
    server = {server!r}
    catalog_id = {catalog_id}
    mode, replace, server, catalog_id = parse_args(server, catalog_id, is_catalog=True)
    credential = get_credential(server)
    catalog = ErmrestCatalog('https', server, catalog_id, credentials=credential)
    main(catalog, mode, replace)
"""