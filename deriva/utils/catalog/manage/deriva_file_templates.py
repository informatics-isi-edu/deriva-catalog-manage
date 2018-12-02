table_file_template="""
import argparse
from attrdict import AttrDict
from deriva.core import ErmrestCatalog, get_credential, DerivaPathError
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater

table_name = '{table_name}'

schema_name = '{schema_name}'

{groups}

{column_annotations}

{column_defs}

{table_annotations}

{key_defs}

{fkey_defs}

{table_def}


def main(mode, replace=False, catalog=None):
    server = {server!r}
    catalog_id = {catalog_id}
    
    if catalog is None:
        mode, replace, server, catalog_id = update_catalog.parse_args(server, catalog_id, is_table=True)
        
    updater = CatalogUpdater(server, catalog_id, catalog=catalog)
    updater.update_table(mode, schema_name, table_def,replace=replace)


if __name__ == "__main__":
    mode, replace, server, catalog_id = update_catalog.parse_args(server, catalog_id, is_catalog=True)
    main(mode, replace)
"""


schema_file_template = """
import argparse
from attrdict import AttrDict
from deriva.core import ErmrestCatalog, get_credential, DerivaPathError
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater

schema_name = '{schema_name}'

{table_names}

{groups}

{annotations}

{acls}

{comments}

schema_def = em.Schema.define(
        '{schema_name}',
        comment=comment,
        acls=acls,
        annotations=annotations,
    )

def main(mode, replace=False, catalog=None):
    server = {server!r}
    catalog_id = {catalog_id}

    if catalog is None:
        mode, replace, server, catalog_id = update_catalog.parse_args(server, catalog_id)
        
    updater = CatalogUpdater(server, catalog_id, catalog=catalog)
    updater.update_catalog.update_schema(mode, schema_name, schema_def, replace=replace)


if __name__ == "__main__":
    mode, replace, server, catalog_id = update_catalog.parse_args(server, catalog_id, is_catalog=True)
    main(mode, replace)
"""

catalog_file_template = """
import argparse
from attrdict import AttrDict
from deriva.core import ErmrestCatalog, get_credential, DerivaPathError
from deriva.utils.catalog.manage import update_catalog
from deriva.core.ermrest_config import tag as chaise_tags
import deriva.core.ermrest_model as em


{catalog_groups}


{tag_variables}


{annotations}


{catalog_acls}

def main(mode, replace=False, catalog=None):
    server = {0!r}
    catalog_id = {1}
    
    updater = CatalogUpdater(server, catalog_id, catalog=catalog)
    updater.update_catalog.update_catalog(mode, annotations, acls, replace=replace)


if __name__ == "__main__":
    mode, replace, server, catalog_id = update_catalog.parse_args(server, catalog_id, is_catalog=True)
    main(mode, replace)
"""