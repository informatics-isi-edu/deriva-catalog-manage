table_file_template="""
import argparse
from attrdict import AttrDict
from deriva.core import ErmrestCatalog, get_credential, DerivaPathError
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage import update_catalog

table_name = '{table_name}'

schema_name = '{schema_name}'

{groups}

{column_annotations}

{column_defs}

{table_annotations}

{key_defs}

{fkey_defs}

{table_def}


def main(skip_args=False, mode='annotations', replace=False, server={server!r}, catalog_id={catalog_id}):
    
    if not skip_args:
        mode, replace, server, catalog_id = update_catalog.parse_args(server, catalog_id, is_table=True)
        
    updater = CatalogUpdater(self, mode, replace, server, catalog_id)
    
    updater.update_table(schema_name, table_name, 
                         table_def, column_defs, key_defs, fkey_defs,
                         table_annotations, table_acls, table_acl_bindings, table_comment,
                         column_annotations, column_acls, column_acl_bindings, column_comment)


if __name__ == "__main__":
    main()
"""


schema_file_template = """
import argparse
from attrdict import AttrDict
from deriva.core import ErmrestCatalog, get_credential, DerivaPathError
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage import update_catalog

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

def main():
    server = {server!r}
    catalog_id = {catalog_id}
    schema_name = '{schema_name}'

    mode, replace, server, catalog_id = update_catalog.parse_args(server, catalog_id)
    updater = CatalogUpdater(self, mode, replace, server, catalog_id)
    updater.update_catalog.update_schema(schema_name, schema_def, annotations, acls, comment)


if __name__ == "__main__":
    main()
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


def main():
    server = {0!r}
    catalog_id = {1}
    mode, replace, server, catalog_id = update_catalog.parse_args(server, catalog_id, is_catalog=True)
    updater = CatalogUpdater(self, mode, replace, server, catalog_id)
    updater.update_catalog.update_catalog(annotations, acls)


if __name__ == "__main__":
    main()
"""