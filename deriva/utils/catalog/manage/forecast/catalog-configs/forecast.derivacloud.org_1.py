import argparse
from deriva.core import ErmrestCatalog, AttrDict, get_credential
from deriva.utils.catalog.manage.update_catalog import CatalogUpdater, parse_args
from deriva.core import tag as chaise_tags
import deriva.core.ermrest_model as em

groups = {
}

annotations = {
    'tag:isrd.isi.edu,2019:catalog-config': {
        'name': 'forecast',
        'groups': {
            'admin': 'https://auth.globus.org/a060fd0b-3f13-11eb-85ca-0aecec13621f',
            'reader': 'https://auth.globus.org/a060fd0b-3f13-11eb-85ca-0aecec13621f',
            'writer': 'https://auth.globus.org/a060fd0b-3f13-11eb-85ca-0aecec13621f',
            'curator': 'https://auth.globus.org/a060fd0b-3f13-11eb-85ca-0aecec13621f'
        }
    },
}

acls = {
}


def main(catalog, mode, replace=False):
    updater = CatalogUpdater(catalog)
    updater.update_catalog(mode, annotations, acls, replace=replace)


if __name__ == "__main__":
    host = 'forecast.derivacloud.org'
    catalog_id = 1
    mode, replace, host, catalog_id = parse_args(host, catalog_id, is_catalog=True)
    catalog = ErmrestCatalog('https', host, catalog_id=catalog_id, credentials=get_credential(host))
    main(catalog, mode, replace)
