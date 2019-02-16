import random
import datetime
import string

from deriva.core.ermrest_catalog import ErmrestCatalog
import deriva.core.ermrest_model as em
from deriva.core.deriva_server import DerivaServer


class LoopbackCatalog:
    class LoopbackResult:
        """
        Class to simulate interactions with a catalog host.
        """

        def __init__(self, uri, json=None):
            self._val = json

        def raise_for_status(self):
            pass

        def json(self):
            return self._val

    def __init__(self, model=None):
        self._server = 'host.local'
        self._catalog_id = 1

        self._model = model
        if self._model is None:
            self._model = em.Model({})

    def get_server_uri(self):
        return 'http://{}/ermrest/{}'.format(self._server, self._catalog_id)

    def getCatalogModel(self):
        return self._model

    def post(self, uri, json=None):
        return LoopbackCatalog.LoopbackResult(uri, json=json)

    def get(self, uri):
        if uri == '/schema':
            return LoopbackCatalog.LoopbackResult(uri, json=self._model.schemas)

    def put(self, uri, json=None, data=None):
        pass

    def delete(self, uri):
        pass


class TempErmrestCatalog(ErmrestCatalog):
    """
    Create a new catalog.  Can be used as as context so that catalog is automatically deleted.
    """
    def __init__(self, scheme, server, **kwargs):
        catalog_id = create_new_catalog(scheme, server, **kwargs)
        super(TempErmrestCatalog, self).__init__(scheme, server, catalog_id, **kwargs)
        return

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.delete_ermrest_catalog(really=True)
        return


def create_new_catalog(scheme, server, **kwargs):
    """
    FUnction to create a new catalog in the specified host
    :param scheme: URL scheme to be used
    :param server: Server on which the catalog is to be created
    :param kwargs:  Other DerivaServer arguments
    :return:
    """
    derivaserver = DerivaServer(scheme, server, **kwargs)
    catalog = derivaserver.create_ermrest_catalog()
    catalog_id = catalog.get_server_uri().split('/')[-1]
    return catalog_id

