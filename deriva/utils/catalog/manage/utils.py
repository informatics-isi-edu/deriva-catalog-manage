import deriva.core.ermrest_model as em
from deriva.core.deriva_server import DerivaServer


class LoopbackCatalog:

    class LoopbackResult:
        """
        Class to simulate interactions with a catalog server.
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


class TempErmrestCatalog:
    def __init__(self, scheme, server, credentials=None, caching=True, session_config=None):
        self.server = server
        self._derivaserver = DerivaServer(scheme, server, credentials, caching, session_config)

    def __enter__(self):
        self._catalog = self._derivaserver.create_ermrest_catalog()
        self.catalog_id = self._catalog.get_server_uri().split('/')[-1]
        return self._catalog

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            self._catalog.delete_ermrest_catalog(really=True)
        except Exception:
            raise
