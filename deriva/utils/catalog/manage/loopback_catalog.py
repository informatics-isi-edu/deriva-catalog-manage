class LoopbackCatalog:
    class LoopbackResult:
        """
        Class to simulate interactions with a catalog server.
        """

        def __init__(self, uri, json=None):
            if uri == '/schema':
                self._val = {0: {}}
            elif 'table' in uri:
                self._val = {0: {}}
            else:
                self._val = json

        def raise_for_status(self):
            pass

        def json(self):
            return self._val

    def post(self, uri, json=None):
        return LoopbackCatalog.LoopbackResult(uri, json=json)