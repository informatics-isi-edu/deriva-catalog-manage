import random
import datetime
import string
from contextlib import contextmanager

from deriva.core.ermrest_catalog import ErmrestCatalog
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


@contextmanager
def temp_ermrest_catalog(scheme, server, **kwargs):
    catalog = TempErmrestCatalog(scheme, server, **kwargs)
    yield catalog
    catalog.delete_ermrest_catalog(really=True)


class TempErmrestCatalog(ErmrestCatalog):
    def __init__(self, scheme, server, **kwargs):
        catalog_id =create_new_catalog(scheme, server, **kwargs)
        super(TempErmrestCatalog, self).__init__(scheme, server, catalog_id, **kwargs)
        return

def create_new_catalog(scheme, server, **kwargs):
    derivaserver = DerivaServer(scheme, server, **kwargs)
    catalog = derivaserver.create_ermrest_catalog()
    catalog_id = catalog.get_server_uri().split('/')[-1]
    return catalog_id

table_schema_ermrest_type_map = {
    'string:default': 'text',
    'string:email': 'text',
    'string:uri': 'ermrest_uri',
    'string:binary': 'text',
    'string:uuid': 'text',
    'number:default': 'float8',
    'integer:default': 'int4',
    'boolean:default': 'boolean',
    'object:default': 'json',
    'array:default': 'json[]',
    'date:default': 'date',
    'date:any': 'date',
    'time:default': 'timestampz',
    'time:any': 'timestampz',
    'datetime:default': 'date',
    'datetime:any': 'date',
    'year:default': 'date',
    'yearmonth:default': 'date'
}


def generate_test_csv(columncnt):
    type_list = ['int4', 'boolean', 'float8', 'date', 'text']
    column_types = [type_list[i % len(type_list)] for i in range(columncnt)]
    column_headings = ['id'] + ['field {}'.format(i) for i in range(len(column_types))]

    missing_value = .2

    base = datetime.datetime.today()
    date_list = [base - datetime.timedelta(days=x) for x in range(0, 100)]

    def col_value(c):
        v = ''

        if random.random() > missing_value:
            if c == 'boolean':
                v = random.choice(['true', 'false'])
            elif c == 'int4':
                v = random.randrange(-1000, 1000)
            elif c == 'float8':
                v = random.uniform(-1000, 1000)
            elif c == 'text':
                v = ''.join(random.sample(string.ascii_letters + string.digits, 5))
            elif c == 'date':
                v = str(random.choice(date_list))
        return v

    def row_generator(header=True):
        row_count = 1
        while True:
            if header is True:
                row = column_headings
                header = False
            else:
                row = [row_count]
                row_count += 1
                row.extend([col_value(i) for i in column_types])
            yield row

    return row_generator(), [{'name': i[0], 'type': i[1]} for i in zip(column_headings, column_types)]
