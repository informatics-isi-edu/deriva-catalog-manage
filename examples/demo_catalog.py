import logging

from deriva.core import get_credential, DerivaServer
from deriva.utils.catalog.components.deriva_model import DerivaColumn, DerivaModel
from deriva.utils.catalog.components.configure_catalog import DerivaCatalogConfigure

logging.basicConfig(
    level=logging.INFO,
)

# These need to be changed depending on the host and groups available.
host = 'dev.isrd.isi.edu'
catalog_name='test'
schema_name='Demo'


logger = logging.getLogger(__name__)


# Create a new catalog instance
def create_catalog(server):
     credentials = get_credential(server)
     catalog = DerivaServer('https', server, credentials=credentials).create_ermrest_catalog()
     catalog_id = catalog.catalog_id
     logger.info('Catalog_id is {}'.format(catalog_id))
     return catalog_id

def menu_url(table_name):
     return "/chaise/recordset/#{{{$catalog.id}}}/" + "{}:{}".format(schema_name, table_name)

logger.info('Creating catalog....')
catalog_id = create_catalog(host)

logger.info('Configuring catalog....')
catalog = DerivaCatalogConfigure(host, catalog_id=catalog_id)
catalog.configure_baseline_catalog(catalog_name='test', admin='isrd-systems')

catalog.navbar_menu = {
     'newTab': False,
     'children': [
          {'name': "Browse",
           'children': [
                {'name': "Collections", 'url': menu_url('Collection')},
                {'name': "Study", 'url': menu_url('Study')},
                {'name': "Experiment", 'url': menu_url("Experiment")},
                {'name': "Replicate", 'url': menu_url('Replicate')},
                {'name': "Specimens", 'url': menu_url('Specimen')},
                {'name': "File", 'url': menu_url("File")},
                {'name': "Imaging", 'url': menu_url("Imaging")},
                {'name': "Anatomy", 'url': menu_url("Anatomy")}
           ]
           },
          {'name': "About", 'url': "https:/chase/record/#{{{$catalog.id}}}/WWW:About"},
          {'name': "Help", 'url': "https:/chase/record/#{{{$catalog.id}}}/WWW:Help"}
     ]
}

logger.info('Creating schema')
schema = catalog.create_schema('DemoSchema')

# Create Basic Tables.
with DerivaModel(catalog):
     logger.info('Creating tables....')
     study = schema.create_table('Study',
                                 [DerivaColumn.define('Title', 'text'),
                                  DerivaColumn.define('Description', 'text')])
     study.configure_table_defaults()

     experiment = schema.create_table('Experiment', [DerivaColumn.define('Experiment_Type', 'text')])
     experiment.configure_table_defaults()

     replicate = schema.create_table('Replicate', [DerivaColumn.define('Replicate_Number', 'int4')])
     replicate.configure_table_defaults()

     specimen = schema.create_table('Specimen', [DerivaColumn.define('Specimen_Type', 'text')])
     specimen.configure_table_defaults()

     # Asset tables
     logger.info('Creating asset tables....')
     file = schema.create_asset('File',
                                column_defs=[
                                     DerivaColumn.define('File_Type', 'text'),
                                     DerivaColumn.define('Description', 'text')
                                ])
     file.configure_table_defaults()

     imaging = schema.create_asset('Imaging')
     imaging.configure_table_defaults()

     # Create collections.
     collection = schema.create_table('Collection',[DerivaColumn.define('Description', 'text'),
                                                    DerivaColumn.define('Name', 'text')
                                                    ])

     # Create links between tables.
     logger.info('Linking tables....')
     experiment.link_tables(study)
     replicate.link_tables(file)

     collection.associate_tables(specimen)
     collection.associate_tables(study)

     specimen.associate_tables(imaging)
     experiment.associate_tables(imaging)

     anatomy = schema.create_vocabulary('Anatomy', 'DEMO:{RID}')
     specimen.associate_vocabulary(anatomy)

# Now add some content.....


logger.info('Catalog %s', study.chaise_uri)