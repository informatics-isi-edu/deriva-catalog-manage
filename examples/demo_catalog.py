import logging

from deriva.core import get_credential, DerivaServer
from deriva.utils.catalog.components.deriva_model import DerivaColumn
from deriva.utils.catalog.components.configure_catalog import DerivaCatalogConfigure

logging.basicConfig(
    level=logging.INFO,
)

# These need to be changed depending on the host and groups available.
host = 'dev.isrd.isi.edu'
catalog_name='test'


logger = logging.getLogger(__name__)


# Create a new catalog instance
def create_catalog(server):
     credentials = get_credential(server)
     catalog = DerivaServer('https', server, credentials=credentials).create_ermrest_catalog()
     catalog_id = catalog.catalog_id
     logger.info('Catalog_id is {}'.format(catalog_id))
     return catalog_id


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
                {'name': "Collections", 'url': "/chaise/recordset/#{{{$catalog.id}}}/Beta_Cell:Dataset"},
                {'name': "Study", 'url': "/chaise/recordset/##{{{$catalog.id}}}/Beta_Cell:Protocol"},
                {'name': "Experiment", 'url': "/chaise/recordset/#{{{$catalog.id}}}/Beta_Cell:Experiment"},
                {'name': "Replicate", 'url': "/chaise/recordset/#{{{$catalog.id}}}/Beta_Cell:Biosample"},
                {'name': "Specimens", 'url': "/chaise/recordset/#{{{$catalog.id}}}/Beta_Cell:Specimen"},
                {'name': "File", 'url': "/chaise/recordset/#{{{$catalog.id}}}/Beta_Cell:Cell_Line"},
                {'name': "Imaging", 'url': "/chaise/recordset/#{{{$catalog.id}}}/Beta_Cell:Cell_Line"},
                {'name': "Anatomy", 'url': "/chaise/recordset/#{{{$catalog.id}}}/Common:Collection"}
           ]
           },
          {'name': "About", 'url': "https:/chase/record/#{{{$catalog.id}}}/WWW:About"},
          {'name': "Help", 'url': "https:/chase/record/#{{{$catalog.id}}}/WWW:Help"}
     ]
}

logger.info('Creating schema')
schema = catalog.create_schema('DemoSchema')

# Create Basic Tables.

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

#file = schema.create_asset('File')
#file.configure_table_defaults()

#imaging = schema.create_asset('Imaging')
#imaging.configure_table_defaults()

#imaging.create_asset_table('Imaging', set_policy=False)
#file.create_asset_table('File', set_policy=False)

# Create links between tables.
experiment.link_tables(study)

# Create collections.
collection = schema.create_table('Collection',[DerivaColumn.define('Description', 'text'),
                                               DerivaColumn.define('Name', 'text')
                                               ])
collection.associate_tables(specimen)
collection.associate_tables(study)


anatomy = catalog.create_vocabulary('Anatomy')
specimen.associate_vocabulary(anatomy)

# Now add some content.....

logger.info('Catalog %s', catalog.server_uri)