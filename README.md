# deriva-catalog-manage
[![PyPi Version](https://img.shields.io/pypi/v/deriva-catalog-manage.svg)](https://pypi.python.org/pypi/deriva-catalog-manage)
[![PyPi Wheel](https://img.shields.io/pypi/wheel/deriva-catalog-manage.svg)](https://pypi.python.org/pypi/deriva-catalog-manage)
[![Python Versions](https://img.shields.io/pypi/pyversions/deriva-catalog-manage.svg)](https://pypi.python.org/pypi/deriva-catalog-manage)
[![License](https://img.shields.io/pypi/l/deriva-catalog-manage.svg)](http://www.apache.org/licenses/LICENSE-2.0)

Deriva catalog management using deriva-py. Deriva catalog management streamlines common tasks associated with creating
and managing catalog.  For example, given an empty catalog and a CSV file that is an example of a table, a functioning
catalog can be created by:

1) Run catalog-configure to set up basic display configuration and a simple access control policy.
2) Run deriva-csv to create a table in the catalog that conforms to the CSV.
3) Run catalog-configure to create an asset table and configure upload annotations to associate files with rows in the
CSV file
4) Run dump-catalog to create a set of deriva-py programs that can be modified to further configure the catalog.

## Installing

This project is mostly in an early development phase. The `master` branch is expect to be stable and usable at every
commit. The APIs and CLIs may change in backward-incompatible ways, so if you depend on an interface you should remember
the GIT commit number.

At this time, we recommend installing from source, which can be accomplished with the `pip` utility.

If you have root access and wish to install into your system Python directory, use the following command:
```
$ sudo pip install --upgrade git+https://github.com/informatics-isi-edu/deriva-catalog-manage.git
```
Otherwise, it is recommended that you install into your user directory using the following command:
```
$ pip install --user --upgrade git+https://github.com/informatics-isi-edu/deriva-catalog-manage.git
```

## Packages

This repo will install a number of different subpackages that will all live in the deriva package hierachy under: 
`deriva.utils.catalog.manage`.  

Current modules include:
- dump_catalog. A module for querying an ERMRest catalog and creation a set of deriva-py scripts to recreate elements of that catalog
- deriva_csv. A module for using tables and [tableschema](https://frictionlessdata.io/specs/table-schema/) to load and mangage ERMRest catalogs. 
- configure_catalog. Set up catalog configuration, policy and table configurations.
To load load a specific module, you can use a import statement such as:
```
from deriva.utils.catalog.manage import deriva_csv
```



## CLIs

The CLIs include:
- `deriva-dump-catalog`: a command-line tools that will dump the current configuration of a catalog as a set of deriva-py scripts. The scripts are pure deriva-py and have placeholder variables to set annotations, acls, and acl-bindings.  The scripts are self contained and can be run directly from the command line using the python interpreter. Run without optioins the program will dump config files for an entire catalog.  Command line arguments can be used to specify a single table be dumped.  The dump catalog CLI also has an option to create an ER diagram of the catalog model.  This can be output in pdf, DOT or other image formats.

- deriva-csv: upload a csv or other table like data with options to create a table in the catalog, to validate data and to upload to the catalog.  This command supports "chunked" upload for large files. If the table has columns that are keys, these can be specified in the command line.  In the absensee of a key in the CSV file, the script will use a system generated upload ID along with the row number of the CSV to ensure that the CSV uploads can be restarted without duplicate rows being entered.

- deriva-catalog-configure: Set up catalog and tables in a catalog to have a standard baseline configuration.  Properties of the standard catalog confuration are:
   - Table and column names are displayed with underscores converted into spaces
   - Access control is configured for self-service. This means that there are four basic groups admin, curator, reader and writer and they are set up with appropriate rights. In addition, the creator of an entity may edit that entity, or assigne rights to a group to edit the entity.
   - Tables and catalog are set up to display user names rather then user IDs.
   
## APIs

### deriva_csv

The module relies on tools for table manipulation that have been developed as part of the [Frictionless Data](https://frictionlessdata.io) initiative.  Documentation and related tools are available from their GitHub [repo](https://github.com/frictionlessdata).

Main entry points of this module are:

- table_schema_from_catalog: Create tableschema from a table in a deriva catalog.
- convert_table_to_deriva: Create a deriva-py program to create a table from a CSV, Google Sheet, database table, or other table format.
- upload_table: Validate a CSV against an ERMRest table and upload it. This API has an option to create a table in the catalog before uploading. By default, all data is validated against the current table schema in the catalog prior to uploading.

- configure_table: This module provides a set of functions that can be used to create a baseline configuration for catalogs and tables.  The baseline configuration is:
    * Convert underscores in table and column names to spaces when displayed
    * Configure the ermrest_client table so that 
    * Configure table so that system columns have meaningful names and foreign keys are created to ermrest_client so that user names are used for creation and modification names.
    * Apply a "self-service" policy which allows creators of a table to edit it.  In addition, an additional 'Owner' column is added to the table to allow the creator to delegate the table to a different user to update.
    




 
