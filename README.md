# deriva-catalog-manage
Deriva catalog management using deriva-py. 

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

To load load a specific module, you can use a import statement such as:
```
from deriva.utils.catalog.manage import deriva_csv
```

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
    * Configure table so that system columns have meaningfull names and foreign keys are created to ermrest_client so that user names are used for creation and modification names.
    * Apply a "self-service" policy which allows creators of a table to edit it.  In addition, an additional 'Owner' column is added to the table to allow the creator to delegate the table to a different user to update.

## CLIs

The CLIs include:
- `deriva-dump-catalog`: a command-line tools that will dump the current configuration of a catalog as a set of deriva-py scripts. The scripts are pure deriva-py and have placeholder variables to set annotations, acls, and acl-bindings.  The scripts are self contained and can be run directly from the command line using the python interpreter. Run without optioins the program will dump config files for an entire catalog.  Command line arguments can be used to specify a single table be dumped.

- deriva-csv: upload a csv or other table like data with options to create a table in the catalog, to validate data and to upload to the catalog.  This command supports "chunked" upload for large files. If the table has columns that are keys, these can be specified in the command line.  In the absensee of a key in the CSV file, the script will use a system generated upload ID along with the row number of the CSV to ensure that the CSV uploads can be restarted without duplicate rows being entered.


## Config files.

All of the scripts in this package can recieved additional configuration information by providing a python config file which is specified by the --configfile command line argument.  Currently, the libary looks in the config file a variable named groups, which is a dictionary that maps between a conveient name and a Globus group URI:  e.g.:

```groups = {
    "admin": "https://auth.globus.org/80df6c56-a0e8-11e8-b9dc-0ada61684422",
    "modeler": "https://auth.globus.org/a45e5ba2-709f-11e8-a40d-0e847f194132",
    "curator ": "https://auth.globus.org/da80b96c-edab-11e8-80e2-0a7c1eab007a",
    "writer": "https://auth.globus.org/6a96ec62-7032-11e8-9132-0a043b872764",
    "reader": "https://auth.globus.org/aa5a2f6e-53e8-11e8-b60b-0a7c735d220a",
    "isrd": 'https://auth.globus.org/3938e0d0-ed35-11e5-8641-22000ab4b42b'
}```

deriva-dump-catalog will include the variables defined in a config file into the generated files, and will attept to factor out use of the variable from any expressions that are generated by the program.




 
