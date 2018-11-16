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
deriva.utils.catalog.manage.  

Current modules include:
- dump_catalog. A module for querying an ERMRest catalog and creation a set of deriva-py scripts to recreate elements of that catalog
- deriva_csv. A module for using tables and [tableschema](https://frictionlessdata.io/specs/table-schema/) to load and mangage ERMRest catalogs. 

To load load a specific module, you can use a import statement such as:
```
from deriva.utils.catalog.manage import update_catalog
```


## APIs

### deriva_csv

The module relys on tools for table manipulation that have been developed as part of the [Frictionless Data](https://frictionlessdata.io) initiative.  Documentation and related tools are available from their GitHub [repo](https://github.com/frictionlessdata).

Main entry points of this module are:
- Create tableschema from a table in a deriva catalog.
- Create a deriva-py program to create a table from a CSV, Google Sheet, database table, or other table format.
- Validate a CSV against an ERMRest table and upload it.

## dump_catalog



## CLIs

The CLIs include:
- `deriva-dump-catalog`: a command-line tools that will dump the current configuration of a catalog as a set of deriva-py scripts.
