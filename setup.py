#
# Copyright 2018 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

""" Installation script for the deriva-catalog-manage package.
"""

from setuptools import setup, find_packages
import re
import io

setup(
    name="deriva-catalog-manage",
    description="Deriva catalog management using deriva-py",
    url='https://github.com/informatics-isi-edu/deriva-catalog-manage',
    maintainer='USC Information Sciences Institute ISR Division',
    maintainer_email='isrd-support@isi.edu',
    version="0.1.0",
    packages=find_packages(),
    namespace_packages=["deriva", "deriva.utils"],
    package_data={},
    entry_points={
        'console_scripts': [
            'deriva-catalog-dump = deriva.utils.catalog.manage.dump_catalog:main',
            'deriva-csv = deriva.utils.catalog.manage.deriva_csv:main'
        ]
    },
    requires=[
        'os',
        'sys',
        're',
        'tempfile',
        'importlib.util',
        'itertools',
        'logging',
        'argparse',
        'datetime',
        'pprint',
        'requests',
        'autopep8',
        'tableschema',
        'goodtables',
        'deriva'],
    install_requires=[
        'pprint',
        'requests',
        'autopep8',
        'tableschema',
        'goodtables',
        'deriva>=0.6.7'
    ],
    license='Apache 2.0',
    classifiers=[
        'Intended Audience :: Science/Research',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        "Operating System :: POSIX",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ]
)

