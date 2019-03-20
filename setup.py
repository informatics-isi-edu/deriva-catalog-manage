#
# Copyright 2018 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

""" Installation script for the deriva-catalog-manage package.
"""

from setuptools import setup, find_packages
import re
import io

__version__ = re.search(
    r'__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
    io.open('deriva/utils/catalog/version.py', encoding='utf_8_sig').read()
    ).group(1)

with io.open('README.md') as readme_file:
    readme = readme_file.read()

setup(
    name="deriva-catalog-manage",
    description="Deriva catalog management using deriva-py",
    long_description=readme,
    long_description_content_type='text/markdown',
    url='https://github.com/informatics-isi-edu/deriva-catalog-manage',
    maintainer='USC Information Sciences Institute ISR Division',
    maintainer_email='isrd-support@isi.edu',
    version=__version__,
    packages=find_packages(),
    package_data={},
    entry_points={
        'console_scripts': [
            'deriva-catalog-dump = deriva.utils.catalog.manage.dump_catalog:main',
            'deriva-csv = deriva.utils.catalog.manage.deriva_csv:main',
            'deriva-catalog-config = deriva.utils.catalog.manage.configure_catalog:main',
            'deriva-model-components = deriva.utils.catalog.components.model_elements:main'
        ]
    },
    requires=[
        'deriva',
        'requests',
        'tableschema',
        'goodtables',
        'graphviz',
        'tabulator',
        'urllib',
        'urlparse',
        'yapf'
    ],
    install_requires=[
        'requests',
        'yapf',
        'tableschema',
        'goodtables',
        'tabulator',
        'graphviz',
        'attrdict',
        'deriva>=0.8.1'
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
