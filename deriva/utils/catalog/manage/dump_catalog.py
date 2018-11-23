from __future__ import print_function

import argparse
import pprint
import os
import re
from yapf.yapflib.yapf_api import FormatCode
import sys
import pathlib
from attrdict import AttrDict
from deriva.core import ErmrestCatalog, get_credential
from deriva.core.ermrest_config import tag as chaise_tags

if sys.version_info > (3, 5):
    import importlib.util
elif sys.version_info > (3, 3):
    from importlib.machinery import SourceFileLoader
elif  sys.version_info > (2, 7):
    import imp

tag_map = AttrDict({
    'immutable':          'tag:isrd.isi.edu,2016:immutable',
    'display':            'tag:misd.isi.edu,2015:display',
    'visible_columns':    'tag:isrd.isi.edu,2016:visible-columns',
    'visible_foreign_keys': 'tag:isrd.isi.edu,2016:visible-foreign-keys',
    'foreign_key':        'tag:isrd.isi.edu,2016:foreign-key',
    'table_display':      'tag:isrd.isi.edu,2016:table-display',
    'table_alternatives': 'tag:isrd.isi.edu,2016:table-alternatives',
    'column_display':     'tag:isrd.isi.edu,2016:column-display',
    'asset':              'tag:isrd.isi.edu,2017:asset',
    'export':             'tag:isrd.isi.edu,2016:export',
    'generated':          'tag:isrd.isi.edu,2016:generated',
    'bulk_upload':        'tag:isrd.isi.edu,2017:bulk-upload'
})

groups = {}

yapf_style = {
    'based_on_style' : 'pep8',
    'allow_split_before_dict_value' : False,
    'split_before_first_argument': False,
    'disable_ending_comma_heuristic': True,
    'column_limit': 90
}


def print_variable(name, value, stream, variables=None):
    """
    Print out a variable assignment on one line if empty, otherwise pretty print.
    :param name:
    :param value:
    :param stream:
    :return:
    """

    s = '{} = {!r}'.format(name, value)
    if variables:
        for k, v in variables.items():
            varsub = r"(['\"])+{}\1".format(v)
            if k in tag_map:
                repl = 'tags.{}'.format(k)
            elif k in groups:
                repl = 'groups.{}'.format(k)
            s = re.sub(varsub, repl, s)
    print(FormatCode(s, style_config=yapf_style)[0], file=stream)


def print_tag_variables(annotations, tag_map, stream, variables=None):
    """
    For each convenient annotation name in tag_map, print out a variable declaration of the form annotation = v where
    v is the value of the annotation the dictionary.  If the tag is not in the set of annotations, do nothing.
    :param annotations:
    :param tag_map:
    :param stream:
    :return:
    """
    for t, v in tag_map.items():
        if v in annotations:
            print_variable(t, annotations[v], stream, variables)


def print_annotations(annotations, tag_map, stream, var_name='annotations', variables={}):
    """
    Print out the annotation definition in annotations, substituting the python variable for each of the tags specified
    in tag_map.
    :param annotations:
    :param tag_map:
    :param stream:
    :return:
    """
    var_map = {v: k for k, v in tag_map.items()}
    if annotations == {}:
        s = '{} = {{}}'.format(var_name)
    else:
        s = '{} = {{'.format(var_name)
        for t, v in annotations.items():
            if t in var_map:
                # Use variable value rather then inline annotation value.
                s += "'{}' : {},".format(t, var_map[t])
            else:
                s += "'{}' : {!r},".format(t, v)
        s += '}'
    print(FormatCode(s, style_config=yapf_style)[0], file=stream)


def print_schema(server, catalog_id, schema_name, stream, variables=None):
    credential = get_credential(server)
    catalog = ErmrestCatalog('https', server, catalog_id, credentials=credential)
    model_root = catalog.getCatalogModel()
    schema = model_root.schemas[schema_name]

    print("""import argparse
from attrdict import AttrDict
from deriva.core import ErmrestCatalog, get_credential, DerivaPathError
import deriva.core.ermrest_model as em
from deriva.utils.catalog.manage import update_catalog
""", file=stream)
    print('table_names = [', file=stream)
    for i in schema.tables:
        print("    '{}',".format(i), file=stream)
    print(']\n', file=stream)
    print_variable('groups', groups, stream)
    print_variable('tags', tag_map, stream)
    print_tag_variables(schema.annotations, tag_map, stream, variables=variables)
    print_annotations(schema.annotations, tag_map, stream, variables=variables)
    print_variable('acls', schema.acls, stream, variables=variables)
    print_variable('comment', schema.comment, stream, variables=variables)
    print('''schema_def = em.Schema.define(
        '{0}',
        comment=comment,
        acls=acls,
        annotations=annotations,
    )


def main():
    server = '{0}'
    catalog_id = {1}
    schema_name = '{2}'
    
    mode, replace, server, catalog_id = update_catalog.parse_args(server, catalog_id)
    update_catalog.update_schema(mode, replace, server, catalog_id, schema_name, schema_def, annotations, acls, comment)


if __name__ == "__main__":
    main()'''.format(server, catalog_id, schema_name), file=stream)


def print_catalog(server, catalog_id, dumpdir, variables=None):
    credential = get_credential(server)
    catalog = ErmrestCatalog('https', server, catalog_id, credentials=credential)
    model_root = catalog.getCatalogModel()

    with open('{}/{}_{}.py'.format(dumpdir, server, catalog_id), 'w') as f:
        print("""import argparse
from attrdict import AttrDict
from deriva.core import ErmrestCatalog, get_credential, DerivaPathError
from deriva.utils.catalog.manage import update_catalog
import deriva.core.ermrest_model as em
""", file=f)
        print_variable('groups', groups, f)
        print_variable('tags', tag_map, f)
        print_tag_variables(model_root.annotations, tag_map, f, variables=variables)
        print_annotations(model_root.annotations, tag_map, f, variables=variables)
        print_variable('acls', model_root.acls, f, variables=variables)
        print('''


def main():
    server = '{0}'
    catalog_id = {1}
    mode, replace, server, catalog_id = update_catalog.parse_args(server, catalog_id, is_catalog=True)
    update_catalog.update_catalog(mode, replace, server, catalog_id, annotations, acls)
    

if __name__ == "__main__":
    main()'''.format(server, catalog_id), file=f)

    for schema_name, schema in model_root.schemas.items():
        filename = '{}/{}.schema.py'.format(dumpdir, schema_name)
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        with open(filename, 'w') as f:
            print_schema(server, catalog_id, schema_name, f, variables=variables)
        f.close()

        for i in schema.tables:
            print('Dumping {}:{}'.format(schema_name, i))
            filename = '{}/{}/{}.py'.format(dumpdir, schema_name, i)
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, 'w') as f:
                print_table(server, catalog_id, schema_name, i, f, variables=variables)
            f.close()


def print_table_annotations(table, stream, variables=None):
    print_variable('groups', groups, stream)
    print_variable('tags', tag_map, stream)
    print_tag_variables(table.annotations, tag_map, stream, variables=variables)
    print_annotations(table.annotations, tag_map, stream, var_name='table_annotations', variables=variables)
    print_variable('table_comment', table.comment, stream, variables=variables)
    print_variable('table_acls', table.acls, stream, variables=variables)
    print_variable('table_acl_bindings', table.acl_bindings, stream, variables=variables)


def print_column_annotations(table, stream, variables=None):
    column_annotations = {}
    column_acls = {}
    column_acl_bindings = {}
    column_comment = {}

    for i in table.column_definitions:
        if not (i.annotations == '' or not i.comment):
            column_annotations[i.name] = i.annotations
        if not (i.comment == '' or not i.comment):
            column_comment[i.name] = i.comment
        if i.annotations != {}:
            column_annotations[i.name] = i.annotations
        if i.acls != {}:
            column_acls[i.name] = i.acls
        if i.acl_bindings != {}:
            column_acl_bindings[i.name] = i.acl_bindings
    print_variable('column_annotations', column_annotations, stream, variables=variables)
    print_variable('column_comment', column_comment, stream, variables=variables)
    print_variable('column_acls', column_acls, stream, variables=variables)
    print_variable('column_acl_bindings', column_acl_bindings, stream, variables=variables)
    return


def print_foreign_key_defs(table, stream, variables=None):
    s = 'fkey_defs = [\n'
    for fkey in table.foreign_keys:
        s += """    em.ForeignKey.define({},
            '{}', '{}', {},
            constraint_names={},\n""".format([c['column_name'] for c in fkey.foreign_key_columns],
                                             fkey.referenced_columns[0]['schema_name'],
                                             fkey.referenced_columns[0]['table_name'],
                                             [c['column_name'] for c in fkey.referenced_columns],
                                             fkey.names)

        for i in ['annotations', 'acls', 'acl_bindings', 'on_update', 'on_delete', 'comment']:
            a = getattr(fkey, i)
            if not (a == {} or a is None or a == 'NO ACTION' or a == ''):
                v = "'" + a + "'" if re.match('comment|on_update|on_delete', i) else a
                s += "        {}={},\n".format(i, v)
        s += '    ),\n'

    s += ']'
    print(FormatCode(s, style_config=yapf_style)[0], file=stream)


def print_key_defs(table, stream):
    s = 'key_defs = [\n'
    for key in table.keys:
        s += """    em.Key.define({},
                   constraint_names={},\n""".format(key.unique_columns, key.names)
        for i in ['annotations',  'comment']:
            a = getattr(key, i)
            if not (a == {} or a is None or a == ''):
                v = "'" + a + "'" if i == 'comment' else a
                s += "       {} = {},\n".format(i, v)
        s += '),\n'
    s += ']'
    print(FormatCode(s, style_config=yapf_style)[0], file=stream)
    return


def print_column_defs(table, stream):
    system_columns = ['RID', 'RCB', 'RMB', 'RCT', 'RMT']
    provide_system = False

    s = 'column_defs = ['
    for col in table.column_definitions:
        if col.name in system_columns:
            provide_system = True
        s += '''    em.Column.define('{}', em.builtin_types['{}'],'''.\
            format(col.name, col.type.typename + '[]' if 'is_array' is True else col.type.typename)
        if col.nullok is False:
            s += "nullok=False,"
        if col.default and col.name not in system_columns:
            s += "default={!r},".format(col.default)
        for i in ['annotations', 'acls', 'acl_bindings', 'comment']:
            colvar = getattr(col, i)
            if colvar:  # if we have a value for this field....
                s += "{}=column_{}['{}'],".format(i, i, col.name)
        s += '),\n'
    s += ']'
    print(FormatCode(s, style_config=yapf_style)[0], file=stream)
    return provide_system


def print_table_def(provide_system, stream):
    s = \
"""table_def = em.Table.define(table_name,
    column_defs=column_defs,
    key_defs=key_defs,
    fkey_defs=fkey_defs,
    annotations=table_annotations,
    acls=table_acls,
    acl_bindings=table_acl_bindings,
    comment=table_comment,
    provide_system = {}
)""".format(provide_system)
    print(FormatCode(s, style_config=yapf_style)[0], file=stream)


def print_table(server, catalog_id, schema_name, table_name, stream, variables=None):
    credential = get_credential(server)
    catalog = ErmrestCatalog('https', server, catalog_id, credentials=credential)
    model_root = catalog.getCatalogModel()
    schema = model_root.schemas[schema_name]
    table = schema.tables[table_name]

    print("""import argparse
from attrdict import AttrDict
from deriva.core import ErmrestCatalog, get_credential, DerivaPathError
import deriva.core.ermrest_model as em
from deriva.utils.catalog.manage import update_catalog

table_name = '{}'
schema_name = '{}'
""".format(table_name, schema_name), file=stream)
    print_variable('groups', groups, stream)
    print_variable('tags', tag_map, stream)
    print_column_annotations(table, stream, variables=variables)
    provide_system = print_column_defs(table, stream)
    print_table_annotations(table, stream, variables=variables)
    print_key_defs(table, stream)
    print_foreign_key_defs(table, stream)
    print_table_def(provide_system, stream)
    print('''
def main(skip_args=False, mode='annotations', replace=False, server={0!r}, catalog_id={1}):
    
    if not skip_args:
        mode, replace, server, catalog_id = update_catalog.parse_args(server, catalog_id, is_table=True)
    update_catalog.update_table(mode, replace, server, catalog_id, schema_name, table_name, 
                                table_def, column_defs, key_defs, fkey_defs,
                                table_annotations, table_acls, table_acl_bindings, table_comment,
                                column_annotations, column_acls, column_acl_bindings, column_comment)


if __name__ == "__main__":
    main()'''.format(server, catalog_id), file=stream)
    return


def main():
    global groups
    parser = argparse.ArgumentParser(description='Dump definition for catalog {}:{}')
    parser.add_argument('server', help='Catalog server name')
    parser.add_argument('--catalog', default=1, help='ID number of desired catalog')
    parser.add_argument('--dir', default="catalog-configs", help='output directory name)')
    parser.add_argument('--config', default=None, help='python script to set up configuration variables)')
    args = parser.parse_args()

    dumpdir = args.dir
    server = args.server
    catalog_id = args.catalog
    configfile = args.config

    try:
        os.makedirs(dumpdir, exist_ok=True)
    except OSError:
        print("Creation of the directory %s failed" % dumpdir)
        sys.exit(1)

    if configfile:
        modname = os.path.splitext(os.path.basename(configfile))[0]
        print('loading config {} {}'.format(configfile, modname))
        if sys.version_info > (3, 5):
            modspec = importlib.util.spec_from_file_location(modname, configfile)
            config = importlib.util.module_from_spec(modspec)
            modspec.loader.exec_module(config)
        elif sys.version_info > (3, 3):
            config = SourceFileLoader(modname, configfile)
        else:
            config = imp.load_source(modname, configfile)

        groups = AttrDict(config.groups)

    print_catalog(server, catalog_id, dumpdir, variables={**groups, **tag_map})


if __name__ == "__main__":
    main()
