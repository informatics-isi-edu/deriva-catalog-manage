from __future__ import print_function

import argparse
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
elif sys.version_info > (2, 7):
    import imp

groups = {}

yapf_style = {
    'based_on_style': 'pep8',
    'allow_split_before_dict_value': False,
    'split_before_first_argument': False,
    'disable_ending_comma_heuristic': True,
    'DEDENT_CLOSING_BRACKETS': True,
    'column_limit': 90
}

def load_deriva_manage_config(configfile):
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
    return groups

def substitute_variables(code, variables):
    """
    Factor out code and replace with a variable name.
    :param code:
    :param variables:
    :return:
    """
    if variables:
        for k, v in variables.items():
            varsub = r"(['\"])+{}\1".format(v)
            if k in chaise_tags:
                repl = 'chaise_tags.{}'.format(k)
            elif k in groups:
                repl = 'groups.{}'.format(k)
            else:
                repl = k
            code = re.sub(varsub, repl, code)
    return code


def variable_to_str(name, value, variables=None):
    """
    Print out a variable assignment on one line if empty, otherwise pretty print.
    :param name: Left hand side of assigment
    :param value: Right hand side of assignment
    :param variables: Dirctionary of variable names to be substituted
    :return:
    """

    s = '{} = {!r}\n'.format(name, value)
    s = substitute_variables(s, variables)
    return s


def tag_variables_to_str(annotations, variables=None):
    """
    For each convenient annotation name in tag_map, print out a variable declaration of the form annotation = v where
    v is the value of the annotation the dictionary.  If the tag is not in the set of annotations, do nothing.
    :param annotations:
    :param variables: Dictionary of variable names to be substituted
    :return:
    """
    s = ''
    for t, v in chaise_tags.items():
        if v in annotations:
            s += variable_to_str(t, annotations[v], variables)
            s += '\n'
    return s


def annotations_to_str(annotations, var_name='annotations', variables=None):
    """
    Print out the annotation definition in annotations, substituting the python variable for each of the tags specified
    in tag_map.
    :param annotations:
    :param var_name:
    :param variables:
    :return:
    """

    var_map = {v: k for k, v in variables.items()}
    if annotations == {}:
        s = '{} = {{}}\n'.format(var_name)
    else:
        s = '{} = {{'.format(var_name)
        for t, v in annotations.items():
            if t in var_map:
                # Use variable value rather then inline annotation value.
                s += substitute_variables('{!r}:{},'.format(t, var_map[t]), variables)
            else:
                s += "'{}' : {!r},".format(t, v)
        s += '}\n'
    return s


def schema_to_str(model_root, server, catalog_id, schema_name, variables=None):

    schema = model_root.schemas[schema_name]

    s = """import argparse
from attrdict import AttrDict
from deriva.core import ErmrestCatalog, get_credential, DerivaPathError
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage import update_catalog

{table_names}

{groups}

{annotations}

{acls}

{comments}

schema_def = em.Schema.define(
        '{schema_name}',
        comment=comment,
        acls=acls,
        annotations=annotations,
    )
    
def main():
    server = {server}
    catalog_id = {catalog_id}
    schema_name = '{schema_name}'
    
    mode, replace, server, catalog_id = update_catalog.parse_args(server, catalog_id)
    update_catalog.update_schema(mode, replace, server, catalog_id, schema_name, schema_def, annotations, acls, comment)


if __name__ == "__main__":
    main()
""".format(server=server, catalog_id=catalog_id, schema_name=schema_name,
           groups=variable_to_str('groups', groups),
           annotations=variable_to_str('annotations', schema.annotations, variables=variables),
           acls=variable_to_str('acls', schema.acls, variables=variables),
           comments=variable_to_str('comment', schema.comment, variables=variables),
           table_names='table_names = [\n{}]\n'.format(str.join('', ['{!r},\n'.format(i) for i in schema.tables])))
    s = FormatCode(s, style_config=yapf_style)[0]
    return s


def catalog_to_str(model_root, server, catalog_id, variables=None):

    s = """import argparse
from attrdict import AttrDict
from deriva.core import ErmrestCatalog, get_credential, DerivaPathError
from deriva.utils.catalog.manage import update_catalog
from deriva.core.ermrest_config import tag as chaise_tags
import deriva.core.ermrest_model as em


{catalog_groups}


{tag_variables}


{annotations}


{catalog_acls}


def main():
    server = '{0}'
    catalog_id = {1}
    mode, replace, server, catalog_id = update_catalog.parse_args(server, catalog_id, is_catalog=True)
    update_catalog.update_catalog(mode, replace, server, catalog_id, annotations, acls)
    

if __name__ == "__main__":
    main()
""".format(server, catalog_id,
           catalog_groups=variable_to_str('groups', groups),
           tag_variables=tag_variables_to_str(model_root.annotations, variables=variables),
           annotations=annotations_to_str(model_root.annotations, variables=variables),
           catalog_acls=variable_to_str('acls', model_root.acls, variables=variables))
    s = FormatCode(s, style_config=yapf_style)[0]
    return s


def table_annotations_to_str(table, variables=None):
    s = tag_variables_to_str(table.annotations, variables=variables)
    s += annotations_to_str(table.annotations, var_name='table_annotations', variables=variables)
    s += variable_to_str('table_comment', table.comment, variables=variables)
    s += variable_to_str('table_acls', table.acls, variables=variables)
    s += variable_to_str('table_acl_bindings', table.acl_bindings, variables=variables)
    return s


def column_annotations_to_str(table, variables=None):
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
    s = variable_to_str('column_annotations', column_annotations, variables=variables) + '\n'
    s += variable_to_str('column_comment', column_comment, variables=variables) + '\n'
    s += variable_to_str('column_acls', column_acls, variables=variables) + '\n'
    s += variable_to_str('column_acl_bindings', column_acl_bindings, variables=variables) + '\n'
    return s


def foreign_key_defs_to_str(table, variables=None):
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
    s = substitute_variables(s, variables)
    return s


def key_defs_to_str(table, variables=None):
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
    s = substitute_variables(s, variables)
    return s


def column_defs_to_str(table, variables=None):
    system_columns = ['RID', 'RCB', 'RMB', 'RCT', 'RMT']

    s = 'column_defs = ['
    for col in table.column_definitions:
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
    return s


def table_def_to_str(provide_system):
    s = """table_def = em.Table.define(table_name,
    column_defs=column_defs,
    key_defs=key_defs,
    fkey_defs=fkey_defs,
    annotations=table_annotations,
    acls=table_acls,
    acl_bindings=table_acl_bindings,
    comment=table_comment,
    provide_system = {}
)""".format(provide_system)
    return s


def table_to_str(model_root, server, catalog_id, schema_name, table_name, variables=None):
    schema = model_root.schemas[schema_name]
    table = schema.tables[table_name]

    provide_system = True
    s = """import argparse
from attrdict import AttrDict
from deriva.core import ErmrestCatalog, get_credential, DerivaPathError
import deriva.core.ermrest_model as em
from deriva.core.ermrest_config import tag as chaise_tags
from deriva.utils.catalog.manage import update_catalog

table_name = '{table_name}'

schema_name = '{schema_name}'

{groups}

{column_annotations}

{column_defs}

{table_annotations}

{key_defs}

{fkey_defs}

{table_def}


def main(skip_args=False, mode='annotations', replace=False, server={server!r}, catalog_id={catalog_id}):
    
    if not skip_args:
        mode, replace, server, catalog_id = update_catalog.parse_args(server, catalog_id, is_table=True)
    update_catalog.update_table(mode, replace, server, catalog_id, schema_name, table_name, 
                                table_def, column_defs, key_defs, fkey_defs,
                                table_annotations, table_acls, table_acl_bindings, table_comment,
                                column_annotations, column_acls, column_acl_bindings, column_comment)


if __name__ == "__main__":
    main()""".format(server=server, catalog_id=catalog_id, table_name=table_name, schema_name=schema_name,
                     groups=variable_to_str('groups', groups),
                     column_annotations=column_annotations_to_str(table, variables=variables),
                     column_defs=column_defs_to_str(table),
                     table_annotations=table_annotations_to_str(table, variables=variables),
                     key_defs=key_defs_to_str(table),
                     fkey_defs=foreign_key_defs_to_str(table),
                     table_def=table_def_to_str(provide_system))
    s = FormatCode(s, style_config=yapf_style)[0]
    return s


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
        groups = load_deriva_manage_config(configfile)

    credential = get_credential(server)
    catalog = ErmrestCatalog('https', server, catalog_id, credentials=credential)
    model_root = catalog.getCatalogModel()

    variables = {**groups, **chaise_tags}

    print("Dumping catalog def....")
    catalog_string = catalog_to_str(model_root, server, catalog_id, variables=variables)
    catalog_string = FormatCode(catalog_string, style_config=yapf_style)[0]

    with open('{}/{}_{}.py'.format(dumpdir, server, catalog_id), 'w') as f:
        print(catalog_string, file=f)

    for schema_name in model_root.schemas:
        print("Dumping schema def for {}....".format(schema_name))
        schema_string = schema_to_str(model_root, server, catalog_id, schema_name, variables=variables)
        schema_string = FormatCode(schema_string, style_config=yapf_style)[0]

        with open('{}/{}.schema.py'.format(dumpdir, schema_name), 'w') as f:
            print(schema_string, file=f)

    for schema_name, schema in model_root.schemas.items():
        for i in schema.tables:
            print('Dumping {}:{}'.format(schema_name, i))
            table_string = table_to_str(model_root, server, catalog_id, schema_name, i, variables=variables)
            table_string = FormatCode(table_string, style_config=yapf_style)[0]
            filename = '{}/{}/{}.py'.format(dumpdir, schema_name, i)
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, 'w') as f:
                print(table_string, file=f)


if __name__ == "__main__":
    main()
