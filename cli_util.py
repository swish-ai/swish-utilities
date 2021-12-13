import os
import click
from pandas import DataFrame
from types import SimpleNamespace
import json
import pathlib

GROUPS = {}
MAIN_GROPUS = {}
INITIAL = {}
MAP_TO = {}
ALL_OPTIONS = set()
DC_UTILITIES_DATA_DIR = './dc_utilities_data'
DC_UTILITIES_MAPPING_FILE = os.path.join(DC_UTILITIES_DATA_DIR, 'mapping_file.csv')
DC_UTILITIES_IMORTANT_TOKEN_FILE = os.path.join(DC_UTILITIES_DATA_DIR, 'important_token_file.txt')
DC_UTILITIES_CUSTOM_TOKENS_DIR = os.path.join(DC_UTILITIES_DATA_DIR, 'custom_tokens')


class DipException(Exception):
    pass


class DipAuthException(Exception):
    pass


def dip_option(*param_decls, **attrs):
    """
        Click wrapper. Adds some additional properties to the standart click.
        Additional props:
            ns - namespace, defines new group.
            initial - initial params not provided by the user,
            groups - to which group to map the value,
            map_to - changes in group prop name
    """
    ns = None
    if "ns" in attrs:
        ns = attrs["ns"]
        del attrs["ns"]
    name = next(x for x in param_decls if x.startswith('--'))
    if name is None:
        name = next(x for x in param_decls if x.startswith('-'))
    name = name.replace('-', '')
    if ns is not None and name is not None:
        MAIN_GROPUS[ns] = name

    initial = None
    if "initial" in attrs:
        initial = attrs['initial']
        del attrs['initial']
        INITIAL[ns] = initial

    map_to = None
    if "map_to" in attrs:
        map_to = attrs['map_to']
        del attrs['map_to']

    for x in param_decls:
        if x in ALL_OPTIONS:
            raise ValueError(f'{name}: {x} already used')

    ALL_OPTIONS.update(param_decls)
    if "groups" in attrs:
        groups = attrs["groups"]
        for g in groups:
            if map_to:
                MAP_TO[(g, name)] = map_to
            GROUPS.setdefault(g, []).append(name)
        del attrs["groups"]
    return click.option(*param_decls, **attrs)


def validate_params(params, type_):
    """
    :param params:
    :param type:
    :return:
    """

    if params.enabled:
        for key in params.__dict__:
            assert params.__dict__[key] is not None, f'{key} argument is missing'

        if 'output_dir' not in params.__dict__:
            directory = f'{type_}_output'
            params.output_dir = os.path.join(os.getcwd(), directory)
        if not os.path.isdir(params.output_dir):
            pathlib.Path(params.output_dir).mkdir(parents=True, exist_ok=True)

        message = f'{type_} output directory is "{params.output_dir}'
        print(message)


def add_group(params, name, values, current_groups, kwargs):
    opt = SimpleNamespace()
    setattr(params, name, opt)
    ns = current_groups[name][0]
    setattr(opt, 'enabled', ns)
    if name in INITIAL:
        initial = INITIAL[name]
        for key, val in initial.items():
            setattr(opt, key, val)
    for val in values:
        if kwargs[val] is None and current_groups[name][0]:
            msg = f"Error: --{val} is required with for group option --{current_groups[name][1]}"
            print(msg)
            raise DipException(msg)
        if (name, val) in MAP_TO:
            setattr(opt, MAP_TO[(name, val)], kwargs[val])
        else:
            setattr(opt, val, kwargs[val])
    validate_params(opt, name)


def get_config_params(data):
    if data.get('config'):
        config_path = data.get('config')
        if os.path.isfile(config_path):
            with open(config_path, 'r') as f:
                return json.load(f)
        else:
            msg = f'Cant find configuration file {config_path}'
            click.echo(msg)
            raise DipException(msg)
    else:
        return None


def setup_important_token_file(kwargs, config_params):
    if config_params.get('important_token_file'):
        kwargs['important_token_file'] = config_params['important_token_file']
    else:
        important_tokens = config_params.get('important_tokens')
        kwargs['important_token_file'] = DC_UTILITIES_IMORTANT_TOKEN_FILE
        if not os.path.isfile(DC_UTILITIES_IMORTANT_TOKEN_FILE):
            with open(DC_UTILITIES_IMORTANT_TOKEN_FILE, 'w+') as f:
                if important_tokens:
                    f.write('\n'.join(important_tokens))
                else:
                    f.write('')


def create_masking_files_if_needed(kwargs, config_params):
    if 'mask' in kwargs or 'export_and_mask' in kwargs:
        if not os.path.isdir(DC_UTILITIES_DATA_DIR):
            os.mkdir(DC_UTILITIES_DATA_DIR)
        if not kwargs.get('mapping_path'):
            df = DataFrame.from_dict(config_params['mask_mapping'])
            df.to_csv(DC_UTILITIES_MAPPING_FILE)
            kwargs['mapping_path'] = DC_UTILITIES_MAPPING_FILE
        if not kwargs.get('important_token_file'):
            setup_important_token_file(kwargs, config_params)
        if not kwargs.get('custom_token_dir'):
            if not os.path.isdir(DC_UTILITIES_CUSTOM_TOKENS_DIR):
                os.mkdir(DC_UTILITIES_CUSTOM_TOKENS_DIR)
            kwargs['custom_token_dir'] = DC_UTILITIES_CUSTOM_TOKENS_DIR


def load_auth(kwargs):
    auth_file = kwargs.get('auth_file')
    if auth_file:
        if os.path.isfile(auth_file):
            with open(auth_file, 'r') as f:
                jsn = json.load(f)
                kwargs['username'] = jsn['username']
                kwargs['password'] = jsn['password']
        else:
            raise DipException(f"{auth_file} is not valid file or doesn't exists")


def create_data_files_if_needed(kwargs, config_params):
    create_masking_files_if_needed(kwargs, config_params)


def setup_cli(**kwargs):
    # set parameters from config but do not overide manually provided values.
    config_params = get_config_params(kwargs)
    if config_params:
        for key, val in config_params.items():
            if key in kwargs and not kwargs[key]:
                kwargs[key] = val
        create_data_files_if_needed(kwargs, config_params)

    load_auth(kwargs)

    current_groups = {key: (kwargs[val], val) for key, val in MAIN_GROPUS.items()}
    params = SimpleNamespace()
    for name, values in GROUPS.items():
        add_group(params, name, values, current_groups, kwargs)
    return params
