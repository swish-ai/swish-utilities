import os
import click
from types import SimpleNamespace

GROUPS = {}
MAIN_GROPUS = {}
INITIAL = {}
MAP_TO = {}
ALL_OPTIONS = set()

class DipException(Exception):
    pass

def dip_option(*param_decls, **attrs):
    """
        Click wrapper. Adds some additional proerties to the standart click.
        Additional props:
            ns - namespace, defines new group.
            initial - initial params not provided by the user,
            groups - to which greoup to map the value,
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

            if not params.__dict__.__contains__('output_dir'):
                directory = f'{type_}_output'
                params.output_dir = os.path.join(os.getcwd(), directory)
                if not os.path.isdir(params.output_dir):
                    os.mkdir(directory)

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
            print(f"Error: --{val} is required with for group option --{current_groups[name][1]}")
            raise DipException("Input error")
        if (name, val) in MAP_TO:
            setattr(opt, MAP_TO[(name, val)], kwargs[val])
        else:
            setattr(opt, val, kwargs[val])
    validate_params(opt, name)

def setup_cli(**kwargs):
    current_groups = {key:(kwargs[val], val) for key, val in MAIN_GROPUS.items()}
    params = SimpleNamespace()
    for name, values in GROUPS.items():
        add_group(params, name, values, current_groups, kwargs)
    return params