# utils.py
# A collection of utility functions for DAO

######################################################################

import functools
from typing import Dict

######################################################################


def get_uri_prefix(data_store_loc: str) -> str | None:
    """

    :param data_store_loc:
    :return:
    """

    if "://" in data_store_loc:
        return data_store_loc.split("://")[0]

    return None


def flatten_kwargs(args: Dict, kwarg: str):
    """

    :param args:
    :param kwarg:
    :return:
    """

    kwarg_value = args.pop(kwarg)

    return {**args, **kwarg_value}


def filter_args(args, parameters):
    """

    :param args:
    :param parameters:
    :return:
    """

    filtered_args = {key: value for key, value in args.items() if key in parameters}

    return filtered_args


def inject_metadata(dao_operation):
    """
    This Decorator will inject the metadata of the function into the key word args.
    :param dao_operation:
    :return:
    """

    @functools.wraps(dao_operation)
    def wrapper(*args, **kwargs):

        return dao_operation(*args, **kwargs, dao_operation=dao_operation.__name__)

    return wrapper
