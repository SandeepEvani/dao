# File : dao.py
# Description : Implementation of the DAO


########################################################

import inspect

from .config.config import Config
from .dao_interface import IDAO
from .router.router import Router

########################################################


class DAO(IDAO):
    """ """

    def __init__(self, confs_location: str):
        """

        :param confs_location: str
        """
        confs = Config(confs_location)

        self._router = Router(confs.get_dao_objects)

        self._write_sig = inspect.signature(self.write)
        self._read_sig = inspect.signature(self.read)

    def write(self, data, destination, **kwargs):
        """

        :param data:
        :param destination:
        :param kwargs:
        :return:
        """

        local_vars = locals()

        parameters = []

        for param_key, param_value in self._write_sig.parameters.items():

            if local_vars.get(param_key):

                if param_value.kind == inspect._VAR_KEYWORD:
                    for kw_param_key, kw_param_value in local_vars[param_key].items():
                        parameters.append(
                            inspect.Parameter(
                                name=kw_param_key,
                                kind=inspect._POSITIONAL_OR_KEYWORD,
                                annotation=type(kw_param_value),
                            )
                        )
                else:
                    parameters.append(
                        inspect.Parameter(
                            name=param_key,
                            kind=inspect._POSITIONAL_OR_KEYWORD,
                            annotation=type(local_vars[param_key]),
                        )
                    )

        self.router.choose_method(inspect.Signature(parameters))

    def read(self, source, *args, **kwargs): ...
