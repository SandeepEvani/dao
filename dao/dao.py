# File : dao.py
# Description : Implementation of the DAO


########################################################

from inspect import signature
from operator import call

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
        self._router = Router(confs_location)

        self._write_sig = signature(self.write)
        self._read_sig = signature(self.read)

    def write(self, data, destination, **kwargs):
        """

        :param data:
        :param destination:
        :param kwargs:
        :return:
        """

        provided_args = locals().copy()
        prefix = destination.split("://")[0]
        route = self._router.choose_method(provided_args, self._write_sig, prefix)

        return call(route["method"], **provided_args)

    def read(self, source, *args, **kwargs): ...
