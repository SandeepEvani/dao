# File : dao.py
# Description : Implementation of the DAO


########################################################

import inspect

from .config.config import Config
from .dao_interface import IDAO
from .router.router import Router

from .sig.signature import Signature

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

        signature = Signature.create_input_signature(locals(), self._write_sig)



    def read(self, source, *args, **kwargs): ...

