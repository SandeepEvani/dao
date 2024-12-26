# File : dao_interface.py
# Description : Defines the DAO Interface

from abc import ABC, abstractmethod


class IDAO(ABC):
    """
    Class IDAO meaning the DAO interface class defines the base
    structure of the DAO class and the necessary methods in the class
    """

    @abstractmethod
    def write(self, data, destination, *args, **kwargs):
        """
        Writes data to an appropriate location
        :return:
        """
        return True

    @abstractmethod
    def read(self, source, *args, **kwargs):
        """
        Reads data from an appropriate location
        :return:
        """
        return True

    def __repr__(self):

        return "<DAO()>"
