# File : dao_interface.py
# Description : Defines the DAO Interface

from abc import ABC, abstractmethod
from typing import Any


class IDAO(ABC):
    """Class IDAO meaning the DAO interface class defines the base structure of the DAO
    class and the necessary methods in the class."""

    @abstractmethod
    def write(self, data, data_object, **kwargs) -> Any:
        """Writes data to an appropriate location :return: Any"""
        return True

    @abstractmethod
    def read(self, data_object, **kwargs) -> Any:
        """Reads data from an appropriate location :return: Any"""
        return True

    def __repr__(self):
        """ """
        return "<DAO()>"
