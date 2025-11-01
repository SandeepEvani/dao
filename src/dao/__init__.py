from dao.core import DataAccessObject
from dao.registrar import register

__all__ = ["DataAccessObject", "register"]


def hello() -> str:
    return "Hello from dao!"
