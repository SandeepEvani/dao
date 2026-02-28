# singleton.py
# Contains helper functions to create singleton objects


class _SingletonMeta(type):
    """Metaclass that enforces singleton behaviour per class.

    Each class (including subclasses) gets its own independent instance.
    __init__ is only ever called once — subsequent calls return the cached
    instance without re-initialising it.
    """

    _instances: dict = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            # Create the instance and run __init__ exactly once.
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


def singleton(class_: type) -> type:
    """A class decorator used to create singleton classes.

    Returns the original class (not a wrapper function), so that it remains
    inheritable. The singleton behaviour is enforced via a metaclass.
    """
    return _SingletonMeta(class_.__name__, (class_,), dict(class_.__dict__))
