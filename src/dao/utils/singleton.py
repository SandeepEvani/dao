# singleton.py
# Contains helper functions to create singleton objects

from functools import wraps
from typing import Callable


def singleton(class_: type) -> Callable:
    """A Class decorator used to create singleton classes"""

    # Placeholder to store the class instance
    object_instance = None

    @wraps(class_)
    def wrapper(*args, **kwargs):
        """Default wrapper which creates and stores the class instance"""

        # Refer to the object_instance in the parent function making it a closure
        nonlocal object_instance

        # Create instance if None
        if object_instance is None:
            object_instance = class_(*args, **kwargs)

        return object_instance

    return wrapper
