# dao_registrar.py
# registers methods from the interface class to the DAO


def register(mode: str, preference: int = 0):

    def internal_wrapper(method):
        """Monkey patches the method provided with the external parameters
        marking it for the DAO class to recognize the method as well as
        collecting the mode, preference parameters for the method :param
        method:

        :return: None
        """

        setattr(method, "__dao_register__", True)
        setattr(method, "__dao_register_params__", (mode, preference))

        return method

    return internal_wrapper
