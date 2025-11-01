# accessor.py
# Defines the data access logic

from inspect import Parameter
from inspect import signature as signature_generator
from typing import Optional

from dao.core.router import Router
from dao.core.signature import SignatureFactory
from dao.data_store import DataStoreFactory, DataStoreRegistry


class DataAccessor:
    """Data descriptor that provides data access logic for DAO methods.

    This class handles the routing and execution of data access operations
    by coordinating with data stores, routers, and signature processors.
    """

    _initialized = set()

    def __init__(self, signature: Optional[dict] = None, doc_string: Optional[str] = None):
        """Initialize the DataAccessor with method signature and documentation.

        Args:
            signature: Optional custom signature for the accessor method.
                      If None, uses the signature of the accessor_function.
            doc_string: Optional documentation string for the accessor method.
        """
        if signature is None:
            self.signature = signature_generator(self.accessor_function)
        self.doc_string = doc_string

        self.signature_factory = SignatureFactory()
        self.data_store_factory = DataStoreFactory()

    def __get__(self, instance, owner):
        """Retrieve the accessor function when accessed as a descriptor.

        Args:
            instance: The instance that the descriptor is accessed from.
            owner: The class that owns the descriptor.

        Returns:
            The bound accessor function.
        """
        return self.accessor_function

    def __set_name__(self, owner: type, name: str):
        """Set the name and owner when descriptor is assigned to a class.

        Args:
            owner: The class that owns this descriptor.
            name: The name of the attribute in the owner class.
        """
        self.owner = owner
        self.action = name
        self.router = Router(name)

    def create_accessor_function(self):
        """Create and configure the accessor function.

        This method should be implemented to create the actual accessor
        function that will handle data operations.
        """
        ...

    def accessor_function(self, data_object, **kwargs):
        """Main accessor function that handles data operations.

        This function is called when the accessor is invoked and coordinates
        the data access logic with the appropriate data store.

        Args:
            data_object: The data object containing data store information.
            **kwargs: Additional arguments for the data operation.

        Returns:
            The result of the data access operation.
        """
        # calls the data access logic
        result = self.data_access_logic(locals().copy())

        return result

    def data_access_logic(self, provided_args):
        """Execute the core data access logic for DAO operations.

        This method processes arguments, determines the appropriate data store,
        initializes routes if needed, and executes the chosen method.

        Args:
            provided_args: Dictionary of arguments provided to the DAO method.

        Returns:
            The result of the executed data access method.
        """
        method_args, conf_args = self.process_args(provided_args)

        argument_signature = self.signature_factory.create_argument_signature(method_args, self.signature)

        # Extract the name of the data store
        data_object = method_args.get("data_object")
        data_store_object = data_object.data_store
        data_store = data_store_object.name

        data_class = conf_args.get("dao_interface_class")

        if (data_store, data_class, self.action) not in self._initialized:
            data_store_config = DataStoreRegistry.get(data_store)

            interface_object = self.data_store_factory.initialize_data_class(data_store_config, data_store, data_class)
            self.router.create_routes_from_interface_object(data_store, interface_object)
            self._initialized.add((data_store, data_class, self.action))

        # If the dao is not lazy, we can safely assume all the data stores
        # are initiated, and we can proceed to choose routes
        route = self.router.choose_route(argument_signature, data_store, conf_args)

        # choosing the required method by using the router
        method = route["method"]

        return method(**method_args)

    def process_args(self, provided_args):
        """process and segregate arguments for method execution.

        Filters out unwanted arguments, flattens keyword arguments, and
        separates method arguments from configuration arguments.

        Args:
            provided_args: Dictionary of arguments provided to the DAO method.

        Returns:
            Tuple of (method_args, conf_args) where:
                method_args: Arguments intended for the data method.
                conf_args: Configuration arguments for the DAO system.
        """
        # Filters the args from unwanted variables
        self._filter_args(provided_args, self.signature)

        # Derives the name of the keyword argument
        kwarg_name: str = [
            key for key, value in self.signature.parameters.items() if value.kind == Parameter.VAR_KEYWORD
        ][0]

        # Updating the flattened KWArg to the provided args
        kwarg_value = provided_args.pop(kwarg_name)
        provided_args.update(kwarg_value)

        # Separate the config args from method args
        method_args, conf_args = self._segregate_args(provided_args)

        return method_args, conf_args

    @staticmethod
    def _filter_args(args, signature):
        """Filter arguments to only include those defined in the method signature.

        Args:
            args: Dictionary of local variables/arguments.
            signature: The signature object defining valid parameters.
        """
        keys = list(args.keys())
        for arg_name in keys:
            if arg_name not in signature.parameters:
                args.pop(arg_name)

    @staticmethod
    def _segregate_args(args, segregation_prefix="dao_"):
        """Segregate configuration arguments from method arguments.

        Args:
            args: Dictionary of arguments to segregate.
            segregation_prefix: Prefix used to identify configuration arguments.

        Returns:
            Tuple of (method_args, conf_args) where:
                method_args: Regular method arguments.
                conf_args: Configuration arguments with the specified prefix.
        """
        confs = {}
        method_args = {}

        # Segregates the args based on the `segregation_prefix`
        for arg_key in args:
            if arg_key.startswith(segregation_prefix):
                confs.update({arg_key: args[arg_key]})
            else:
                method_args.update({arg_key: args[arg_key]})

        return method_args, confs
