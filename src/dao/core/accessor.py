# accessor.py
# Defines the data access logic

from __future__ import annotations

from functools import partial
from inspect import signature
from types import MethodType
from typing import Callable, Optional

from makefun import create_function

from dao.core.router import Router
from dao.core.signature import SignatureFactory
from dao.data_store import DataStore


class DataAccessor:
    """Data descriptor that provides data access logic for DAO methods.

    This class handles the routing and execution of data access operations
    by coordinating with data stores, routers, and signature processors.
    """

    _accessor_function: Optional[Callable] = None
    _initialized = set()

    def __init__(
        self,
        function: Callable = None,
        *,
        name: Optional[str] = None,
        doc_string: Optional[str] = None,
        data_object: Optional[str] = None,
    ):
        """Initialize the DataAccessor descriptor with configuration.

        The DataAccessor is a descriptor that wraps DAO methods and handles
        intelligent routing of data access operations to the appropriate backend
        interface. It uses the method signature and provided arguments to route
        calls to the best matching interface implementation.

        Args:
            function (Callable): The accessor method being decorated. This is the
                                method from the DAO class that will be enhanced
                                with routing and data access logic.
            name (str, optional): Custom name for the accessor. If not provided,
                                 uses the function's __name__. Defaults to None.
            doc_string (str, optional): Custom documentation string that will replace
                                       the function's docstring. Useful for
                                       overriding or providing enhanced docs.
                                       Defaults to None.
            data_object (str, optional): The parameter name that contains the DataObject.
                                        Allows flexibility for methods where data_object
                                        is not a direct parameter (e.g., move(source, dest)
                                        where 'source' is the data_object).
                                        Defaults to 'data_object'.
        """

        self.function = function
        self.name = name
        self.doc_string = doc_string
        self.data_object = data_object or "data_object"

        self.action = self.name or function.__name__

        self.signature = signature(function)
        self.signature_factory = SignatureFactory()
        self.router = Router(action=self.action)

    def __get__(self, instance, owner):
        """Retrieve the accessor function when accessed as a descriptor.

        This method is called when the accessor is accessed as a class attribute.
        It lazy-creates and caches the accessor function.

        Args:
            instance: The instance that the descriptor is accessed from, or None
                     if accessed from the class itself.
            owner: The class that owns this descriptor.

        Returns:
            For class access: The unbound accessor function.
            For instance access: A bound method with instance context.
        """
        # If accessed from the class, return the unbound function
        if instance is None:
            return self.function

        # Lazy initialization of the accessor function (created only once)
        if self._accessor_function is None:
            self._accessor_function = self.create_accessor_function()

        return self._accessor_function

    def create_accessor_function(self) -> Callable:
        """Create and configure the accessor function with proper signature.

        This method dynamically creates the accessor function using makefun's
        create_function utility, ensuring the generated function has the same
        signature as the original method while delegating to the _ implementation.

        The function signature is extracted from the original method to maintain
        IDE support, type hints, and docstring clarity.

        Returns:
            Callable: A dynamically created function with the proper signature
                     that delegates to self._ for actual execution.

        Notes:
            - The returned function will have the same signature as self.function
            - It delegates all execution to self._ which contains the core logic
            - This enables proper IDE autocomplete and type checking
        """
        # Create a bound method to extract the correct signature
        bound_method = MethodType(self.function, self)
        method_signature = signature(bound_method)

        name = self.name or self.function.__name__
        doc = self.doc_string or self.function.__doc__
        qual_name = self.function.__qualname__

        # Dynamically create a function with the proper signature
        # that delegates to the _ implementation
        accessor_func = create_function(
            func_signature=method_signature, func_impl=self._, name=name, qualname=qual_name, doc=doc
        )

        return accessor_func

    def _(self, **provided_args):
        """Execute the core data access logic for DAO operations.

        This method orchestrates the data access flow by:
        1. Segregating configuration from method arguments
        2. Creating argument signatures for routing
        3. Extracting data store information
        4. Initializing interfaces as needed
        5. Routing to the appropriate method
        6. Executing and returning results

        Args:
            provided_args: Dictionary of arguments provided to the DAO method.

        Returns:
            The result of the executed data access method.
        """
        # Separate configuration arguments from method arguments
        method_args, conf_args = self.segregate_args(provided_args)

        # Create argument signature for intelligent routing
        argument_signature = self.signature_factory.create_argument_signature(method_args, self.signature)

        # Extract data store information
        data_store, data_class = self._extract_data_store_info(method_args, conf_args)

        # Initialize interface if not already done
        self._initialize_interface_if_needed(data_store, data_class)

        # Route to the appropriate method
        method = self._get_routed_method(argument_signature, data_store.name, conf_args)

        # Execute and return the result
        return method(**method_args)

    def _extract_data_store_info(self, method_args: dict, conf_args: dict) -> tuple:
        """Extract data store name and class from method arguments.

        Args:
            method_args: Dictionary of method arguments.
            conf_args: Dictionary of configuration arguments.

        Returns:
            Tuple of (data_store_name, data_class) where:
                data_store_name (str): Name of the data store.
                data_class (str): Optional specific interface class name.
        """
        # Get the data_object parameter (maybe named differently)
        data_object = method_args.get(self.data_object)
        if not data_object:
            raise ValueError(f"data_object parameter '{self.data_object}' not found in method arguments")

        # Extract data store information from the data object
        data_store = data_object.data_store
        data_class = conf_args.get("dao_interface_class")

        return data_store, data_class

    def _initialize_interface_if_needed(self, data_store: DataStore, data_class: str) -> None:
        """Initialize interface and routes if not already initialized.

        Args:
            data_store: Name of the data store.
            data_class: Optional specific interface class name.
        """
        # Check if this combination has already been initialized
        cache_key = (data_store, data_class, self.action)
        if cache_key in self._initialized:
            return

        interface_object = data_store.get_interface_object(data_class)

        # Create routes from the interface object
        self.router.create_routes_from_interface_object(data_store.name, interface_object)

        # Mark as initialized
        self._initialized.add(cache_key)

    def _get_routed_method(self, argument_signature, data_store: str, conf_args: dict) -> Callable:
        """Get the appropriate method based on routing logic.

        Args:
            argument_signature: Signature of the provided arguments.
            data_store: Name of the data store.
            conf_args: Configuration arguments.

        Returns:
            Callable: The routed method to execute.
        """
        # Use router to find the best matching method
        route = self.router.choose_route(argument_signature, data_store, conf_args)
        return route["method"]

    @staticmethod
    def segregate_args(args, segregation_prefix="dao_"):
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


def data_accessor(
    function: Callable = None,
    *,
    name: Optional[str] = None,
    doc_string: Optional[str] = None,
    data_object: Optional[str] = None,
) -> partial[DataAccessor] | DataAccessor:
    """Decorator factory to create a DataAccessor descriptor for DAO methods.

    This decorator transforms a method into a DataAccessor descriptor that handles
    intelligent routing of data access operations to the appropriate backend interface.
    It supports both direct decoration and parameterized decoration patterns.

    Args:
        function (Callable, optional): The accessor method being decorated. If None,
                                       returns a partial function for parameterized
                                       decoration. Defaults to None.
        name (str, optional): Custom name for the accessor. If not provided,
                             the function's name is used. Defaults to None.
        doc_string (str, optional): Custom documentation string for the accessor.
                                   Useful for overriding or enhancing the function's
                                   docstring. Defaults to None.
        data_object (str, optional): Specifies which parameter contains the data_object.
                                    Useful for methods where data_object is not a direct
                                    parameter (e.g., move/copy methods). Defaults to None.

    Returns:
        Callable: Either a DataAccessor instance (when function is provided) or a
                 partial function for parameterized decoration (when function is None).
    """
    # Handle parameterized decoration: @data_accessor(name="custom_name")
    # When parameters are provided, function is None, so return a partial
    # function with the parameters bound, ready to receive the actual function
    if function is None:
        return partial(DataAccessor, name=name, doc_string=doc_string, data_object=data_object)

    # Handle direct decoration: @data_accessor
    # When called without parameters, function contains the method being decorated
    # Create and return the DataAccessor descriptor with all parameters
    return DataAccessor(function, name=name, doc_string=doc_string, data_object=data_object)
