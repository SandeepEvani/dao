# router.py
# Routes data access operations to appropriate interface methods

from inspect import Parameter, getmembers, ismethod
from typing import Any, Dict, List

from typeguard import CollectionCheckStrategy, TypeCheckError, check_type

from dao.core.signature import Signature, SignatureFactory


class Router:
    """Intelligent router that matches data access operations to appropriate interface methods.

    The Router manages a table of available methods from all registered interfaces and uses
    method signatures to intelligently route DAO calls to the best matching implementation.
    It supports:
    - Multiple data stores with different interfaces
    - Primary and secondary interface selection
    - Signature-based method matching
    - Custom method registration via @register decorator

    Attributes:
        action (str): The action type (read, write, run, etc.) this router handles.
        routes (List[Dict]): Route table containing method metadata and signatures for fast lookup.
    """

    def __init__(self, action: str) -> None:
        """Initialize the Router with an action type.

        Args:
            action (str): The data access action this router handles (e.g., 'read', 'write').
        """
        self.action = action
        # Route table: list of dictionaries containing method metadata and signatures
        self.routes: List[Dict[str, Any]] = []

    def choose_route(self, args: Dict[str, Any], data_store: str, confs: Dict[str, Any]) -> Dict[str, Any]:
        """Select the best matching route for the given arguments and data store.

        This method filters available routes and finds the first (best) matching method
        based on the argument signature. Routes are pre-sorted by preference during
        table creation.

        Args:
            args: The Methods args provided in the DAO call
            data_store (str): The data store identifier for the operation.
            confs (Dict[str, Any]): Configuration options for routing.

        Returns:
            Dict[str, Any]: A route dictionary containing method and metadata.

        Raises:
            RuntimeError: If no compatible method is found for the given signature.
        """
        # Filter routes for this data store, action, and argument length
        search_space = self.filter_routes(data_store, len(args))

        # Find and return the first matching route
        route = self.get_route(search_space, args, confs)
        return route

    def filter_routes(self, data_store: str, arg_length: int) -> List[Dict[str, Any]]:
        """Filter routes based on data store, action, and argument count.

        Routes are filtered to match:
        - The target data store identifier
        - The action type (read, write, run, etc.)
        - Argument count compatibility (method's non-var args <= provided args)

        Args:
            data_store (str): Data store identifier to filter by.
            arg_length (int): Number of arguments provided to the DAO method.
            action (str): Action type (read, write, run, etc.).

        Returns:
            List[Dict[str, Any]]: Filtered routes matching the criteria.
        """
        return [
            route
            for route in self.routes
            if (route["identifier"] == data_store and route["length_non_var_args"] <= arg_length)
        ]

    def _list_methods_with_predicate(self, interface_object) -> List:
        """Extract methods from interface that match the action predicate.

        Finds all methods on the interface object that either:
        1. Start with the action prefix (e.g., 'read_' for read action)
        2. Are registered via @register decorator with matching action

        Args:
            interface_object: The interface instance to extract methods from.

        Returns:
            List: Methods matching the action predicate.
        """
        predicated_methods = []

        for method_name, method in getmembers(interface_object, predicate=ismethod):
            # Check if method name starts with the action prefix
            if method_name.startswith(self.action):
                predicated_methods.append(method)
            # Check if the method is registered with matching action
            elif getattr(method, "__dao_register_action__", None) == self.action:
                predicated_methods.append(method)

        return predicated_methods

    def get_route(
        self, search_space: List[Dict[str, Any]], args: Dict[str, Any], confs: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Find the first compatible route from the search space.

        Routes are pre-sorted by preference, so the first match is the best match.
        Compatibility is determined by the argument signature's is_compatible method.

        Args:
            search_space (List[Dict]): Pre-filtered list of candidate routes.
            args: The Methods args provided in the DAO call
            confs (Dict[str, Any]): Configuration options.

        Returns:
            Dict[str, Any]: The matched route containing the method and metadata.

        Raises:
            RuntimeError: If no compatible method is found in the search space.
        """

        if not search_space:
            raise RuntimeError("No methods available for the specified data store and action")

        for route in search_space:
            if self._is_compatible_route(route, args) and self._matches_conditions(route, args):
                return route

        raise RuntimeError(f"No compatible method found for action '{self.action}'")

    def create_routes_from_interface_object(self, data_store: str, interface_object) -> None:
        """Create and register routes from a single interface object.

        Extracts all applicable methods from the interface and creates route entries
        for each method signature combination. Routes are automatically sorted by
        preference to enable efficient first-match semantics in choose_route.

        Args:
            data_store (str): Identifier for the data store this interface belongs to.
            interface_object: The interface instance to extract methods from.

        Returns:
            None
        """
        # Extract methods matching this action
        methods = self._list_methods_with_predicate(interface_object)

        # Create route entries for each method and its signature variants
        new_routes = self._create_route_entries(
            interface_object=interface_object, methods=methods, data_store=data_store
        )

        # Merge new routes into the route table, maintaining sort order
        self.routes.extend(new_routes)
        self._sort_routes()

    @staticmethod
    def _create_route_entries(interface_object, methods: List, data_store: str) -> List[Dict[str, Any]]:
        """Create route entries for all methods and their signature combinations.

        This is the core route generation logic. For each method:
        1. Get method metadata (type, preference, class name)
        2. Generate all possible signatures for the method
        3. Create route entry for each signature variant

        The resulting routes are pre-sorted by preference to enable efficient
        first-match semantics in choose_route.

        Args:
            interface_object: The interface instance (for getting class name).
            methods (List): Methods to create routes for.
            data_store (str): Data store identifier.

        Returns:
            List[Dict[str, Any]]: Created route entries sorted by preference.
        """
        routes = []
        interface_class = interface_object.__class__.__name__

        for method in methods:
            # Generate all signature variants for this method
            signatures = SignatureFactory().create_method_signatures(method)

            # Create a route entry for each signature variant
            for signature in signatures:
                route_entry = {
                    "identifier": data_store,
                    "interface_class": interface_class,
                    "method": method,
                    "method_name": method.__name__,
                    "signature": signature,
                    "length_non_var_args": signature.len_non_var_args,
                    "length_all_args": signature.len_all_args,
                    "when": getattr(method, "__dao_when__", {}),
                }
                routes.append(route_entry)

        return routes

    def _sort_routes(self) -> None:
        """Sort routes by preference for efficient first-match routing.

        Routes are sorted by:
        1. identifier (data store) - primary grouping
        2. length_non_var_args - descending (prefer exact matches)
        4. length_all_args - ascending (prefer fewer total args)

        This sort order ensures that when filtering by data store and method type,
        the first matching route is always the best choice.

        Returns:
            None
        """
        self.routes.sort(
            key=lambda r: (
                r["identifier"],
                -r["length_non_var_args"],
                r["length_all_args"],
            )
        )

    @staticmethod
    def _is_compatible_route(route: Dict[str, Any], args: Dict[str, Any]) -> bool:
        """Check if these arguments are compatible with a target method signature.

        @when condition keys that are absent from the method's declared parameters
        are pure routing hints — they must be excluded from structural and type
        checks so they don't inflate the argument count or cause spurious mismatches.

        Args:
            route: The full route dict (contains signature and when conditions).
            args: The arguments provided in the DAO call.

        Returns:
            bool: True if arguments are compatible with the method.
        """
        signature = route["signature"]

        # Exclude @when keys that are not declared on the method — they are
        # routing hints only and must not affect structural or type matching.
        hint_only_keys = set(route.get("when", {}).keys()) - set(signature.parameters.keys())
        effective_args = {k: v for k, v in args.items() if k not in hint_only_keys}

        # Check structural compatibility
        if not Router._check_structure(signature, effective_args):
            return False

        # Check type compatibility
        return Router._check_types(signature, effective_args)

    @staticmethod
    def _check_structure(signature: Signature, args: Dict[str, Any]) -> bool:
        """Verify that all required parameters are present.

        Args:
            signature: The target method signature.
            args: The arguments provided in the DAO call.

        Returns:
            bool: True if all required parameters are provided.
        """
        # If target doesn't have varargs, argument count must match exactly
        if not signature.has_var_args and signature.len_all_args != len(args):
            return False

        # Check that all required arguments are present
        return set(signature.non_var_args).issubset(set(args))

    @staticmethod
    def _check_types(signature: Signature, args: Dict[str, Any]) -> bool:
        """Verify that provided argument types match target type annotations.

        Uses typeguard with ALL_ITEMS collection strategy so every element in
        a collection is checked — not just the first — giving deterministic
        results even with inheritance hierarchies.

        Args:
            signature: The target method signature.
            args: The arguments provided in the DAO call.

        Returns:
            bool: True if all types are compatible.
        """
        for param_name, target_param in signature.parameters.items():
            if target_param.annotation == Parameter.empty:
                continue
            try:
                check_type(
                    args[param_name],
                    target_param.annotation,
                    collection_check_strategy=CollectionCheckStrategy.ALL_ITEMS,
                )
            except TypeCheckError:
                return False
        return True

    @staticmethod
    def _matches_conditions(route: Dict[str, Any], args) -> bool:
        """Check if provided argument values satisfy all ``@when`` conditions.

        When a method is decorated with ``@when({"key": value})``, the router
        only selects it if every condition is met by the caller's arguments.
        Methods with no ``@when`` decorator always pass this check.

        Args:
            route: The route dictionary containing the ``'when'`` criteria.
            args: The arguments provided in the DAO call.

        Returns:
            bool: True if all conditions are satisfied (or none were declared).
        """
        for param_name, expected_value in route.get("when", {}).items():
            if args.get(param_name) != expected_value:
                return False
        return True
