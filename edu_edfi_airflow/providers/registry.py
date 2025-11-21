"""
Generic registry for managing backend operators (object storage, databases, etc.)
"""


class BackendRegistry:
    """
    Generic registry for backend operators.
    
    This registry manages mappings between connection parameters and their
    corresponding single and bulk operators. Used for both object storage
    backends (S3, ADLS, etc.) and database backends (Snowflake, Databricks, etc.)
    """
    
    def __init__(self, backend_type: str = "backend"):
        """
        Initialize the registry.
        
        Args:
            backend_type: Type of backend being registered (e.g., "object storage", "database")
                         Used in error messages for clarity.
        """
        self._backends = {}
        self._backend_type = backend_type
    
    def register(self, conn_param: str, single_operator, bulk_operator):
        """
        Register a backend with its connection parameter and operators.
        
        Args:
            conn_param: The connection parameter name (e.g., 's3_conn_id', 'snowflake_conn_id')
            single_operator: The operator class to use for single resource operations
            bulk_operator: The operator class to use for bulk resource operations
        """
        self._backends[conn_param] = (single_operator, bulk_operator)
    
    def get_operators(self, conn_param: str):
        """
        Get the operators for a given connection parameter.
        
        Args:
            conn_param: The connection parameter name
            
        Returns:
            Tuple of (single_operator, bulk_operator) or None if not found
        """
        return self._backends.get(conn_param)
    
    def get_registered_conn_params(self):
        """
        Get all registered connection parameters.
        
        Returns:
            A view of all registered connection parameter names
        """
        return self._backends.keys()
    
    def find_backend_for_kwargs(self, **kwargs):
        """
        Find the first matching backend from kwargs.
        
        Args:
            **kwargs: Keyword arguments to search for registered connection parameters
            
        Returns:
            Tuple of (conn_param, (single_operator, bulk_operator))
            
        Raises:
            ValueError: If no registered backend is found in kwargs
        """
        for conn_param in self._backends:
            if conn_param in kwargs and kwargs[conn_param] is not None:
                return conn_param, self._backends[conn_param]
        
        # Raise error if no backend found
        available_params = list(self._backends.keys())
        raise ValueError(
            f"No {self._backend_type} backend found. "
            f"One of {available_params} must be provided in kwargs."
        )

