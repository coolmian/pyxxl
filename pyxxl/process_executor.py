"""Process-based executor for running sync handlers in separate processes."""

import pickle
from typing import Any, Callable, Dict

from pyxxl.schema import RunData


def run_handler_in_process(handler_func: Callable, run_data_dict: Dict[str, Any]) -> Any:
    """Execute a handler function in a separate process with provided context data.

    This function is designed to be pickle-serializable and run in a separate process.
    It recreates the necessary context from the provided data.

    Args:
        handler_func: The handler function to execute
        run_data_dict: Serialized RunData as dict to recreate context

    Returns:
        The result of the handler function execution
    
    Raises:
        Exception: Any exception that occurs during handler execution
    """
    try:
        # Import here to avoid circular imports and ensure proper process isolation
        from pyxxl.ctx import g

        # Recreate the RunData from dictionary
        run_data = RunData.from_dict(run_data_dict)

        # Set the context data in the new process
        g.set_xxl_run_data(run_data)

        # Execute the handler function
        return handler_func()
    except Exception as e:
        # Re-raise with additional context for debugging
        raise type(e)(f"Error in process execution of {getattr(handler_func, '__name__', 'unknown')}: {e}") from e


def is_pickle_serializable(obj: Any) -> bool:
    """Check if an object can be pickle serialized.
    
    This function specifically tests if the object and its context can be
    safely serialized for multiprocessing, including checking for common
    issues like ContextVar references.
    """
    try:
        # First, try to pickle the object itself
        pickle.dumps(obj)
        
        # For functions, also try to serialize with a minimal context
        # to catch ContextVar-related issues early
        if callable(obj):
            # Test if the function can be called without context issues
            # by attempting to pickle it in a clean environment
            import contextvars
            
            # Create a minimal context to test serialization
            ctx = contextvars.copy_context()
            
            def test_in_context():
                return pickle.dumps(obj)
            
            # Try to serialize the function within a context
            ctx.run(test_in_context)
            
        return True
    except (pickle.PicklingError, TypeError, AttributeError):
        return False
