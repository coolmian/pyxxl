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
    """
    # Import here to avoid circular imports and ensure proper process isolation
    from pyxxl.ctx import g

    # Recreate the RunData from dictionary
    run_data = RunData.from_dict(run_data_dict)

    # Set the context data in the new process
    g.set_xxl_run_data(run_data)

    # Execute the handler function
    return handler_func()


def is_pickle_serializable(obj: Any) -> bool:
    """Check if an object can be pickle serialized."""
    try:
        pickle.dumps(obj)
        return True
    except (pickle.PicklingError, TypeError, AttributeError):
        return False
