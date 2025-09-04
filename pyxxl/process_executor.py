"""Process-based executor for running sync handlers in separate processes."""

import pickle
import logging
from typing import Any, Callable, Dict

from pyxxl.schema import RunData


def run_handler_in_process(handler_func: Callable, run_data_dict: Dict[str, Any]) -> Any:
    """Execute a handler function in a separate process with provided context data.

    This function is designed to be pickle-serializable and run in a separate process.
    It recreates the necessary context from the provided data without ContextVar dependencies.

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

        # Set the context data in the new process - this creates new ContextVar instances
        # in the process, avoiding serialization issues
        g.set_xxl_run_data(run_data)

        # Create a simple logger for the process that doesn't depend on ContextVar
        process_logger = logging.getLogger(f"pyxxl.process.{run_data.jobId}.{run_data.logId}")
        if not process_logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            process_logger.addHandler(handler)
            process_logger.setLevel(logging.INFO)

        # Set a process-local logger that doesn't use ContextVar
        g.set_task_logger(process_logger)

        # Execute the handler function
        return handler_func()
    except Exception as e:
        # Re-raise with additional context for debugging
        raise type(e)(f"Error in process execution of {getattr(handler_func, '__name__', 'unknown')}: {e}") from e
