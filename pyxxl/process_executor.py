"""Process-based executor for running sync handlers in separate processes."""

import logging
from typing import Any, Callable, Dict

from pyxxl.schema import RunData


def run_handler_in_process(handler_func: Callable, run_data_dict: Dict[str, Any], logger_factory_info: Dict[str, Any] = None) -> Any:
    """Execute a handler function in a separate process with provided context data.

    This function is designed to be pickle-serializable and run in a separate process.
    It recreates the necessary context from the provided data without ContextVar dependencies.

    Args:
        handler_func: The handler function to execute
        run_data_dict: Serialized RunData as dict to recreate context
        logger_factory_info: Serialized logger factory information to recreate proper logging

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

        # Create a proper logger using the logger factory information
        process_logger = _create_process_logger(run_data.logId, logger_factory_info)

        # Set the process-local logger in the ContextVar so g.logger works correctly
        g.set_task_logger(process_logger)

        # Execute the handler function
        return handler_func()
    except Exception as e:
        # Re-raise with additional context for debugging
        raise type(e)(f"Error in process execution of {getattr(handler_func, '__name__', 'unknown')}: {e}") from e


def _create_process_logger(log_id: int, logger_factory_info: Dict[str, Any] = None) -> logging.Logger:
    """Create a logger in the subprocess that matches the main process logger factory."""
    if logger_factory_info and logger_factory_info.get('type') == 'DiskLog':
        # Recreate DiskLog functionality in the subprocess
        from pyxxl.logger.common import TASK_FORMATTER, PyxxlFileHandler, PyxxlStreamHandler
        
        log_path = logger_factory_info['log_path']
        
        # Create a logger similar to DiskLog.get_logger
        logger = logging.getLogger(f"pyxxl.task_log.disk.task-{log_id}")
        logger.propagate = False
        logger.setLevel(logging.INFO)
        
        # Clear any existing handlers to avoid duplicates
        logger.handlers.clear()
        
        # Add stdout handler
        stdout_handler = PyxxlStreamHandler()
        stdout_handler.setFormatter(TASK_FORMATTER)
        stdout_handler.setLevel(logging.INFO)
        logger.addHandler(stdout_handler)
        
        # Add file handler  
        from pathlib import Path
        log_file_path = Path(log_path) / f"pyxxl-{log_id}.log"
        # Ensure directory exists
        log_file_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = PyxxlFileHandler(str(log_file_path), delay=True)
        file_handler.setFormatter(TASK_FORMATTER)
        file_handler.setLevel(logging.INFO)
        logger.addHandler(file_handler)
        
        return logger
    else:
        # Fallback to basic logger for unsupported factory types
        process_logger = logging.getLogger(f"pyxxl.process.{log_id}")
        if not process_logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            process_logger.addHandler(handler)
            process_logger.setLevel(logging.INFO)
        return process_logger
