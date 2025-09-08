"""
Test for the file handle leak fix in process executor.
"""

import asyncio
import tempfile
import logging
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

import pytest

from pyxxl.process_executor import run_handler_in_process


def simple_logging_handler():
    """Handler that does some logging and returns a result"""
    from pyxxl.ctx import g
    
    logger = g.logger
    logger.info("Starting handler execution")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    logger.info("Handler execution completed")
    
    return "Handler completed successfully"


def failing_logging_handler():
    """Handler that logs and then fails"""
    from pyxxl.ctx import g
    
    logger = g.logger
    logger.info("Starting failing handler")
    logger.error("About to fail")
    
    raise RuntimeError("Handler intentionally failed")


@pytest.mark.asyncio
async def test_process_executor_logging():
    """Test that process executor correctly handles logging"""
    with tempfile.TemporaryDirectory() as temp_dir:
        log_path = Path(temp_dir)
        
        logger_factory_info = {
            'type': 'DiskLog',
            'log_path': str(log_path),
            'log_tail_lines': 1000,
            'expired_seconds': 1209600,
        }
        
        run_data_dict = {
            'jobId': 123,
            'logId': 456,
            'executorHandler': 'test_handler',
            'executorBlockStrategy': 'DISCARD_LATER',
            'executorParams': '',
            'executorTimeout': 0,
            'logDateTime': 1699123456000,
            'broadcastIndex': 0,
            'broadcastTotal': 1
        }
        
        loop = asyncio.get_event_loop()
        with ProcessPoolExecutor(max_workers=1) as executor:
            result = await loop.run_in_executor(
                executor,
                run_handler_in_process,
                simple_logging_handler,
                run_data_dict,
                logger_factory_info
            )
        
        assert result == "Handler completed successfully"
        
        # Check that log file was created
        expected_log_file = log_path / "pyxxl-456.log"
        assert expected_log_file.exists()
        
        # Check log file content
        log_content = expected_log_file.read_text()
        assert "Starting handler execution" in log_content
        assert "This is a warning message" in log_content
        assert "This is an error message" in log_content
        assert "Handler execution completed" in log_content


@pytest.mark.asyncio
async def test_process_executor_error_handling():
    """Test that process executor correctly handles errors while logging"""
    with tempfile.TemporaryDirectory() as temp_dir:
        log_path = Path(temp_dir)
        
        logger_factory_info = {
            'type': 'DiskLog',
            'log_path': str(log_path),
            'log_tail_lines': 1000,
            'expired_seconds': 1209600,
        }
        
        run_data_dict = {
            'jobId': 789,
            'logId': 101112,
            'executorHandler': 'failing_handler',
            'executorBlockStrategy': 'DISCARD_LATER',
            'executorParams': '',
            'executorTimeout': 0,
            'logDateTime': 1699123456000,
            'broadcastIndex': 0,
            'broadcastTotal': 1
        }
        
        loop = asyncio.get_event_loop()
        with ProcessPoolExecutor(max_workers=1) as executor:
            with pytest.raises(RuntimeError, match="Handler intentionally failed"):
                await loop.run_in_executor(
                    executor,
                    run_handler_in_process,
                    failing_logging_handler,
                    run_data_dict,
                    logger_factory_info
                )
        
        # Check that log file was created even though handler failed
        expected_log_file = log_path / "pyxxl-101112.log"
        assert expected_log_file.exists()
        
        # Check log file content shows the failure
        log_content = expected_log_file.read_text()
        assert "Starting failing handler" in log_content
        assert "About to fail" in log_content


@pytest.mark.asyncio
async def test_multiple_concurrent_process_executions():
    """Test multiple concurrent process executions to verify no file handle leaks"""
    with tempfile.TemporaryDirectory() as temp_dir:
        log_path = Path(temp_dir)
        
        logger_factory_info = {
            'type': 'DiskLog',
            'log_path': str(log_path),
            'log_tail_lines': 1000,
            'expired_seconds': 1209600,
        }
        
        # Run multiple concurrent tasks
        tasks = []
        loop = asyncio.get_event_loop()
        
        for i in range(5):
            run_data_dict = {
                'jobId': 1000 + i,
                'logId': 2000 + i,
                'executorHandler': f'test_handler_{i}',
                'executorBlockStrategy': 'DISCARD_LATER',
                'executorParams': '',
                'executorTimeout': 0,
                'logDateTime': 1699123456000,
                'broadcastIndex': 0,
                'broadcastTotal': 1
            }
            
            with ProcessPoolExecutor(max_workers=1) as executor:
                task = loop.run_in_executor(
                    executor,
                    run_handler_in_process,
                    simple_logging_handler,
                    run_data_dict,
                    logger_factory_info
                )
                tasks.append(task)
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)
        
        # Verify all tasks completed successfully
        for result in results:
            assert result == "Handler completed successfully"
        
        # Verify all log files were created
        log_files = list(log_path.glob("*.log"))
        assert len(log_files) == 5
        
        # Verify each log file has content
        for log_file in log_files:
            log_content = log_file.read_text()
            assert "Starting handler execution" in log_content
            assert "Handler execution completed" in log_content