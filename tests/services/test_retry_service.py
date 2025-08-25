import os
import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime, UTC
import time

import azure.functions as func

# Set required env vars for module import
os.environ['SQLALCHEMY_CONNECTION_STRING'] = 'sqlite:///:memory:'

from tvbingefriend_season_service.services.retry_service import RetryService
from tvbingefriend_season_service.config import SEASONS_QUEUE


class TestRetryService(unittest.TestCase):

    def setUp(self):
        self.mock_storage_service = MagicMock()
        self.mock_monitoring_service = MagicMock()
        self.service = RetryService(
            storage_service=self.mock_storage_service,
            monitoring_service=self.mock_monitoring_service
        )

    def test_with_retry_decorator_success(self):
        """Test retry decorator with successful function execution."""
        @self.service.with_retry('test_operation')
        def test_function(x, y):
            return x + y
        
        result = test_function(2, 3)
        self.assertEqual(result, 5)
        
        # Should not track any retry attempts on success
        self.mock_monitoring_service.track_retry_attempt.assert_not_called()

    def test_with_retry_decorator_eventual_success(self):
        """Test retry decorator with eventual success after failures."""
        attempt_count = 0
        
        @self.service.with_retry('test_operation', max_attempts=3)
        def test_function():
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise Exception(f"Attempt {attempt_count} failed")
            return "success"
        
        with patch('time.sleep'):  # Skip actual sleep delays
            result = test_function()
        
        self.assertEqual(result, "success")
        self.assertEqual(attempt_count, 3)
        
        # Should track the failed attempts
        self.assertEqual(self.mock_monitoring_service.track_retry_attempt.call_count, 2)

    def test_with_retry_decorator_all_attempts_fail(self):
        """Test retry decorator when all attempts fail."""
        @self.service.with_retry('test_operation', max_attempts=2)
        def test_function():
            raise ValueError("Always fails")
        
        with patch('time.sleep'):
            with self.assertRaises(ValueError):
                test_function()
        
        # Should track all failed attempts
        self.assertEqual(self.mock_monitoring_service.track_retry_attempt.call_count, 2)

    def test_calculate_backoff_delay(self):
        """Test exponential backoff delay calculation."""
        self.assertEqual(self.service.calculate_backoff_delay(1), 2)  # 2 * (2^0)
        self.assertEqual(self.service.calculate_backoff_delay(2), 4)  # 2 * (2^1)
        self.assertEqual(self.service.calculate_backoff_delay(3), 8)  # 2 * (2^2)

    def test_handle_queue_message_with_retry_success(self):
        """Test successful queue message handling."""
        mock_message = MagicMock()
        mock_message.id = "test_message_123"
        mock_message.dequeue_count = 1
        
        mock_handler = MagicMock()
        
        result = self.service.handle_queue_message_with_retry(
            mock_message, mock_handler, "test_operation"
        )
        
        self.assertTrue(result)
        mock_handler.assert_called_once_with(mock_message)

    def test_handle_queue_message_with_retry_max_attempts_exceeded(self):
        """Test queue message handling when max attempts exceeded."""
        mock_message = MagicMock()
        mock_message.id = "test_message_123"
        mock_message.dequeue_count = 4  # Exceeds default max of 3
        
        mock_handler = MagicMock()
        
        result = self.service.handle_queue_message_with_retry(
            mock_message, mock_handler, "test_operation"
        )
        
        self.assertFalse(result)
        mock_handler.assert_not_called()
        # Should send to dead letter queue
        self.mock_storage_service.upload_queue_message.assert_called_once()

    def test_handle_queue_message_with_retry_with_backoff(self):
        """Test queue message handling with retry backoff."""
        mock_message = MagicMock()
        mock_message.id = "test_message_123"
        mock_message.dequeue_count = 2  # This is a retry
        
        mock_handler = MagicMock()
        
        with patch('time.sleep') as mock_sleep:
            result = self.service.handle_queue_message_with_retry(
                mock_message, mock_handler, "test_operation"
            )
        
        self.assertTrue(result)
        # Should apply backoff for retry attempt
        mock_sleep.assert_called_once_with(2.0)  # 2^(2-1) * base_delay
        
        # Should track retry attempt
        self.mock_monitoring_service.track_retry_attempt.assert_called_once()

    def test_handle_queue_message_with_retry_handler_failure(self):
        """Test queue message handling when handler fails."""
        mock_message = MagicMock()
        mock_message.id = "test_message_123"
        mock_message.dequeue_count = 1
        
        mock_handler = MagicMock()
        mock_handler.side_effect = Exception("Handler failed")
        
        with self.assertRaises(Exception):
            self.service.handle_queue_message_with_retry(
                mock_message, mock_handler, "test_operation"
            )

    def test_handle_queue_message_final_attempt_failure(self):
        """Test queue message handling on final attempt failure."""
        mock_message = MagicMock()
        mock_message.id = "test_message_123"
        mock_message.dequeue_count = 3  # Final attempt
        
        mock_handler = MagicMock()
        mock_handler.side_effect = Exception("Final failure")
        
        result = self.service.handle_queue_message_with_retry(
            mock_message, mock_handler, "test_operation"
        )
        
        self.assertFalse(result)
        # Should send to dead letter queue
        self.mock_storage_service.upload_queue_message.assert_called_once()

    def test_send_to_dead_letter_queue(self):
        """Test sending message to dead letter queue."""
        mock_message = MagicMock()
        mock_message.get_json.return_value = {"show_id": 123}
        mock_message.id = "test_message_123"
        mock_message.dequeue_count = 3
        mock_message.insertion_time = datetime.now(UTC)
        
        self.service.send_to_dead_letter_queue(
            mock_message, "test_operation", "Test error"
        )
        
        # Verify dead letter message was uploaded
        self.mock_storage_service.upload_queue_message.assert_called_once()
        call_args = self.mock_storage_service.upload_queue_message.call_args
        
        self.assertEqual(call_args[1]['queue_name'], SEASONS_QUEUE + "-deadletter")
        dead_letter_msg = call_args[1]['message']
        self.assertEqual(dead_letter_msg['original_message'], {"show_id": 123})
        self.assertEqual(dead_letter_msg['operation_type'], "test_operation")
        self.assertEqual(dead_letter_msg['failure_reason'], "Test error")

    def test_get_dead_letter_queue_name(self):
        """Test getting dead letter queue name."""
        result = self.service.get_dead_letter_queue_name("any_operation")
        expected = SEASONS_QUEUE + "-deadletter"
        self.assertEqual(result, expected)

    def test_retry_failed_operation_success(self):
        """Test successful retry of failed operation."""
        operation_type = "season_import"
        operation_data = {"show_id": 123}
        
        result = self.service.retry_failed_operation(operation_type, operation_data)
        
        self.assertTrue(result)
        self.mock_storage_service.upload_queue_message.assert_called_once_with(
            queue_name=SEASONS_QUEUE,
            message=operation_data
        )

    def test_retry_failed_operation_failure(self):
        """Test failed retry of operation."""
        operation_type = "season_import"
        operation_data = {"show_id": 123}
        
        self.mock_storage_service.upload_queue_message.side_effect = Exception("Queue error")
        
        result = self.service.retry_failed_operation(operation_type, operation_data)
        
        self.assertFalse(result)

    def test_process_dead_letter_queue(self):
        """Test processing dead letter queue."""
        result = self.service.process_dead_letter_queue(max_messages=5)
        
        # Currently returns 0 as placeholder implementation
        self.assertEqual(result, 0)

    def test_get_dead_letter_statistics(self):
        """Test getting dead letter queue statistics."""
        result = self.service.get_dead_letter_statistics()
        
        # Verify basic structure
        self.assertIn('last_check', result)
        self.assertIn('queues', result)
        
        dead_letter_queue_name = SEASONS_QUEUE + "-deadletter"
        self.assertIn(dead_letter_queue_name, result['queues'])
        
        queue_stats = result['queues'][dead_letter_queue_name]
        self.assertIn('message_count', queue_stats)
        self.assertEqual(queue_stats['message_count'], 0)  # Placeholder value

    def test_send_to_dead_letter_queue_exception(self):
        """Test exception handling in send_to_dead_letter_queue."""
        mock_message = MagicMock()
        mock_message.get_json.side_effect = Exception("JSON parsing error")
        
        with patch('tvbingefriend_season_service.services.retry_service.logging') as mock_logging:
            self.service.send_to_dead_letter_queue(
                mock_message, "test_operation", "Test error"
            )
        
        # Should log error but not raise exception
        mock_logging.error.assert_called()

    def test_get_dead_letter_statistics_exception(self):
        """Test exception handling in get_dead_letter_statistics."""
        with patch('tvbingefriend_season_service.services.retry_service.datetime') as mock_datetime:
            mock_datetime.now.side_effect = Exception("Time error")
            
            result = self.service.get_dead_letter_statistics()
        
        self.assertIn('error', result)

    def test_process_dead_letter_queue_exception(self):
        """Test exception handling in process_dead_letter_queue."""
        with patch('tvbingefriend_season_service.services.retry_service.logging') as mock_logging:
            # Force an exception by patching something the method uses
            mock_logging.info.side_effect = Exception("Logging error")
            
            result = self.service.process_dead_letter_queue()
        
        self.assertEqual(result, 0)
        mock_logging.error.assert_called()


if __name__ == '__main__':
    unittest.main()