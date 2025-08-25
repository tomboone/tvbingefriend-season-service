"""Service for handling retries with exponential backoff and dead letter queues."""
import logging
import time
from datetime import datetime, UTC
from typing import Any, Dict, Optional, Callable
from functools import wraps

import azure.functions as func
from tvbingefriend_azure_storage_service import StorageService  # type: ignore

from tvbingefriend_season_service.config import (
    STORAGE_CONNECTION_STRING,
    SEASONS_QUEUE
)
from tvbingefriend_season_service.services.monitoring_service import MonitoringService


# noinspection PyMethodMayBeStatic,PyUnusedLocal
class RetryService:
    """Service for handling operation retries with exponential backoff."""
    
    def __init__(
            self,
            storage_service: Optional[StorageService] = None,
            monitoring_service: Optional[MonitoringService] = None
    ) -> None:
        self.storage_service = storage_service or StorageService(STORAGE_CONNECTION_STRING)
        self.monitoring_service = monitoring_service or MonitoringService(self.storage_service)
        self.dead_letter_queue_suffix = "-deadletter"
        self.max_retry_attempts = 3
        self.base_delay_seconds = 2
    
    def with_retry(self, operation_type: str, max_attempts: Optional[int] = None):
        """Decorator for adding retry logic to functions.
        
        Args:
            operation_type: Type of operation for tracking (e.g., 'season_import', 'api_call')
            max_attempts: Maximum retry attempts (defaults to self.max_retry_attempts)
        """
        def decorator(funct: Callable) -> Callable:
            """Decorator for adding retry logic to functions."""
            @wraps(funct)
            def wrapper(*args, **kwargs):
                """Wrapper function."""
                attempts = max_attempts or self.max_retry_attempts
                last_exception = None
                
                for attempt in range(1, attempts + 1):
                    try:
                        return funct(*args, **kwargs)
                    except Exception as e:
                        last_exception = e
                        operation_id = f"{funct.__name__}_{hash(str(args) + str(kwargs))}"
                        
                        # Track retry attempt
                        self.monitoring_service.track_retry_attempt(
                            operation_type=operation_type,
                            identifier=operation_id,
                            attempt=attempt,
                            max_attempts=attempts,
                            error=str(e)
                        )
                        
                        if attempt < attempts:
                            delay = self.calculate_backoff_delay(attempt)
                            logging.warning(
                                f"Attempt {attempt}/{attempts} failed for {operation_type}:{operation_id}. "
                                f"Retrying in {delay}s. Error: {e}"
                            )
                            time.sleep(delay)
                        else:
                            logging.error(
                                f"All {attempts} attempts failed for {operation_type}:{operation_id}. Error: {e}"
                            )
                
                # All attempts failed
                raise last_exception
            return wrapper
        return decorator
    
    def calculate_backoff_delay(self, attempt: int) -> float:
        """Calculate exponential backoff delay.
        
        Args:
            attempt: Current attempt number (1-based)
            
        Returns:
            Delay in seconds
        """
        return self.base_delay_seconds * (2 ** (attempt - 1))
    
    def handle_queue_message_with_retry(
            self, message: func.QueueMessage, handler_func: Callable, operation_type: str
    ) -> bool:
        """Handle a queue message with retry logic.
        
        Args:
            message: Azure Functions queue message
            handler_func: Function to handle the message
            operation_type: Type of operation for tracking
            
        Returns:
            True if message was processed successfully, False if it should go to dead letter
        """
        message_id = getattr(message, 'id', 'unknown')
        dequeue_count = getattr(message, 'dequeue_count', 1)
        
        try:
            # Check if this message has exceeded retry attempts
            if dequeue_count > self.max_retry_attempts:
                logging.error(
                    f"Message {message_id} exceeded max retry attempts ({self.max_retry_attempts}). "
                    "Moving to dead letter queue."
                )
                self.send_to_dead_letter_queue(message, operation_type, "Max retry attempts exceeded")
                return False
            
            # Apply exponential backoff if this is a retry
            if dequeue_count > 1:
                delay = self.calculate_backoff_delay(dequeue_count - 1)
                logging.info(f"Retry attempt {dequeue_count} for message {message_id}. Applying {delay}s backoff.")
                time.sleep(delay)
            
            # Track the retry attempt
            if dequeue_count > 1:
                self.monitoring_service.track_retry_attempt(
                    operation_type=operation_type,
                    identifier=message_id,
                    attempt=dequeue_count,
                    max_attempts=self.max_retry_attempts,
                    error="Queue message retry"
                )
            
            # Process the message
            handler_func(message)
            
            logging.info(f"Successfully processed message {message_id} on attempt {dequeue_count}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to process message {message_id} on attempt {dequeue_count}: {e}")
            
            # If this was the final attempt, send to dead letter queue
            if dequeue_count >= self.max_retry_attempts:
                self.send_to_dead_letter_queue(message, operation_type, str(e))
                return False
            
            # Let the message return to queue for retry
            raise e
    
    def send_to_dead_letter_queue(self, message: func.QueueMessage, operation_type: str, error_reason: str) -> None:
        """Send a failed message to the dead letter queue.
        
        Args:
            message: Original queue message that failed
            operation_type: Type of operation that failed
            error_reason: Reason for failure
        """
        try:
            # Create dead letter message with metadata
            dead_letter_message = {
                "original_message": message.get_json(),
                "operation_type": operation_type,
                "failure_reason": error_reason,
                "original_message_id": getattr(message, 'id', 'unknown'),
                "dequeue_count": getattr(message, 'dequeue_count', 0),
                "failed_at": datetime.now(UTC).isoformat(),
                "insertion_time": getattr(
                    message, 'insertion_time', datetime.now(UTC)
                ).isoformat() if hasattr(message, 'insertion_time') else datetime.now(UTC).isoformat()
            }
            
            # Send to seasons dead letter queue
            dead_letter_queue_name = SEASONS_QUEUE + self.dead_letter_queue_suffix
            
            # Send to dead letter queue
            self.storage_service.upload_queue_message(
                queue_name=dead_letter_queue_name,
                message=dead_letter_message
            )
            
            logging.info(f"Sent failed message to dead letter queue: {dead_letter_queue_name}")
            
        except Exception as e:
            logging.error(f"Failed to send message to dead letter queue: {e}")
    
    def get_dead_letter_queue_name(self, operation_type: str) -> str:
        """Get the dead letter queue name for an operation type.
        
        Args:
            operation_type: Type of operation
            
        Returns:
            Dead letter queue name
        """
        # Since there's only one queue (SEASONS_QUEUE), all dead letters go to the same place
        return SEASONS_QUEUE + self.dead_letter_queue_suffix
    
    def process_dead_letter_queue(self, max_messages: int = 10) -> int:
        """Process messages from the seasons dead letter queue for manual intervention.
        
        Args:
            max_messages: Maximum number of messages to process
            
        Returns:
            Number of messages processed
        """
        try:
            processed = 0
            queue_name = SEASONS_QUEUE + self.dead_letter_queue_suffix
            logging.info(f"Processing dead letter queue: {queue_name}")
            
            # Note: This is a simplified implementation
            # You would need to implement queue message retrieval in your StorageService
            # or use Azure Functions Queue triggers for dead letter queues
            
            return processed
            
        except Exception as e:
            logging.error(f"Failed to process dead letter queue: {e}")
            return 0
    
    def retry_failed_operation(self, operation_type: str, operation_data: Dict[str, Any]) -> bool:
        """Manually retry a failed operation.
        
        Args:
            operation_type: Type of operation to retry
            operation_data: Data needed to retry the operation
            
        Returns:
            True if retry was successful
        """
        try:
            # Requeue the operation to the seasons queue
            self.storage_service.upload_queue_message(
                queue_name=SEASONS_QUEUE,
                message=operation_data
            )
            
            logging.info(f"Successfully requeued {operation_type} operation")
            return True
            
        except Exception as e:
            logging.error(f"Failed to retry operation {operation_type}: {e}")
            return False
    
    def get_dead_letter_statistics(self) -> Dict[str, Any]:
        """Get statistics about the seasons dead letter queue.
        
        Returns:
            Dictionary with dead letter queue statistics
        """
        try:
            stats: dict[str, Any] = {
                "last_check": datetime.now(UTC).isoformat(),
                "queues": {}
            }
            
            # Check seasons dead letter queue
            dead_letter_queue_name = SEASONS_QUEUE + self.dead_letter_queue_suffix
            
            try:
                # Note: You would need queue statistics from your StorageService
                stats["queues"][dead_letter_queue_name] = {
                    "message_count": 0,  # Placeholder
                    "oldest_message": None,  # Placeholder
                    "newest_message": None   # Placeholder
                }
            except Exception as e:
                logging.warning(f"Could not get stats for queue {dead_letter_queue_name}: {e}")
                stats["queues"][dead_letter_queue_name] = {"error": str(e)}
            
            return stats
            
        except Exception as e:
            logging.error(f"Failed to get dead letter statistics: {e}")
            return {"error": str(e)}