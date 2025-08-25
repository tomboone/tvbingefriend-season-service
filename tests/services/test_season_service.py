import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch, call
from types import ModuleType

import azure.functions as func

# Set required env vars for module import
os.environ['SQLALCHEMY_CONNECTION_STRING'] = 'sqlite:///:memory:'

# Create a mock TVMaze module to avoid import errors
mock_tvmaze_module = ModuleType('tvbingefriend_tvmaze_client')
mock_tvmaze_module.TVMazeAPI = MagicMock
sys.modules['tvbingefriend_tvmaze_client'] = mock_tvmaze_module

from tvbingefriend_season_service.services.season_service import SeasonService
from tvbingefriend_season_service.config import SEASONS_QUEUE, SHOW_IDS_TABLE


class TestSeasonService(unittest.TestCase):

    def setUp(self):
        """Set up test environment for each test."""
        self.mock_season_repo = MagicMock()
        with patch('tvbingefriend_season_service.services.season_service.db_session_manager'), \
             patch('tvbingefriend_season_service.services.season_service.TVMazeAPI') as mock_tvmaze:
            mock_tvmaze_instance = MagicMock()
            mock_tvmaze.return_value = mock_tvmaze_instance
            self.service = SeasonService(season_repository=self.mock_season_repo)
        self.service.storage_service = MagicMock()
        self.service.tvmaze_api = MagicMock()
        self.service.monitoring_service = MagicMock()
        
        # Mock retry_service but make it actually execute the handler function
        self.service.retry_service = MagicMock()
        def mock_handle_retry(message, handler_func, operation_type):
            # Actually call the handler function for testing
            return handler_func(message)
        self.service.retry_service.handle_queue_message_with_retry.side_effect = mock_handle_retry
        
        # Mock the retry decorator to actually execute functions
        def mock_with_retry(operation_type, max_attempts=None):
            def decorator(func):
                return func  # Just return the function unchanged for testing
            return decorator
        self.service.retry_service.with_retry = mock_with_retry

    def test_start_get_all_shows_seasons(self):
        """Test starting the process of getting all shows' seasons."""
        # Mock show entities returned from storage
        mock_show_entities = [
            {"RowKey": "1", "PartitionKey": "show"},
            {"RowKey": "2", "PartitionKey": "show"},
            {"RowKey": "3", "PartitionKey": "show"}
        ]
        self.service.storage_service.get_entities.return_value = mock_show_entities
        
        import_id = self.service.start_get_all_shows_seasons()
        
        # Verify import tracking was started
        self.service.monitoring_service.start_show_seasons_import_tracking.assert_called_once()
        call_args = self.service.monitoring_service.start_show_seasons_import_tracking.call_args[1]
        self.assertEqual(call_args['show_id'], -1)  # Placeholder for bulk operation
        self.assertEqual(call_args['estimated_seasons'], 3)  # Number of shows
        
        # Verify all shows were queued
        self.assertEqual(self.service.storage_service.upload_queue_message.call_count, 3)
        expected_calls = [
            call(queue_name=SEASONS_QUEUE, message={"show_id": 1, "import_id": import_id}),
            call(queue_name=SEASONS_QUEUE, message={"show_id": 2, "import_id": import_id}),
            call(queue_name=SEASONS_QUEUE, message={"show_id": 3, "import_id": import_id})
        ]
        self.service.storage_service.upload_queue_message.assert_has_calls(expected_calls, any_order=True)
        
        # Verify entities were fetched from correct table
        self.service.storage_service.get_entities.assert_called_once_with(
            table_name=SHOW_IDS_TABLE,
            filter_query="PartitionKey eq 'show'"
        )

    def test_start_get_all_shows_seasons_no_shows(self):
        """Test starting seasons import when no shows exist."""
        self.service.storage_service.get_entities.return_value = []
        
        import_id = self.service.start_get_all_shows_seasons()
        
        # Should still return import ID but not queue anything
        self.assertIsNotNone(import_id)
        self.service.storage_service.upload_queue_message.assert_not_called()

    def test_start_get_all_shows_seasons_exception(self):
        """Test exception handling in start_get_all_shows_seasons."""
        self.service.storage_service.get_entities.side_effect = Exception("Storage error")
        
        with self.assertRaises(Exception):
            self.service.start_get_all_shows_seasons()
        
        # Should complete import with failed status
        self.service.monitoring_service.complete_show_seasons_import.assert_called()

    def test_get_show_seasons_success(self):
        """Test processing seasons for a show successfully."""
        # Mock queue message
        mock_message = MagicMock()
        mock_message.get_json.return_value = {"show_id": 123, "import_id": "test_import_id"}
        
        # Mock seasons returned from TVMaze API
        mock_seasons = [
            {"id": 1, "name": "Season 1", "number": 1},
            {"id": 2, "name": "Season 2", "number": 2}
        ]
        self.service.tvmaze_api.get_seasons.return_value = mock_seasons
        
        # Mock database session manager
        with patch('tvbingefriend_season_service.services.season_service.db_session_manager') as mock_db_mgr:
            mock_db = MagicMock()
            mock_db_mgr.return_value.__enter__ = MagicMock(return_value=mock_db)
            mock_db_mgr.return_value.__exit__ = MagicMock(return_value=None)
            
            self.service.get_show_seasons(mock_message)
        
        # Verify TVMaze API was called
        self.service.tvmaze_api.get_seasons.assert_called_once_with(123)
        
        # Verify seasons were upserted
        self.assertEqual(self.mock_season_repo.upsert_season.call_count, 2)
        expected_upsert_calls = [
            call(mock_seasons[0], 123, mock_db),
            call(mock_seasons[1], 123, mock_db)
        ]
        self.mock_season_repo.upsert_season.assert_has_calls(expected_upsert_calls)
        
        # Verify progress tracking
        progress_calls = self.service.monitoring_service.update_season_import_progress.call_args_list
        self.assertEqual(len(progress_calls), 2)

    def test_get_show_seasons_no_seasons(self):
        """Test processing when show has no seasons."""
        mock_message = MagicMock()
        mock_message.get_json.return_value = {"show_id": 123, "import_id": "test_import_id"}
        
        self.service.tvmaze_api.get_seasons.return_value = None
        
        self.service.get_show_seasons(mock_message)
        
        # Should not attempt to upsert anything
        self.mock_season_repo.upsert_season.assert_not_called()

    def test_get_show_seasons_missing_show_id(self):
        """Test processing with missing show_id in message."""
        mock_message = MagicMock()
        mock_message.get_json.return_value = {"import_id": "test_import_id"}  # Missing show_id
        
        self.service.get_show_seasons(mock_message)
        
        # Should not call TVMaze API
        self.service.tvmaze_api.get_seasons.assert_not_called()

    def test_get_show_seasons_upsert_failure(self):
        """Test handling of upsert failures."""
        mock_message = MagicMock()
        mock_message.get_json.return_value = {"show_id": 123, "import_id": "test_import_id"}
        
        mock_seasons = [{"id": 1, "name": "Season 1", "number": 1}]
        self.service.tvmaze_api.get_seasons.return_value = mock_seasons
        
        # Mock upsert to fail
        with patch('tvbingefriend_season_service.services.season_service.db_session_manager'):
            # Make the retry decorator fail
            self.service.retry_service.with_retry.return_value = MagicMock(side_effect=Exception("Upsert failed"))
            
            self.service.get_show_seasons(mock_message)
        
        # Should track failed season
        # Check that update_season_import_progress was called
        self.service.monitoring_service.update_season_import_progress.assert_called()

    def test_get_updates_success(self):
        """Test getting updates successfully."""
        mock_updates = {
            "1": 1640995200,  # timestamp
            "2": 1640995300,
            "3": 1640995400
        }
        self.service.tvmaze_api.get_show_updates.return_value = mock_updates
        
        self.service.get_updates("day")
        
        # Verify TVMaze API was called
        self.service.tvmaze_api.get_show_updates.assert_called_once_with(period="day")
        
        # Verify shows were queued for season processing
        self.assertEqual(self.service.storage_service.upload_queue_message.call_count, 3)
        expected_calls = [
            call(queue_name=SEASONS_QUEUE, message={"show_id": 1}),
            call(queue_name=SEASONS_QUEUE, message={"show_id": 2}),
            call(queue_name=SEASONS_QUEUE, message={"show_id": 3})
        ]
        self.service.storage_service.upload_queue_message.assert_has_calls(expected_calls, any_order=True)
        
        # Verify health metrics were updated
        self.service.monitoring_service.update_data_health.assert_called_once_with(
            metric_name="updates_processed",
            value=3,
            threshold=3 * 0.95  # 95% success rate threshold
        )

    def test_get_updates_no_updates(self):
        """Test get_updates when no updates are found."""
        self.service.tvmaze_api.get_show_updates.return_value = None
        
        self.service.get_updates("day")
        
        # Should not queue anything or update health metrics
        self.service.storage_service.upload_queue_message.assert_not_called()
        self.service.monitoring_service.update_data_health.assert_not_called()

    def test_get_updates_exception(self):
        """Test exception handling in get_updates."""
        self.service.tvmaze_api.get_show_updates.side_effect = Exception("API error")
        
        with self.assertRaises(Exception):
            self.service.get_updates("day")
        
        # Should update health metrics with failure
        self.service.monitoring_service.update_data_health.assert_called_once_with(
            metric_name="updates_failed",
            value=1
        )

    def test_get_updates_queue_failure(self):
        """Test handling of queue upload failures in get_updates."""
        mock_updates = {"1": 1640995200}
        self.service.tvmaze_api.get_show_updates.return_value = mock_updates
        self.service.storage_service.upload_queue_message.side_effect = Exception("Queue error")
        
        self.service.get_updates("day")
        
        # Should still update health metrics showing 0 successful
        self.service.monitoring_service.update_data_health.assert_called_once_with(
            metric_name="updates_processed",
            value=0,
            threshold=1 * 0.95
        )

    def test_get_import_status(self):
        """Test getting import status."""
        expected_status = {"status": "in_progress", "completed": 5, "failed": 1}
        self.service.monitoring_service.get_import_status.return_value = expected_status
        
        result = self.service.get_import_status("test_import_id")
        
        self.assertEqual(result, expected_status)
        self.service.monitoring_service.get_import_status.assert_called_once_with("test_import_id")

    def test_get_system_health(self):
        """Test getting system health status."""
        mock_health = {"overall_health": "healthy", "active_imports": 0}
        mock_freshness = {"is_fresh": True, "stale_count": 0}
        
        self.service.monitoring_service.get_health_summary.return_value = mock_health
        self.service.monitoring_service.check_data_freshness.return_value = mock_freshness
        
        result = self.service.get_system_health()
        
        self.assertEqual(result["overall_health"], "healthy")
        self.assertTrue(result["tvmaze_api_healthy"])
        self.assertEqual(result["data_freshness"], mock_freshness)

    def test_retry_failed_operations_success(self):
        """Test retrying failed operations successfully."""
        mock_failed_ops = [
            {"operation_id": "op1", "data": {"show_id": 1}},
            {"operation_id": "op2", "data": {"show_id": 2}}
        ]
        self.service.monitoring_service.get_failed_operations.return_value = mock_failed_ops
        self.service.retry_service.retry_failed_operation.return_value = True
        
        result = self.service.retry_failed_operations("show_seasons", 24)
        
        self.assertEqual(result["operation_type"], "show_seasons")
        self.assertEqual(result["found_failed_operations"], 2)
        self.assertEqual(result["successful_retries"], 2)
        self.assertEqual(result["failed_retries"], 0)
        
        # Verify retry service was called for each operation
        self.assertEqual(self.service.retry_service.retry_failed_operation.call_count, 2)

    def test_retry_failed_operations_with_failures(self):
        """Test retrying failed operations with some failures."""
        mock_failed_ops = [
            {"operation_id": "op1", "data": {"show_id": 1}},
            {"operation_id": "op2", "data": {"show_id": 2}}
        ]
        self.service.monitoring_service.get_failed_operations.return_value = mock_failed_ops
        # First succeeds, second fails
        self.service.retry_service.retry_failed_operation.side_effect = [True, False]
        
        result = self.service.retry_failed_operations("show_seasons", 24)
        
        self.assertEqual(result["successful_retries"], 1)
        self.assertEqual(result["failed_retries"], 1)

    def test_retry_failed_operations_with_exception(self):
        """Test retry operations when retry service raises exception."""
        mock_failed_ops = [{"operation_id": "op1", "data": {"show_id": 1}}]
        self.service.monitoring_service.get_failed_operations.return_value = mock_failed_ops
        self.service.retry_service.retry_failed_operation.side_effect = Exception("Retry error")
        
        result = self.service.retry_failed_operations("show_seasons", 24)
        
        self.assertEqual(result["successful_retries"], 0)
        self.assertEqual(result["failed_retries"], 1)
        self.assertEqual(len(result["retry_attempts"]), 1)
        self.assertIn("error", result["retry_attempts"][0])


if __name__ == '__main__':
    unittest.main()