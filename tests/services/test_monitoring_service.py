import os
import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime, UTC

# Set required env vars for module import
os.environ['SQLALCHEMY_CONNECTION_STRING'] = 'sqlite:///:memory:'

from tvbingefriend_season_service.services.monitoring_service import MonitoringService, ImportStatus


class TestMonitoringService(unittest.TestCase):

    def setUp(self):
        self.mock_storage_service = MagicMock()
        self.service = MonitoringService(storage_service=self.mock_storage_service)

    def test_start_show_seasons_import_tracking(self):
        """Test starting season import tracking."""
        import_id = "test_import_123"
        show_id = 456
        estimated_seasons = 10
        
        self.service.start_show_seasons_import_tracking(import_id, show_id, estimated_seasons)
        
        # Verify entity was upserted to tracking table
        self.mock_storage_service.upsert_entity.assert_called_once()
        call_args = self.mock_storage_service.upsert_entity.call_args
        
        self.assertEqual(call_args[1]['table_name'], "seasonimporttracking")
        entity = call_args[1]['entity']
        self.assertEqual(entity['PartitionKey'], "show_seasons_import")
        self.assertEqual(entity['RowKey'], import_id)
        self.assertEqual(entity['Status'], ImportStatus.IN_PROGRESS.value)
        self.assertEqual(entity['ShowId'], show_id)
        self.assertEqual(entity['EstimatedSeasons'], estimated_seasons)
        self.assertEqual(entity['CompletedSeasons'], 0)
        self.assertEqual(entity['FailedSeasons'], 0)

    def test_update_season_import_progress_success(self):
        """Test updating season import progress successfully."""
        import_id = "test_import_123"
        season_id = 789
        
        # Mock existing entity
        existing_entity = {
            'PartitionKey': 'show_seasons_import',
            'RowKey': import_id,
            'CompletedSeasons': 5,
            'FailedSeasons': 1
        }
        self.mock_storage_service.get_entities.return_value = [existing_entity]
        
        self.service.update_season_import_progress(import_id, season_id, success=True)
        
        # Verify entity was fetched and updated
        self.mock_storage_service.get_entities.assert_called_once_with(
            table_name="seasonimporttracking",
            filter_query=f"PartitionKey eq 'show_seasons_import' and RowKey eq '{import_id}'"
        )
        
        # Verify upsert was called with updated entity
        self.mock_storage_service.upsert_entity.assert_called_once()
        updated_entity = self.mock_storage_service.upsert_entity.call_args[1]['entity']
        self.assertEqual(updated_entity['CompletedSeasons'], 6)  # Incremented
        self.assertEqual(updated_entity['FailedSeasons'], 1)  # Unchanged
        self.assertEqual(updated_entity['LastProcessedSeasonId'], season_id)

    def test_update_season_import_progress_failure(self):
        """Test updating season import progress with failure."""
        import_id = "test_import_123"
        season_id = 789
        
        # Mock existing entity
        existing_entity = {
            'PartitionKey': 'show_seasons_import',
            'RowKey': import_id,
            'CompletedSeasons': 5,
            'FailedSeasons': 1
        }
        self.mock_storage_service.get_entities.return_value = [existing_entity]
        
        self.service.update_season_import_progress(import_id, season_id, success=False)
        
        # Verify failed seasons was incremented
        updated_entity = self.mock_storage_service.upsert_entity.call_args[1]['entity']
        self.assertEqual(updated_entity['CompletedSeasons'], 5)  # Unchanged
        self.assertEqual(updated_entity['FailedSeasons'], 2)  # Incremented

    def test_update_season_import_progress_entity_not_found(self):
        """Test updating progress when tracking entity doesn't exist."""
        import_id = "test_import_123"
        season_id = 789
        
        self.mock_storage_service.get_entities.return_value = []
        
        with patch('tvbingefriend_season_service.services.monitoring_service.logging') as mock_logging:
            self.service.update_season_import_progress(import_id, season_id)
        
        # Should log error and not attempt upsert
        mock_logging.error.assert_called_once()
        self.mock_storage_service.upsert_entity.assert_not_called()

    def test_complete_show_seasons_import(self):
        """Test completing season import tracking."""
        import_id = "test_import_123"
        final_status = ImportStatus.COMPLETED
        
        # Mock existing entity
        existing_entity = {
            'PartitionKey': 'show_seasons_import',
            'RowKey': import_id,
            'Status': ImportStatus.IN_PROGRESS.value
        }
        self.mock_storage_service.get_entities.return_value = [existing_entity]
        
        self.service.complete_show_seasons_import(import_id, final_status)
        
        # Verify entity was updated with completion status
        updated_entity = self.mock_storage_service.upsert_entity.call_args[1]['entity']
        self.assertEqual(updated_entity['Status'], ImportStatus.COMPLETED.value)
        self.assertIn('EndTime', updated_entity)
        self.assertIn('LastActivityTime', updated_entity)

    def test_get_import_status_success(self):
        """Test getting import status successfully."""
        import_id = "test_import_123"
        expected_entity = {
            'PartitionKey': 'show_seasons_import',
            'RowKey': import_id,
            'Status': ImportStatus.IN_PROGRESS.value,
            'CompletedSeasons': 8,
            'FailedSeasons': 2
        }
        self.mock_storage_service.get_entities.return_value = [expected_entity]
        
        result = self.service.get_import_status(import_id)
        
        self.assertEqual(result, expected_entity)

    def test_get_import_status_not_found(self):
        """Test getting import status when not found."""
        import_id = "test_import_123"
        self.mock_storage_service.get_entities.return_value = []
        
        result = self.service.get_import_status(import_id)
        
        self.assertEqual(result, {})

    def test_track_retry_attempt(self):
        """Test tracking retry attempts."""
        operation_type = "season_details"
        identifier = "season_123"
        attempt = 2
        max_attempts = 3
        error = "Network timeout"
        
        self.service.track_retry_attempt(operation_type, identifier, attempt, max_attempts, error)
        
        # Verify retry entity was stored
        self.mock_storage_service.upsert_entity.assert_called_once()
        call_args = self.mock_storage_service.upsert_entity.call_args
        
        self.assertEqual(call_args[1]['table_name'], "seasonretrytracking")
        entity = call_args[1]['entity']
        self.assertEqual(entity['PartitionKey'], operation_type)
        self.assertEqual(entity['RowKey'], f"{identifier}_{attempt}")
        self.assertEqual(entity['Identifier'], identifier)
        self.assertEqual(entity['AttemptNumber'], attempt)
        self.assertEqual(entity['MaxAttempts'], max_attempts)
        self.assertEqual(entity['ErrorMessage'], error)
        self.assertIn('AttemptTime', entity)
        self.assertIn('NextRetryTime', entity)

    def test_get_failed_operations(self):
        """Test getting failed operations."""
        operation_type = "season_details"
        max_age_hours = 24
        
        with patch('tvbingefriend_season_service.services.monitoring_service.logging') as mock_logging:
            result = self.service.get_failed_operations(operation_type, max_age_hours)
        
        # Currently returns empty list as placeholder
        self.assertEqual(result, [])
        mock_logging.info.assert_called_once()

    def test_update_data_health(self):
        """Test updating data health metrics."""
        metric_name = "seasons_processed"
        value = 150
        threshold = 200
        
        self.service.update_data_health(metric_name, value, threshold)
        
        # Verify health entity was stored
        self.mock_storage_service.upsert_entity.assert_called_once()
        call_args = self.mock_storage_service.upsert_entity.call_args
        
        self.assertEqual(call_args[1]['table_name'], "seasondatahealth")
        entity = call_args[1]['entity']
        self.assertEqual(entity['PartitionKey'], "health")
        self.assertEqual(entity['RowKey'], metric_name)
        self.assertEqual(entity['Value'], str(value))
        self.assertEqual(entity['Threshold'], str(threshold))
        self.assertTrue(entity['IsHealthy'])  # 150 <= 200

    def test_update_data_health_unhealthy(self):
        """Test updating data health with unhealthy values."""
        metric_name = "error_rate"
        value = 15
        threshold = 10
        
        self.service.update_data_health(metric_name, value, threshold)
        
        entity = self.mock_storage_service.upsert_entity.call_args[1]['entity']
        self.assertFalse(entity['IsHealthy'])  # 15 > 10

    def test_check_data_freshness(self):
        """Test checking data freshness."""
        max_age_days = 7
        
        result = self.service.check_data_freshness(max_age_days)
        
        # Verify basic structure of response
        self.assertIn('last_check', result)
        self.assertIn('max_age_days', result)
        self.assertEqual(result['max_age_days'], max_age_days)
        self.assertIn('is_fresh', result)
        self.assertIn('total_seasons', result)
        
        # Verify data health was updated
        self.mock_storage_service.upsert_entity.assert_called_once()

    def test_get_health_summary(self):
        """Test getting health summary."""
        result = self.service.get_health_summary()
        
        # Verify basic structure of summary
        self.assertIn('last_check', result)
        self.assertIn('active_imports', result)
        self.assertIn('failed_operations', result)
        self.assertIn('data_freshness', result)
        self.assertIn('overall_health', result)
        self.assertEqual(result['overall_health'], "healthy")

    def test_exception_handling_in_update_progress(self):
        """Test exception handling in update_season_import_progress."""
        import_id = "test_import_123"
        season_id = 789
        
        self.mock_storage_service.get_entities.side_effect = Exception("Storage error")
        
        with patch('tvbingefriend_season_service.services.monitoring_service.logging') as mock_logging:
            self.service.update_season_import_progress(import_id, season_id)
        
        # Should log error but not raise exception
        mock_logging.error.assert_called()

    def test_exception_handling_in_complete_import(self):
        """Test exception handling in complete_show_seasons_import."""
        import_id = "test_import_123"
        final_status = ImportStatus.COMPLETED
        
        self.mock_storage_service.get_entities.side_effect = Exception("Storage error")
        
        with patch('tvbingefriend_season_service.services.monitoring_service.logging') as mock_logging:
            self.service.complete_show_seasons_import(import_id, final_status)
        
        # Should log error but not raise exception
        mock_logging.error.assert_called()


if __name__ == '__main__':
    unittest.main()