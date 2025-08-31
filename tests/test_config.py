import os
import unittest
from unittest.mock import patch

# Set required env vars for module import
os.environ['SQLALCHEMY_CONNECTION_STRING'] = 'mysql://test_user:test_pass@test_host:3306/test_db'

# This needs to be imported after the environment is patched
from tvbingefriend_season_service.config import _get_setting, SQLALCHEMY_CONNECTION_STRING


class TestConfig(unittest.TestCase):

    @patch.dict(os.environ, {'SQLALCHEMY_CONNECTION_STRING': 'mysql://test_user:test_pass@test_host:3306/test_db'})
    def test_db_connection_string_loaded_from_env(self):
        """Check if the DB connection string is loaded correctly from the environment."""
        # Import after patching to get the patched value
        from tvbingefriend_season_service.config import _get_setting
        result = _get_setting('SQLALCHEMY_CONNECTION_STRING')
        self.assertEqual(result, 'mysql://test_user:test_pass@test_host:3306/test_db')

    @patch.dict(os.environ, {'TEST_VAR': 'test_value'})
    def test_get_setting_found_in_env(self):
        """Test _get_setting when a variable is present in the environment."""
        self.assertEqual(_get_setting('TEST_VAR'), 'test_value')

    @patch('tvbingefriend_season_service.config._local_settings', {'TEST_VAR': 'local_value'})
    def test_get_setting_fallback_to_local_settings(self):
        """Test _get_setting falls back to local settings when env var is missing."""
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(_get_setting('TEST_VAR'), 'local_value')

    def test_get_setting_missing_required(self):
        """Test _get_setting raises ValueError when a required variable is missing."""
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "Missing required setting: 'MISSING_VAR'"):
                _get_setting('MISSING_VAR', required=True)

    def test_get_setting_missing_not_required(self):
        """Test _get_setting returns None when a non-required variable is missing."""
        with patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(_get_setting('MISSING_VAR', required=False))

    def test_get_setting_missing_not_required_with_default(self):
        """Test _get_setting returns the default value for a missing non-required variable."""
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(_get_setting('MISSING_VAR', required=False, default='default_val'), 'default_val')

    @patch.dict(os.environ, {'SEASONS_QUEUE': 'test-seasons-queue'})
    def test_seasons_queue_config(self):
        """Test SEASONS_QUEUE configuration loading."""
        # Import after patching to get the patched value
        import importlib
        import tvbingefriend_season_service.config
        importlib.reload(tvbingefriend_season_service.config)
        from tvbingefriend_season_service.config import SEASONS_QUEUE
        # Note: SEASONS_QUEUE uses SEASONS_QUEUE env var as configured
        self.assertEqual(SEASONS_QUEUE, 'test-seasons-queue')

    @patch.dict(os.environ, {'SHOW_IDS_TABLE': 'test_show_ids'})
    def test_show_ids_table_config(self):
        """Test SHOW_IDS_TABLE configuration loading."""
        # Import after patching to get the patched value
        import importlib
        import tvbingefriend_season_service.config
        importlib.reload(tvbingefriend_season_service.config)
        from tvbingefriend_season_service.config import SHOW_IDS_TABLE
        self.assertEqual(SHOW_IDS_TABLE, 'test_show_ids')


if __name__ == '__main__':
    unittest.main()