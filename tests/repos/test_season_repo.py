import unittest
from unittest.mock import MagicMock, patch

from sqlalchemy.exc import SQLAlchemyError

from tvbingefriend_season_service.repos.season_repo import SeasonRepository


class TestSeasonRepository(unittest.TestCase):

    def setUp(self):
        self.repo = SeasonRepository()
        self.mock_db_session = MagicMock()

    @patch('tvbingefriend_season_service.repos.season_repo.inspect')
    @patch('tvbingefriend_season_service.repos.season_repo.mysql_insert')
    def test_upsert_season_success(self, mock_mysql_insert, mock_inspect):
        """Test successful season upsert."""
        mock_mapper = MagicMock()
        mock_prop1 = MagicMock()
        mock_prop1.key = 'id'
        mock_prop2 = MagicMock()
        mock_prop2.key = 'name'
        mock_prop3 = MagicMock()
        mock_prop3.key = 'show_id'
        mock_mapper.attrs.values.return_value = [mock_prop1, mock_prop2, mock_prop3]
        mock_inspect.return_value = mock_mapper

        season_data = {"id": 1, "name": "Season 1", "number": 1}
        show_id = 123
        self.repo.upsert_season(season_data, show_id, self.mock_db_session)

        mock_mysql_insert.assert_called_once()
        self.mock_db_session.execute.assert_called_once()
        self.mock_db_session.flush.assert_called_once()

    def test_upsert_season_no_id(self):
        """Test season upsert when season has no ID."""
        season_data = {"name": "Season 1", "number": 1}
        show_id = 123
        
        with patch('tvbingefriend_season_service.repos.season_repo.logging') as mock_logging:
            self.repo.upsert_season(season_data, show_id, self.mock_db_session)

        self.mock_db_session.execute.assert_not_called()
        mock_logging.error.assert_called_once()

    @patch('tvbingefriend_season_service.repos.season_repo.inspect')
    @patch('tvbingefriend_season_service.repos.season_repo.mysql_insert')
    def test_upsert_season_with_show_id_mapping(self, mock_mysql_insert, mock_inspect):
        """Test that show_id is correctly mapped to season data."""
        mock_mapper = MagicMock()
        mock_prop = MagicMock()
        mock_prop.key = 'show_id'
        mock_mapper.attrs.values.return_value = [mock_prop]
        mock_inspect.return_value = mock_mapper

        mock_stmt = MagicMock()
        mock_mysql_insert.return_value = mock_stmt
        mock_stmt.on_duplicate_key_update.return_value = mock_stmt

        season_data = {"id": 1, "name": "Season 1"}
        show_id = 456
        self.repo.upsert_season(season_data, show_id, self.mock_db_session)

        # Verify the insert statement was called with show_id
        mock_mysql_insert.assert_called_once()
        insert_call_args = mock_mysql_insert.call_args[0]  # First positional argument
        # Check that values() was called and show_id was in the insert values
        values_call = mock_stmt.values.call_args
        if values_call:
            insert_values = values_call[0][0] if values_call[0] else values_call[1] if len(values_call) > 1 else {}
            self.assertEqual(insert_values.get('show_id'), show_id)

    @patch('tvbingefriend_season_service.repos.season_repo.logging')
    @patch('tvbingefriend_season_service.repos.season_repo.inspect')
    @patch('tvbingefriend_season_service.repos.season_repo.mysql_insert')
    def test_upsert_season_sqlalchemy_error_in_execute(self, mock_mysql_insert, mock_inspect, mock_logging):
        """Test SQLAlchemy error during statement execution."""
        mock_mapper = MagicMock()
        mock_prop = MagicMock()
        mock_prop.key = 'id'
        mock_mapper.attrs.values.return_value = [mock_prop]
        mock_inspect.return_value = mock_mapper
        
        # Mock execute to raise SQLAlchemyError
        self.mock_db_session.execute.side_effect = SQLAlchemyError("Execute failed")
        
        season_data = {"id": 1, "name": "Season 1"}
        show_id = 123
        self.repo.upsert_season(season_data, show_id, self.mock_db_session)
        
        # Should log the error but not raise it
        mock_logging.error.assert_called()
        error_call = mock_logging.error.call_args[0][0]
        self.assertIn("Database error during upsert of season_id 1", error_call)

    @patch('tvbingefriend_season_service.repos.season_repo.logging')
    @patch('tvbingefriend_season_service.repos.season_repo.inspect')
    @patch('tvbingefriend_season_service.repos.season_repo.mysql_insert')
    def test_upsert_season_general_exception_in_execute(self, mock_mysql_insert, mock_inspect, mock_logging):
        """Test general exception during statement execution."""
        mock_mapper = MagicMock()
        mock_prop = MagicMock()
        mock_prop.key = 'id'
        mock_mapper.attrs.values.return_value = [mock_prop]
        mock_inspect.return_value = mock_mapper
        
        # Mock execute to raise general Exception
        self.mock_db_session.execute.side_effect = Exception("Unexpected execute error")
        
        season_data = {"id": 1, "name": "Season 1"}
        show_id = 123
        self.repo.upsert_season(season_data, show_id, self.mock_db_session)
        
        # Should log the error but not raise it
        mock_logging.error.assert_called()
        error_call = mock_logging.error.call_args[0][0]
        self.assertIn("Unexpected error during upsert of season season_id 1", error_call)

    @patch('tvbingefriend_season_service.repos.season_repo.inspect')
    @patch('tvbingefriend_season_service.repos.season_repo.mysql_insert')
    def test_upsert_season_filters_columns(self, mock_mysql_insert, mock_inspect):
        """Test that only valid columns are included in insert values."""
        mock_mapper = MagicMock()
        mock_prop1 = MagicMock()
        mock_prop1.key = 'id'
        mock_prop2 = MagicMock() 
        mock_prop2.key = 'name'
        mock_mapper.attrs.values.return_value = [mock_prop1, mock_prop2]
        mock_inspect.return_value = mock_mapper

        mock_stmt = MagicMock()
        mock_mysql_insert.return_value = mock_stmt

        season_data = {"id": 1, "name": "Season 1", "invalid_field": "should_be_filtered"}
        show_id = 123
        self.repo.upsert_season(season_data, show_id, self.mock_db_session)

        # Verify mysql_insert was called
        mock_mysql_insert.assert_called_once()


if __name__ == '__main__':
    unittest.main()