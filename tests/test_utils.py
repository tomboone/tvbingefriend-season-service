import os
import unittest
from unittest.mock import MagicMock, patch
from sqlalchemy.exc import SQLAlchemyError

# Set required env vars for module import
os.environ['SQLALCHEMY_CONNECTION_STRING'] = 'sqlite:///:memory:'

from tvbingefriend_season_service.utils import db_session_manager


class TestUtils(unittest.TestCase):

    @patch('tvbingefriend_season_service.utils.get_session_maker')
    def test_db_session_manager_success(self, mock_get_session_maker):
        """Test db_session_manager commits on success."""
        mock_session_maker = MagicMock()
        mock_session = MagicMock()
        mock_session_maker.return_value = mock_session
        mock_get_session_maker.return_value = mock_session_maker
        
        with db_session_manager() as session:
            session.add(MagicMock())  # Simulate database operation
        
        mock_session.commit.assert_called_once()
        mock_session.close.assert_called_once()
        mock_session.rollback.assert_not_called()

    @patch('tvbingefriend_season_service.utils.get_session_maker')
    @patch('tvbingefriend_season_service.utils.logging')
    def test_db_session_manager_exception(self, mock_logging, mock_get_session_maker):
        """Test db_session_manager rolls back on exception."""
        mock_session_maker = MagicMock()
        mock_session = MagicMock()
        mock_session_maker.return_value = mock_session
        mock_get_session_maker.return_value = mock_session_maker
        
        test_exception = SQLAlchemyError("Test error")
        
        with self.assertRaises(SQLAlchemyError):
            with db_session_manager() as session:
                raise test_exception
        
        mock_session.rollback.assert_called_once()
        mock_session.close.assert_called_once()
        mock_session.commit.assert_not_called()
        mock_logging.error.assert_called_once()

    @patch('tvbingefriend_season_service.utils.get_session_maker')
    def test_db_session_manager_yields_session(self, mock_get_session_maker):
        """Test db_session_manager yields the correct session."""
        mock_session_maker = MagicMock()
        mock_session = MagicMock()
        mock_session_maker.return_value = mock_session
        mock_get_session_maker.return_value = mock_session_maker
        
        with db_session_manager() as session:
            self.assertEqual(session, mock_session)

    @patch('tvbingefriend_season_service.utils.get_session_maker')
    def test_db_session_manager_close_called_on_exception(self, mock_get_session_maker):
        """Test db_session_manager closes session even when commit/rollback fails."""
        mock_session_maker = MagicMock()
        mock_session = MagicMock()
        mock_session.rollback.side_effect = Exception("Rollback failed")
        mock_session_maker.return_value = mock_session
        mock_get_session_maker.return_value = mock_session_maker
        
        test_exception = Exception("Test error")
        
        with self.assertRaises(Exception):
            with db_session_manager() as session:
                raise test_exception
        
        mock_session.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()