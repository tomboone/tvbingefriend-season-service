import os
import unittest
from unittest.mock import patch, MagicMock, mock_open

# Set required env vars for module import
os.environ['SQLALCHEMY_CONNECTION_STRING'] = 'mysql://test_user:test_pass@test_host:3306/test_db'

from tvbingefriend_season_service.database import get_engine, get_session_maker


class TestDatabase(unittest.TestCase):

    def setUp(self):
        """Reset the global engine and session maker for each test."""
        import tvbingefriend_season_service.database
        tvbingefriend_season_service.database._db_engine = None
        tvbingefriend_season_service.database._session_maker = None
        tvbingefriend_season_service.database._cert_file_path = None

    @patch('tvbingefriend_season_service.database.create_engine')
    def test_get_engine_without_ssl(self, mock_create_engine):
        """Test get_engine without SSL certificate."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        with patch('tvbingefriend_season_service.database.SQLALCHEMY_CONNECTION_STRING', 'mysql://user:pass@host:3306/db'), \
             patch('tvbingefriend_season_service.database.MYSQL_SSL_CA_CONTENT', None):
            engine = get_engine()
        
        self.assertEqual(engine, mock_engine)
        mock_create_engine.assert_called_once_with(
            'mysql://user:pass@host:3306/db',
            echo=True,
            pool_pre_ping=True,
            connect_args={}
        )

    @patch('tvbingefriend_season_service.database.create_engine')
    @patch('tvbingefriend_season_service.database.tempfile.NamedTemporaryFile')
    def test_get_engine_with_ssl(self, mock_temp_file, mock_create_engine):
        """Test get_engine with SSL certificate."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # Mock the temporary file
        mock_file = MagicMock()
        mock_file.name = '/tmp/test_cert.pem'
        mock_temp_file.return_value.__enter__ = MagicMock(return_value=mock_file)
        mock_temp_file.return_value.__exit__ = MagicMock(return_value=None)
        
        ssl_content = """-----BEGIN CERTIFICATE-----
MIIDQTCCAimgAwIBAgITBmyfz5m/jAo54vB4ikPmljZbyjANBgkqhkiG9w0BAQsF
-----END CERTIFICATE-----"""
        
        with patch('tvbingefriend_season_service.database.SQLALCHEMY_CONNECTION_STRING', 'mysql://user:pass@host:3306/db'), \
             patch('tvbingefriend_season_service.database.MYSQL_SSL_CA_CONTENT', ssl_content):
            engine = get_engine()
        
        self.assertEqual(engine, mock_engine)
        mock_create_engine.assert_called_once()
        # Verify SSL connect_args were set
        call_args = mock_create_engine.call_args
        self.assertIn('connect_args', call_args[1])
        connect_args = call_args[1]['connect_args']
        self.assertEqual(connect_args['ssl_ca'], '/tmp/test_cert.pem')
        self.assertFalse(connect_args['ssl_disabled'])

    def test_get_engine_missing_connection_string(self):
        """Test get_engine raises ValueError when connection string is missing."""
        with patch('tvbingefriend_season_service.database.SQLALCHEMY_CONNECTION_STRING', None):
            with self.assertRaises(ValueError) as context:
                get_engine()
            self.assertIn("SQLALCHEMY_CONNECTION_STRING environment variable not set", str(context.exception))

    @patch('tvbingefriend_season_service.database.get_engine')
    @patch('tvbingefriend_season_service.database.sessionmaker')
    def test_get_session_maker(self, mock_sessionmaker, mock_get_engine):
        """Test get_session_maker creates and returns session maker."""
        mock_engine = MagicMock()
        mock_get_engine.return_value = mock_engine
        mock_session_maker = MagicMock()
        mock_sessionmaker.return_value = mock_session_maker
        
        session_maker = get_session_maker()
        
        self.assertEqual(session_maker, mock_session_maker)
        mock_sessionmaker.assert_called_once_with(bind=mock_engine)

    @patch('tvbingefriend_season_service.database.create_engine')
    @patch('tvbingefriend_season_service.config.SQLALCHEMY_CONNECTION_STRING', 'mysql://user:pass@host:3306/db')
    @patch('tvbingefriend_season_service.config.MYSQL_SSL_CA_CONTENT', None)
    def test_get_engine_caching(self, mock_create_engine):
        """Test that get_engine caches the engine instance."""
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        engine1 = get_engine()
        engine2 = get_engine()
        
        self.assertEqual(engine1, engine2)
        # create_engine should only be called once due to caching
        mock_create_engine.assert_called_once()


if __name__ == '__main__':
    unittest.main()