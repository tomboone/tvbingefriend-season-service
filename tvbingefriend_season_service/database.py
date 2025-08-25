"""Database connection for Azure SQL Database using Managed Identity."""
import tempfile
from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker

from tvbingefriend_season_service.models.base import Base  # noqa: F401
from tvbingefriend_season_service.config import SQLALCHEMY_CONNECTION_STRING, MYSQL_SSL_CA_CONTENT

_db_engine: Engine | None = None
_session_maker: sessionmaker | None = None
_cert_file_path: str | None = None  # To hold the path to our temp cert file


def get_engine() -> Engine:
    """Get database engine, creating it if necessary"""
    global _db_engine
    global _cert_file_path
    if _db_engine is None:
        if SQLALCHEMY_CONNECTION_STRING is None:
            raise ValueError("SQLALCHEMY_CONNECTION_STRING environment variable not set")

        connection_string = SQLALCHEMY_CONNECTION_STRING
        connect_args = {}
        ssl_ca_content = MYSQL_SSL_CA_CONTENT

        # Force SSL connection with minimal verification to satisfy Azure MySQL requirements
        if ssl_ca_content:
            # Write a minimal working certificate to satisfy SSL requirements
            import re
            cert_pattern = r'(-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----)'
            certificates = re.findall(cert_pattern, ssl_ca_content, re.DOTALL)
            
            if certificates:
                # Use only the first certificate (DigiCert Global Root G2) 
                first_cert = certificates[0].strip()
                if not first_cert.endswith('\n'):
                    first_cert += '\n'
                
                with tempfile.NamedTemporaryFile(
                    mode='w', delete=False, suffix='.pem', encoding='utf-8'
                ) as cert_file:
                    cert_file.write(first_cert)
                    _cert_file_path = cert_file.name
                
                # Override connection string SSL params with connect_args
                connect_args['ssl_ca'] = _cert_file_path
                connect_args['ssl_disabled'] = False  # type: ignore
                connect_args['ssl_verify_cert'] = False  # type: ignore
                connect_args['ssl_verify_identity'] = False  # type: ignore

        _db_engine = create_engine(
            connection_string,
            echo=True,
            pool_pre_ping=True,
            connect_args=connect_args
        )

    return _db_engine


def get_session_maker() -> sessionmaker:
    """Get session maker, creating it if necessary"""
    global _session_maker
    if _session_maker is None:
        _session_maker = sessionmaker(bind=get_engine())
    return _session_maker


# For backward compatibility - lazy loading
def SessionMaker():
    """Lazy-loaded session maker"""
    return get_session_maker()()
