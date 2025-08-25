"""Shared utility functions and classes for the application."""
import logging
from contextlib import contextmanager
from typing import Generator

from sqlalchemy.orm import Session

from tvbingefriend_season_service.database import get_session_maker


@contextmanager
def db_session_manager() -> Generator[Session, None, None]:
    """
    Provide a transactional scope around a series of operations.
    Handles session creation, commit, rollback, and closing.
    """
    session_maker = get_session_maker()
    db = session_maker()
    try:
        yield db
        db.commit()
    except Exception as e:
        logging.error(f"Session rollback due to exception: {e}", exc_info=True)
        db.rollback()
        raise
    finally:
        db.close()
