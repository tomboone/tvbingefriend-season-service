"""Repository for seasons"""
import logging
from typing import Any

from sqlalchemy import inspect
from sqlalchemy.dialects.mysql import Insert, insert as mysql_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, Mapper, ColumnProperty

from tvbingefriend_season_service.models.season import Season


# noinspection PyMethodMayBeStatic
class SeasonRepository:
    """Repository for seasons"""
    def upsert_season(self, season: dict[str, Any], show_id: int, db: Session) -> None:
        """Upsert a season in the database

        Args:
            season (dict[str, Any]): Season to upsert
            show_id (int): ID of the show this season belongs to
            db (Session): Database session
        """
        season_id: int | None = season.get("id")  # get season_id from season
        logging.debug(f"SeasonRepository.upsert_season: season_id: {season_id}")

        if not season_id:  # if season_id is missing, log error and return
            logging.error("season_repository.upsert_season: Error upserting season: Season must have a season_id")
            return

        mapper: Mapper = inspect(Season)  # get season mapper
        season_columns: set[str] = {  # get season columns
            prop.key for prop in mapper.attrs.values() if isinstance(prop, ColumnProperty)
        }

        insert_values: dict[str, Any] = {  # create insert values
            key: value for key, value in season.items() if key in season_columns
        }
        insert_values["id"] = season_id  # add id value to insert values
        insert_values["show_id"] = show_id  # add show_id value to insert values

        update_values: dict[str, Any] = {  # create update values
            key: value for key, value in insert_values.items() if key != "id"
        }

        try:

            # noinspection PyTypeHints
            stmt: Insert = mysql_insert(Season).values(insert_values)  # create insert statement
            stmt = stmt.on_duplicate_key_update(**update_values)  # add duplicate key update statement

            db.execute(stmt)  # execute insert statement
            db.flush()  # flush changes

        except SQLAlchemyError as e:  # catch any SQLAchemy errors and log them
            logging.error(
                f"season_repository.upsert_season: Database error during upsert of season_id {season_id}: {e}"
            )
        except Exception as e:  # catch any other errors and log them
            logging.error(
                f"season_repository.upsert_season: Unexpected error during upsert of season season_id {season_id}: {e}"
            )

    def get_seasons_by_show_id(self, show_id: int, db: Session) -> list[Season]:
        """Get all seasons for a show by its ID

        Args:
            show_id (int): Show ID
            db (Session): Database session

        Returns:
            list[Season]: List of seasons ordered by number
        """
        try:
            # Optimized query with limit for typical TV shows (most have < 20 seasons)
            seasons = (db.query(Season)
                      .filter(Season.show_id == show_id)
                      .order_by(Season.number)
                      .limit(50)  # Reasonable limit for TV show seasons
                      .all())
            return seasons
        except SQLAlchemyError as e:
            logging.error(f"season_repository.get_seasons_by_show_id: Database error getting seasons for show_id {show_id}: {e}")
            return []
        except Exception as e:
            logging.error(f"season_repository.get_seasons_by_show_id: Unexpected error getting seasons for show_id {show_id}: {e}")
            return []

    def get_season_by_id(self, season_id: int, db: Session) -> Season | None:
        """Get a season by its ID

        Args:
            season_id (int): Season ID
            db (Session): Database session

        Returns:
            Season | None: Season if found, None otherwise
        """
        try:
            season = db.query(Season).filter(Season.id == season_id).first()
            return season
        except SQLAlchemyError as e:
            logging.error(f"season_repository.get_season_by_id: Database error getting season_id {season_id}: {e}")
            return None
        except Exception as e:
            logging.error(f"season_repository.get_season_by_id: Unexpected error getting season_id {season_id}: {e}")
            return None

    def get_season_by_show_and_number(self, show_id: int, season_number: int, db: Session) -> Season | None:
        """Get a season by show ID and season number

        Args:
            show_id (int): Show ID
            season_number (int): Season number
            db (Session): Database session

        Returns:
            Season | None: Season if found, None otherwise
        """
        try:
            season = db.query(Season).filter(
                Season.show_id == show_id,
                Season.number == season_number
            ).first()
            return season
        except SQLAlchemyError as e:
            logging.error(f"season_repository.get_season_by_show_and_number: Database error getting season for show_id {show_id}, season {season_number}: {e}")
            return None
        except Exception as e:
            logging.error(f"season_repository.get_season_by_show_and_number: Unexpected error getting season for show_id {show_id}, season {season_number}: {e}")
            return None
