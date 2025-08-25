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
