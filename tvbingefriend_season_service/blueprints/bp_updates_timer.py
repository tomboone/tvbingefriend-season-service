"""Update seasons from TV Maze"""
import logging

import azure.functions as func

from tvbingefriend_season_service.config import UPDATES_NCRON
from tvbingefriend_season_service.services.season_service import SeasonService

bp: func.Blueprint = func.Blueprint()


# noinspection PyUnusedLocal
@bp.function_name(name="get_updates_timer")
@bp.timer_trigger(
    arg_name="updateseasons",
    schedule=UPDATES_NCRON,
    run_on_startup=False
)
def get_updates_timer(updateseasons: func.TimerRequest) -> None:
    """Update seasons from TV Maze"""
    try:
        season_service: SeasonService = SeasonService()  # create season service
        season_service.get_updates()  # get updates
    except Exception as e:   # catch errors and log them
        logging.error(
            f"get_updates_timer: Unhandled exception. Error: {e}",
            exc_info=True
        )
        raise
