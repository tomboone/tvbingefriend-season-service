"""Get one show's seasons"""
import logging

import azure.functions as func

from tvbingefriend_season_service.config import SEASONS_QUEUE, STORAGE_CONNECTION_SETTING_NAME
from tvbingefriend_season_service.services.season_service import SeasonService

bp: func.Blueprint = func.Blueprint()


@bp.function_name(name="get_show_seasons")
@bp.queue_trigger(
    arg_name="seasonmsg",
    queue_name=SEASONS_QUEUE,
    connection=STORAGE_CONNECTION_SETTING_NAME
)
def get_show_seasons(seasonmsg: func.QueueMessage) -> None:
    """Get all seasons for one show

    Args:
        seasonmsg (func.QueueMessage): Show ID message
    """
    try:
        logging.info("=== PROCESSING SHOW SEASONS MESSAGE ===")
        logging.info(f"Message ID: {seasonmsg.id}")
        logging.info(f"Message content: {seasonmsg.get_body().decode()}")
        logging.info(f"Dequeue count: {seasonmsg.dequeue_count}")
        logging.info(f"Pop receipt: {seasonmsg.pop_receipt}")
        
        # Try to parse message content
        try:
            msg_data = seasonmsg.get_json()
            logging.info(f"Parsed message data: {msg_data}")
        except Exception as parse_e:
            logging.error(f"Failed to parse message JSON: {parse_e}")
            raise
        
        logging.info("Initializing SeasonService...")
        season_service: SeasonService = SeasonService()  # initialize season service
        
        logging.info("Calling season_service.get_show_seasons...")
        season_service.get_show_seasons(seasonmsg)   # get and process show seasons
        
        logging.info(f"=== SUCCESSFULLY PROCESSED MESSAGE ID: {seasonmsg.id} ===")
    except Exception as e:  # catch any exceptions, log them, and re-raise them
        logging.error(
            f"=== ERROR PROCESSING MESSAGE ID {seasonmsg.id} ===",
            exc_info=True
        )
        logging.error(f"Exception type: {type(e).__name__}")
        logging.error(f"Exception message: {str(e)}")
        raise
