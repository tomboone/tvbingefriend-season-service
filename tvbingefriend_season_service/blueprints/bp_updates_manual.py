"""Update seasons manually"""
import logging

import azure.functions as func

from tvbingefriend_season_service.services.season_service import SeasonService

bp: func.Blueprint = func.Blueprint()


@bp.function_name(name="get_updates_manually")
@bp.route(route="update_seasons_manually", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def get_updates_manually(req: func.HttpRequest) -> func.HttpResponse:
    """Update seasons manually

    An optional 'since' query parameter can be provided to filter updates to a
    specified period (e.g., day, week, or month)

    Args:
        req (func.HttpRequest): Request object
    Returns:
        func.HttpResponse: Response object
    """
    # Get 'since' param, default to 'day' if not present in the query string.
    since: str = req.params.get('since', 'day')

    if since not in ('day', 'week', 'month'):  # if invalid, log error and return
        logging.error(f"Invalid since parameter provided: {since}")
        return func.HttpResponse(
            "Query parameter 'since' must be 'day', 'week', or 'month'.",
            status_code=400
        )

    season_service: SeasonService = SeasonService()  # create season service
    season_service.get_updates(since)  # update seasons manually

    message = f"Getting all updates from TV Maze for the last {since} and queuing seasons for processing"

    return func.HttpResponse(message, status_code=202)
