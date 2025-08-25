"""Start get all seasons from TV Maze"""
import azure.functions as func

from tvbingefriend_season_service.services.season_service import SeasonService

bp: func.Blueprint = func.Blueprint()


# noinspection PyUnusedLocal
@bp.function_name(name="start_get_all")
@bp.route(route="start_get_seasons", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def start_get_all(req: func.HttpRequest) -> func.HttpResponse:
    """Start get all seasons from TV Maze

    Gets all show IDs from the SHOW_IDS_TABLE and queues each show for season processing.

    Args:
        req (func.HttpRequest): Request object

    Returns:
        func.HttpResponse: Response object
    """

    season_service: SeasonService = SeasonService()  # initialize season service

    import_id = season_service.start_get_all_shows_seasons()  # initiate retrieval of all seasons

    response_text = f"Getting all seasons from TV Maze for all shows. Import ID: {import_id}"  # set response text
    response = func.HttpResponse(response_text, status_code=202)  # set http response

    return response
