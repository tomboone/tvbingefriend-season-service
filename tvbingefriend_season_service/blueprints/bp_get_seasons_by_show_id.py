"""Get seasons for a specific show by show ID"""
import json
import logging

import azure.functions as func

from tvbingefriend_season_service.services.season_service import SeasonService

bp: func.Blueprint = func.Blueprint()


@bp.function_name(name="get_seasons_by_show_id")
@bp.route(route="shows/{show_id:int}/seasons", methods=["GET"])
def get_seasons_by_show_id(req: func.HttpRequest) -> func.HttpResponse:
    """Get all seasons for a show by its ID

    Args:
        req (func.HttpRequest): HTTP request

    Returns:
        func.HttpResponse: HTTP response with seasons data
    """
    try:
        show_id = req.route_params.get('show_id')
        if not show_id:
            return func.HttpResponse(
                body="Show ID is required",
                status_code=400
            )

        show_id_int = int(show_id)
        season_service = SeasonService()
        seasons = season_service.get_seasons_by_show_id(show_id_int)

        # Generate ETag for caching
        import hashlib
        etag = hashlib.md5(json.dumps(seasons, sort_keys=True).encode(), usedforsecurity=False).hexdigest()

        # Check if client has current version
        if_none_match = req.headers.get('If-None-Match')
        if if_none_match == etag:
            return func.HttpResponse(status_code=304)

        return func.HttpResponse(
            body=json.dumps(seasons),
            status_code=200,
            headers={
                "Content-Type": "application/json",
                "Cache-Control": "public, max-age=3600",  # Cache for 1 hour
                "ETag": etag
            }
        )

    except ValueError:
        return func.HttpResponse(
            body="Invalid show ID format",
            status_code=400
        )
    except Exception as e:
        logging.error(f"get_seasons_by_show_id: Unhandled exception: {e}", exc_info=True)
        return func.HttpResponse(
            body="Internal server error",
            status_code=500
        )