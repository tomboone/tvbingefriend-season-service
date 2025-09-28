"""Get season by show ID and season number"""
import json
import logging
import hashlib

import azure.functions as func

from tvbingefriend_season_service.services.season_service import SeasonService

bp: func.Blueprint = func.Blueprint()


@bp.function_name(name="get_season_by_show_and_number")
@bp.route(route="shows/{show_id:int}/seasons/{season_number:int}", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_season_by_show_and_number(req: func.HttpRequest) -> func.HttpResponse:
    """Get a season by show ID and season number

    Args:
        req (func.HttpRequest): HTTP request

    Returns:
        func.HttpResponse: HTTP response with season data
    """
    try:
        show_id = req.route_params.get('show_id')
        season_number = req.route_params.get('season_number')

        if not show_id or not season_number:
            return func.HttpResponse(
                body="Show ID and season number are required",
                status_code=400
            )

        show_id_int = int(show_id)
        season_number_int = int(season_number)

        season_service = SeasonService()
        season = season_service.get_season_by_show_and_number(show_id_int, season_number_int)

        if not season:
            return func.HttpResponse(
                body="Season not found",
                status_code=404
            )

        # Generate ETag for caching
        etag = hashlib.md5(json.dumps(season, sort_keys=True).encode(), usedforsecurity=False).hexdigest()

        # Check if client has current version
        if_none_match = req.headers.get('If-None-Match')
        if if_none_match == etag:
            return func.HttpResponse(status_code=304)

        return func.HttpResponse(
            body=json.dumps(season),
            status_code=200,
            headers={
                "Content-Type": "application/json",
                "Cache-Control": "public, max-age=3600",  # Cache for 1 hour
                "ETag": etag
            }
        )

    except ValueError:
        return func.HttpResponse(
            body="Invalid show ID or season number format",
            status_code=400
        )
    except Exception as e:
        logging.error(f"get_season_by_show_and_number: Unhandled exception: {e}", exc_info=True)
        return func.HttpResponse(
            body="Internal server error",
            status_code=500
        )