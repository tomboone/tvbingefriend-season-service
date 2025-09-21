"""Get season by ID"""
import json
import logging
import hashlib

import azure.functions as func

from tvbingefriend_season_service.services.season_service import SeasonService

bp: func.Blueprint = func.Blueprint()


@bp.function_name(name="get_season_by_id")
@bp.route(route="seasons/{season_id:int}", methods=["GET"])
def get_season_by_id(req: func.HttpRequest) -> func.HttpResponse:
    """Get a season by its ID

    Args:
        req (func.HttpRequest): HTTP request

    Returns:
        func.HttpResponse: HTTP response with season data
    """
    try:
        season_id = req.route_params.get('season_id')
        if not season_id:
            return func.HttpResponse(
                body="Season ID is required",
                status_code=400
            )

        season_id_int = int(season_id)
        season_service = SeasonService()
        season = season_service.get_season_by_id(season_id_int)

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
            body="Invalid season ID format",
            status_code=400
        )
    except Exception as e:
        logging.error(f"get_season_by_id: Unhandled exception: {e}", exc_info=True)
        return func.HttpResponse(
            body="Internal server error",
            status_code=500
        )