"""Blueprint for health monitoring and system status endpoints."""
import logging
import json

import azure.functions as func

from tvbingefriend_season_service.services.season_service import SeasonService

bp: func.Blueprint = func.Blueprint()


# noinspection PyUnusedLocal
@bp.function_name(name="health_check")
@bp.route(route="health", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """Health check endpoint for monitoring system status.
    
    Args:
        req: HTTP request object
        
    Returns:
        HTTP response with health status
    """
    try:
        season_service = SeasonService()
        
        # Get comprehensive health status
        health_status = season_service.get_system_health()
        
        # Determine overall health
        is_healthy = all([
            health_status.get('overall_health') == 'healthy',
            not health_status.get('rate_limiting', {}).get('in_backoff_period', False),
            health_status.get('data_freshness', {}).get('is_fresh', True)
        ])
        
        status_code = 200 if is_healthy else 503
        
        response_data = {
            "status": "healthy" if is_healthy else "unhealthy",
            "timestamp": health_status.get('last_check'),
            "details": health_status
        }
        
        return func.HttpResponse(
            body=json.dumps(response_data, indent=2),
            status_code=status_code,
            headers={"Content-Type": "application/json"}
        )
        
    except Exception as e:
        logging.error(f"Health check failed: {e}")
        return func.HttpResponse(
            body=json.dumps({
                "status": "error",
                "error": str(e)
            }),
            status_code=500,
            headers={"Content-Type": "application/json"}
        )


@bp.function_name(name="import_status")
@bp.route(route="import_status", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
def import_status(req: func.HttpRequest) -> func.HttpResponse:
    """Get status of a specific season import operation.
    
    Args:
        req: HTTP request object with import_id parameter
        
    Returns:
        HTTP response with import status
    """
    try:
        import_id = req.params.get('import_id')
        if not import_id:
            return func.HttpResponse(
                body=json.dumps({"error": "import_id parameter is required"}),
                status_code=400,
                headers={"Content-Type": "application/json"}
            )
        
        season_service = SeasonService()
        import_status_data = season_service.get_import_status(import_id)
        
        if not import_status_data:
            return func.HttpResponse(
                body=json.dumps({"error": f"Import {import_id} not found"}),
                status_code=404,
                headers={"Content-Type": "application/json"}
            )
        
        return func.HttpResponse(
            body=json.dumps(import_status_data, indent=2),
            status_code=200,
            headers={"Content-Type": "application/json"}
        )
        
    except Exception as e:
        logging.error(f"Failed to get import status: {e}")
        return func.HttpResponse(
            body=json.dumps({"error": str(e)}),
            status_code=500,
            headers={"Content-Type": "application/json"}
        )


@bp.function_name(name="retry_failed_operations")
@bp.route(route="retry_operations", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def retry_failed_operations(req: func.HttpRequest) -> func.HttpResponse:
    """Retry failed operations of a specific type.
    
    Args:
        req: HTTP request object with operation_type parameter
        
    Returns:
        HTTP response with retry results
    """
    try:
        operation_type = req.params.get('operation_type')
        if not operation_type:
            return func.HttpResponse(
                body=json.dumps({"error": "operation_type parameter is required"}),
                status_code=400,
                headers={"Content-Type": "application/json"}
            )
        
        max_age_hours = int(req.params.get('max_age_hours', 24))
        
        season_service = SeasonService()
        retry_results = season_service.retry_failed_operations(operation_type, max_age_hours)
        
        return func.HttpResponse(
            body=json.dumps(retry_results, indent=2),
            status_code=200,
            headers={"Content-Type": "application/json"}
        )
        
    except Exception as e:
        logging.error(f"Failed to retry operations: {e}")
        return func.HttpResponse(
            body=json.dumps({"error": str(e)}),
            status_code=500,
            headers={"Content-Type": "application/json"}
        )


# noinspection PyUnusedLocal
@bp.function_name(name="tvmaze_api_status")
@bp.route(route="tvmaze_api_status", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
def tvmaze_api_status(req: func.HttpRequest) -> func.HttpResponse:
    """Get current TVMaze API reliability status.
    
    Args:
        req: HTTP request object
        
    Returns:
        HTTP response with TVMaze API reliability status
    """
    try:
        season_service = SeasonService()
        
        # Get TVMaze API reliability status
        reliability_status = season_service.tvmaze_api.get_reliability_status()
        api_health = season_service.tvmaze_api.is_healthy()
        
        status_data = {
            "tvmaze_api": {
                "is_healthy": api_health,
                "reliability_status": reliability_status
            }
        }
        
        return func.HttpResponse(
            body=json.dumps(status_data, indent=2),
            status_code=200,
            headers={"Content-Type": "application/json"}
        )
        
    except Exception as e:
        logging.error(f"Failed to get TVMaze API status: {e}")
        return func.HttpResponse(
            body=json.dumps({"error": str(e)}),
            status_code=500,
            headers={"Content-Type": "application/json"}
        )