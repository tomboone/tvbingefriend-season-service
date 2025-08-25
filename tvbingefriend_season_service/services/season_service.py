"""Service for TV season-related operations."""
import logging
from datetime import datetime, UTC
from typing import Any
import uuid

import azure.functions as func
from tvbingefriend_azure_storage_service import StorageService  # type: ignore
from tvbingefriend_tvmaze_client import TVMazeAPI  # type: ignore

from tvbingefriend_season_service.config import (
    STORAGE_CONNECTION_STRING,
    SEASONS_QUEUE,
    SHOW_IDS_TABLE
)
from tvbingefriend_season_service.repos.season_repo import SeasonRepository
from tvbingefriend_season_service.utils import db_session_manager
from tvbingefriend_season_service.services.monitoring_service import MonitoringService, ImportStatus
from tvbingefriend_season_service.services.retry_service import RetryService


# noinspection PyMethodMayBeStatic
class SeasonService:
    """Service for TV season-related operations."""
    def __init__(self, 
                 season_repository: SeasonRepository | None = None,
                 monitoring_service: MonitoringService | None = None,
                 retry_service: RetryService | None = None) -> None:
        self.season_repository = season_repository or SeasonRepository()
        self.storage_service = StorageService(STORAGE_CONNECTION_STRING)
        
        # Use TVMaze client
        self.tvmaze_api = TVMazeAPI()
        
        # Initialize monitoring services
        self.monitoring_service = monitoring_service or MonitoringService()
        self.retry_service = retry_service or RetryService()
        
        # Current bulk import ID for tracking
        self.current_import_id: str | None = None

    def start_get_all_shows_seasons(self) -> str:
        """Start getting all seasons for all shows from the SHOW_IDS_TABLE.
        
        Returns:
            Import ID for tracking progress
        """
        # Generate unique import ID
        import_id = f"seasons_import_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
        self.current_import_id = import_id
        
        logging.info(
            f"SeasonService.start_get_all_shows_seasons: Starting seasons retrieval with import ID: {import_id}"
        )

        try:
            # Get all show IDs from the SHOW_IDS_TABLE
            show_entities = self.storage_service.get_entities(
                table_name=SHOW_IDS_TABLE,
                filter_query="PartitionKey eq 'show'"
            )
            
            if not show_entities:
                logging.warning("No show IDs found in SHOW_IDS_TABLE")
                return import_id
            
            # Start tracking this bulk import
            self.monitoring_service.start_show_seasons_import_tracking(
                import_id=import_id,
                show_id=-1,  # Placeholder for bulk operation
                estimated_seasons=len(show_entities)  # Estimate one request per show
            )
            
            # Queue each show ID for season processing
            for show_entity in show_entities:
                show_id = int(show_entity.get("RowKey"))
                show_message: dict[str, Any] = {
                    "show_id": show_id,
                    "import_id": import_id
                }
                
                self.storage_service.upload_queue_message(
                    queue_name=SEASONS_QUEUE,
                    message=show_message
                )
            
            logging.info(
                f"SeasonService.start_get_all_shows_seasons: Queued {len(show_entities)} shows for season processing"
            )
            return import_id
            
        except Exception as e:
            logging.error(f"Failed to start season import: {e}")
            self.monitoring_service.complete_show_seasons_import(import_id, ImportStatus.FAILED)
            raise

    def get_show_seasons(self, season_msg: func.QueueMessage) -> None:
        """Get all seasons for a specific show from TV Maze.

        Args:
            season_msg (func.QueueMessage): Show ID message
        """
        logging.info("=== SeasonService.get_show_seasons ENTRY ===")
        
        # Handle message with retry logic
        def handle_show_seasons(message: func.QueueMessage) -> None:
            """Handle show seasons message."""
            logging.info("=== handle_show_seasons ENTRY ===")
            try:
                msg_data = message.get_json()
                logging.info(f"Message data in handle_show_seasons: {msg_data}")
                show_id: int | None = msg_data.get("show_id")
                import_id: str | None = msg_data.get("import_id")
                logging.info(f"Extracted show_id: {show_id}, import_id: {import_id}")

                if show_id is None:
                    logging.error("Queue message is missing 'show_id' number.")
                    return

                logging.info(f"SeasonService.get_show_seasons: Getting seasons from TV Maze for show_id: {show_id}")
            except Exception as err:
                logging.error(f"Error in handle_show_seasons setup: {err}", exc_info=True)
                raise

            try:
                logging.info(f"Calling TVMaze API for show {show_id} seasons...")
                # TVMaze API now has built-in rate limiting and retry logic
                seasons: list[dict[str, Any]] | None = self.tvmaze_api.get_seasons(show_id)
                logging.info(f"TVMaze API returned {len(seasons) if seasons else 0} seasons for show {show_id}")

                if seasons:
                    # Process seasons with database retry logic
                    success_count = 0
                    for season in seasons:
                        if not season or not isinstance(season, dict):
                            logging.error("SeasonService.upsert_season: Season not found.")
                            continue
                        
                        @self.retry_service.with_retry('database_write', max_attempts=3)
                        def upsert_with_retry():
                            """Upsert season into database."""
                            with db_session_manager() as db:
                                # Pass the show_id along with the season data
                                self.season_repository.upsert_season(season, show_id, db)
                        
                        try:
                            upsert_with_retry()
                            success_count += 1
                            
                            # Update progress tracking for each season
                            if import_id:
                                season_id = season.get('id')
                                if season_id:
                                    self.monitoring_service.update_season_import_progress(import_id, season_id)
                            
                        except Exception as err:
                            logging.error(
                                f"Failed to upsert season {season.get('id', 'unknown')} for show {show_id} "
                                f"after retries: {err}")
                            if import_id:
                                season_id = season.get('id')
                                if season_id:
                                    self.monitoring_service.update_season_import_progress(
                                        import_id, season_id, success=False
                                    )

                    logging.info(f"Successfully processed {success_count}/{len(seasons)} seasons for show {show_id}")
                else:
                    logging.info(f"No seasons returned for show {show_id}")

            except Exception as err:
                logging.error(f"Failed to get seasons for show {show_id}: {err}")
                raise

        # Process with retry logic
        logging.info("=== Calling retry_service.handle_queue_message_with_retry ===")
        try:
            self.retry_service.handle_queue_message_with_retry(
                message=season_msg,
                handler_func=handle_show_seasons,
                operation_type="show_seasons"
            )
            logging.info("=== retry_service.handle_queue_message_with_retry COMPLETED ===")
        except Exception as e:
            logging.error(f"=== ERROR in retry_service.handle_queue_message_with_retry: {e} ===", exc_info=True)
            raise

    def get_updates(self, since: str = "day"):
        """Get updates with rate limiting and monitoring.

        Args:
            since (str): Since parameter for TV Maze API. Defaults to "day".
        """
        logging.info(f"SeasonService.get_updates: Getting updates since {since}")
        
        try:
            # TVMaze API now has built-in rate limiting and retry logic
            updates: dict[str, Any] = self.tvmaze_api.get_show_updates(period=since)
            
            if not updates:
                logging.info("No updates found")
                return
            
            logging.info(f"Found {len(updates)} show updates")
            
            # Process updates
            success_count = 0
            for show_id, last_updated in updates.items():
                try:
                    seasons_queue_msg = {
                        "show_id": int(show_id),
                    }
                    
                    # Queue show ID to seasons queue for season processing
                    self.storage_service.upload_queue_message(
                        queue_name=SEASONS_QUEUE,
                        message=seasons_queue_msg
                    )
                    
                    success_count += 1
                    
                except Exception as e:
                    logging.error(f"Failed to process update for show {show_id}: {e}")
            
            logging.info(f"Successfully queued {success_count}/{len(updates)} show updates for season processing")
            
            # Update data health metrics
            self.monitoring_service.update_data_health(
                metric_name="updates_processed",
                value=success_count,
                threshold=len(updates) * 0.95  # Alert if less than 95% success rate
            )
            
        except Exception as e:
            logging.error(f"Failed to get show updates: {e}")
            self.monitoring_service.update_data_health(
                metric_name="updates_failed",
                value=1
            )
            raise

    def get_import_status(self, import_id: str) -> dict[str, Any]:
        """Get the status of a season import operation.
        
        Args:
            import_id: Import operation identifier
            
        Returns:
            Dictionary with import status information
        """
        return self.monitoring_service.get_import_status(import_id)
    
    def get_system_health(self) -> dict[str, Any]:
        """Get overall system health status.
        
        Returns:
            Dictionary with system health information
        """
        health_summary = self.monitoring_service.get_health_summary()
        
        # TVMaze API status (basic connectivity assumed)
        health_summary['tvmaze_api_healthy'] = True  # Assume healthy for standard client
        
        # Add data freshness check
        freshness_status = self.monitoring_service.check_data_freshness()
        health_summary['data_freshness'] = freshness_status
        
        return health_summary
    
    def retry_failed_operations(self, operation_type: str, max_age_hours: int = 24) -> dict[str, Any]:
        """Retry failed operations of a specific type.
        
        Args:
            operation_type: Type of operations to retry
            max_age_hours: Only retry failures within this many hours
            
        Returns:
            Summary of retry attempts
        """
        failed_operations = self.monitoring_service.get_failed_operations(operation_type, max_age_hours)
        
        retry_summary: dict[str, Any] = {
            'operation_type': operation_type,
            'found_failed_operations': len(failed_operations),
            'successful_retries': 0,
            'failed_retries': 0,
            'retry_attempts': []
        }
        
        for operation in failed_operations:
            try:
                success = self.retry_service.retry_failed_operation(operation_type, operation)
                if success:
                    retry_summary['successful_retries'] += 1
                else:
                    retry_summary['failed_retries'] += 1
                
                retry_summary['retry_attempts'].append({
                    'operation': operation,
                    'success': success
                })
                
            except Exception as e:
                logging.error(f"Failed to retry operation {operation}: {e}")
                retry_summary['failed_retries'] += 1
                retry_summary['retry_attempts'].append({
                    'operation': operation,
                    'success': False,
                    'error': str(e)
                })
        
        return retry_summary
