"""Season service for TVBF"""
import azure.functions as func

from tvbingefriend_season_service.blueprints import bp_get_show_seasons
from tvbingefriend_season_service.blueprints import bp_health_monitoring
from tvbingefriend_season_service.blueprints import bp_start_get_all
from tvbingefriend_season_service.blueprints import bp_updates_manual
from tvbingefriend_season_service.blueprints import bp_updates_timer

app = func.FunctionApp()

app.register_blueprint(bp_get_show_seasons)
app.register_blueprint(bp_health_monitoring)
app.register_blueprint(bp_start_get_all)
app.register_blueprint(bp_updates_manual)
app.register_blueprint(bp_updates_timer)
