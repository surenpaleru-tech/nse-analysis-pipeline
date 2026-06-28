"""
ORM Models — exports all models so they are registered with Base.metadata.
Import this package to ensure all tables are visible to Alembic.
"""

from app.models.spot_price import SpotPrice
from app.models.option_chain import OptionChain
from app.models.futures_chain import FuturesChain
from app.models.expiry import Expiry
from app.models.india_vix import IndiaVIX
from app.models.fno_universe import FnOUniverse
from app.models.optimal_band import OptimalSellingBand
from app.models.daily_recommendation import DailyRecommendation
from app.models.strategy_result import StrategyResult
from app.models.user import User
from app.models.chat_history import ChatHistory
from app.models.alert import Alert

__all__ = [
    "SpotPrice",
    "OptionChain",
    "FuturesChain",
    "Expiry",
    "IndiaVIX",
    "FnOUniverse",
    "OptimalSellingBand",
    "DailyRecommendation",
    "StrategyResult",
    "User",
    "ChatHistory",
    "Alert",
]
