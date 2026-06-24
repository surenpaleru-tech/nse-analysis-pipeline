"""
Risk Metrics Calculator — computes Sharpe, Sortino, Calmar, drawdown, etc.

Calculates comprehensive risk metrics for each CE/PE selling combination.
"""

from dataclasses import dataclass
from typing import Optional

import numpy as np


@dataclass
class RiskMetrics:
    """Complete set of risk metrics for a strategy."""
    win_rate: float
    avg_profit: float
    avg_loss: float
    expected_value: float
    max_drawdown: float
    profit_factor: float
    sharpe_ratio: float
    sortino_ratio: float
    calmar_ratio: float
    kelly_criterion: float
    prob_ce_worthless: float
    prob_pe_worthless: float
    combined_prob_worthless: float
    total_trades: int


def calculate_risk_metrics(
    pnl_series: list[float],
    ce_worthless: list[bool],
    pe_worthless: list[bool],
    risk_free_rate: float = 0.065,  # ~6.5% annual (Indian risk-free rate)
    annualization_factor: float = 52,  # Weekly for weekly expiries, 12 for monthly
) -> Optional[RiskMetrics]:
    """
    Calculate comprehensive risk metrics from a P&L series.

    Args:
        pnl_series: List of P&L values (positive = profit)
        ce_worthless: List of booleans indicating if CE expired worthless
        pe_worthless: List of booleans indicating if PE expired worthless
        risk_free_rate: Annual risk-free rate
        annualization_factor: Factor to annualize returns (52 for weekly, 12 for monthly)

    Returns:
        RiskMetrics dataclass or None if insufficient data
    """
    if len(pnl_series) < 5:
        return None

    pnl = np.array(pnl_series, dtype=np.float64)
    total_trades = len(pnl)

    # Win/Loss classification
    wins = pnl[pnl > 0]
    losses = pnl[pnl <= 0]

    win_rate = len(wins) / total_trades if total_trades > 0 else 0
    avg_profit = float(np.mean(wins)) if len(wins) > 0 else 0.0
    avg_loss = float(np.mean(losses)) if len(losses) > 0 else 0.0

    # Expected Value
    expected_value = float(np.mean(pnl))

    # Maximum Drawdown
    cumulative = np.cumsum(pnl)
    running_max = np.maximum.accumulate(cumulative)
    drawdowns = running_max - cumulative
    max_drawdown = float(np.max(drawdowns)) if len(drawdowns) > 0 else 0.0

    # Profit Factor
    gross_profit = float(np.sum(wins)) if len(wins) > 0 else 0.0
    gross_loss = float(np.abs(np.sum(losses))) if len(losses) > 0 else 0.001
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    # Sharpe Ratio (annualized)
    mean_return = float(np.mean(pnl))
    std_return = float(np.std(pnl, ddof=1)) if len(pnl) > 1 else 0.001
    periodic_rf = risk_free_rate / annualization_factor
    sharpe_ratio = (
        (mean_return - periodic_rf) / std_return * np.sqrt(annualization_factor)
        if std_return > 0 else 0.0
    )

    # Sortino Ratio (uses downside deviation)
    downside_returns = pnl[pnl < periodic_rf] - periodic_rf
    downside_std = (
        float(np.sqrt(np.mean(downside_returns ** 2)))
        if len(downside_returns) > 0 else 0.001
    )
    sortino_ratio = (
        (mean_return - periodic_rf) / downside_std * np.sqrt(annualization_factor)
        if downside_std > 0 else 0.0
    )

    # Calmar Ratio
    annualized_return = mean_return * annualization_factor
    calmar_ratio = (
        annualized_return / max_drawdown if max_drawdown > 0 else 0.0
    )

    # Kelly Criterion
    if avg_loss != 0 and win_rate > 0:
        b = abs(avg_profit / avg_loss) if avg_loss != 0 else 1
        kelly_criterion = win_rate - (1 - win_rate) / b
    else:
        kelly_criterion = 0.0

    # Probability of expiring worthless
    ce_w = np.array(ce_worthless)
    pe_w = np.array(pe_worthless)
    prob_ce = float(np.mean(ce_w)) if len(ce_w) > 0 else 0.0
    prob_pe = float(np.mean(pe_w)) if len(pe_w) > 0 else 0.0
    both_worthless = np.array(ce_worthless) & np.array(pe_worthless)
    combined_prob = float(np.mean(both_worthless)) if len(both_worthless) > 0 else 0.0

    return RiskMetrics(
        win_rate=round(win_rate, 4),
        avg_profit=round(avg_profit, 2),
        avg_loss=round(avg_loss, 2),
        expected_value=round(expected_value, 2),
        max_drawdown=round(max_drawdown, 2),
        profit_factor=round(min(profit_factor, 999.99), 4),
        sharpe_ratio=round(sharpe_ratio, 4),
        sortino_ratio=round(sortino_ratio, 4),
        calmar_ratio=round(calmar_ratio, 4),
        kelly_criterion=round(kelly_criterion, 4),
        prob_ce_worthless=round(prob_ce, 4),
        prob_pe_worthless=round(prob_pe, 4),
        combined_prob_worthless=round(combined_prob, 4),
        total_trades=total_trades,
    )
