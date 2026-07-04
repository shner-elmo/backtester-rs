use serde::{Deserialize, Serialize};

/// One completed round trip: the full lifetime of a position from the fill
/// that opened it from flat to the fill that returned it to flat (or flipped
/// it). Intermediate rebalance fills are netted into the entry/exit averages
/// rather than reported as separate trades.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub symbol: String,
    /// `"long"` or `"short"`.
    pub direction: String,
    /// Volume-weighted average price of all fills that built the position.
    pub entry_price: f64,
    /// Volume-weighted average price of all fills that unwound it.
    pub exit_price: f64,
    pub entry_time: String,
    pub exit_time: String,
    /// Total (absolute) quantity that round-tripped.
    pub quantity: f64,
    /// Realized PnL over the lifetime, net of commissions.
    pub pnl: f64,
}

/// Mark-to-market portfolio equity at the end of one trading day.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EquityPoint {
    /// Trading date, `YYYY-MM-DD`.
    pub time: String,
    pub equity: f64,
}

/// A position still open when the backtest ended, marked at the last known
/// market price.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenPositionSummary {
    pub symbol: String,
    pub quantity: f64,
    pub avg_price: f64,
    pub last_price: f64,
    pub market_value: f64,
    pub unrealized_pnl: f64,
    /// PnL already realized by partial unwinds (and commissions paid) during
    /// this still-open position lifetime. Not part of any completed trade.
    pub realized_pnl: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestStats {
    pub trade_count: usize,
    pub win_rate: f64,
    /// Sum of realized trade PnL (net of commissions).
    pub total_pnl: f64,
    pub profit_factor: f64,
    /// Worst peak-to-trough decline of the daily mark-to-market equity curve.
    pub max_drawdown: f64,
    /// Annualized from daily equity-curve returns (√252 scaling).
    pub sharpe_ratio: f64,
}

pub fn compute_stats(trades: &[Trade], equity_curve: &[EquityPoint]) -> BacktestStats {
    let trade_count = trades.len();

    let total_pnl: f64 = trades.iter().map(|t| t.pnl).sum();
    let wins = trades.iter().filter(|t| t.pnl > 0.0).count();
    let win_rate = if trade_count == 0 { 0.0 } else { wins as f64 / trade_count as f64 };

    let gross_profit: f64 = trades.iter().filter(|t| t.pnl > 0.0).map(|t| t.pnl).sum();
    let gross_loss: f64 = trades.iter().filter(|t| t.pnl < 0.0).map(|t| t.pnl.abs()).sum();
    let profit_factor = if gross_loss == 0.0 {
        if gross_profit > 0.0 {
            f64::INFINITY
        } else {
            0.0
        }
    } else {
        gross_profit / gross_loss
    };

    // Drawdown over the mark-to-market curve, so it captures pain from open
    // positions, not just realized losses.
    let mut peak = f64::NEG_INFINITY;
    let mut max_drawdown: f64 = 0.0;
    for p in equity_curve {
        if p.equity > peak {
            peak = p.equity;
        }
        if peak > 0.0 {
            let dd = (peak - p.equity) / peak;
            if dd > max_drawdown {
                max_drawdown = dd;
            }
        }
    }

    // Sharpe from daily returns of the equity curve (risk-free rate = 0).
    let returns: Vec<f64> = equity_curve
        .windows(2)
        .filter(|w| w[0].equity > 0.0)
        .map(|w| w[1].equity / w[0].equity - 1.0)
        .collect();
    let sharpe_ratio = if returns.len() < 2 {
        0.0
    } else {
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let var = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / returns.len() as f64;
        let std = var.sqrt();
        if std == 0.0 {
            0.0
        } else {
            mean / std * 252.0_f64.sqrt()
        }
    };

    BacktestStats { trade_count, win_rate, total_pnl, profit_factor, max_drawdown, sharpe_ratio }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trade(pnl: f64) -> Trade {
        Trade {
            symbol: "AAPL".into(),
            direction: "long".into(),
            entry_price: 100.0,
            exit_price: 100.0,
            entry_time: "2023-01-03T14:30:00+00:00".into(),
            exit_time: "2023-01-04T14:30:00+00:00".into(),
            quantity: 10.0,
            pnl,
        }
    }

    fn curve(points: &[f64]) -> Vec<EquityPoint> {
        points
            .iter()
            .enumerate()
            .map(|(i, &e)| EquityPoint { time: format!("2023-01-{:02}", i + 1), equity: e })
            .collect()
    }

    #[test]
    fn empty_inputs_produce_zeroed_stats() {
        let s = compute_stats(&[], &[]);
        assert_eq!(s.trade_count, 0);
        assert_eq!(s.total_pnl, 0.0);
        assert_eq!(s.max_drawdown, 0.0);
        assert_eq!(s.sharpe_ratio, 0.0);
    }

    #[test]
    fn win_rate_and_profit_factor() {
        let trades = vec![trade(100.0), trade(-50.0), trade(300.0), trade(-100.0)];
        let s = compute_stats(&trades, &[]);
        assert_eq!(s.trade_count, 4);
        assert_eq!(s.win_rate, 0.5);
        assert_eq!(s.total_pnl, 250.0);
        assert!((s.profit_factor - 400.0 / 150.0).abs() < 1e-12);
    }

    #[test]
    fn drawdown_comes_from_the_equity_curve() {
        // Peak 110, trough 88 -> 20% drawdown, even with no losing trades.
        let s = compute_stats(&[trade(10.0)], &curve(&[100.0, 110.0, 88.0, 120.0]));
        assert!((s.max_drawdown - 0.2).abs() < 1e-12);
    }

    #[test]
    fn flat_curve_has_zero_sharpe_and_drawdown() {
        let s = compute_stats(&[], &curve(&[100.0, 100.0, 100.0]));
        assert_eq!(s.max_drawdown, 0.0);
        assert_eq!(s.sharpe_ratio, 0.0);
    }
}
