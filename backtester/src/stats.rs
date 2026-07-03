use serde::Serialize;

#[derive(Serialize)]
pub struct Trade {
    pub symbol: String,
    pub entry_price: f64,
    pub exit_price: f64,
    pub entry_time: String,
    pub exit_time: String,
    pub quantity: f64,
    pub pnl: f64,
}

pub struct BacktestStats {
    pub trade_count: usize,
    pub win_rate: f64,
    pub total_pnl: f64,
    pub profit_factor: f64,
    pub max_drawdown: f64,
    pub sharpe_ratio: f64,
    pub final_equity: f64,
}

pub fn compute_stats(trades: &[Trade], initial_cash: f64) -> BacktestStats {
    let trade_count = trades.len();

    if trade_count == 0 {
        return BacktestStats {
            trade_count: 0,
            win_rate: 0.0,
            total_pnl: 0.0,
            profit_factor: 0.0,
            max_drawdown: 0.0,
            sharpe_ratio: 0.0,
            final_equity: initial_cash,
        };
    }

    let total_pnl: f64 = trades.iter().map(|t| t.pnl).sum();
    let wins = trades.iter().filter(|t| t.pnl > 0.0).count();
    let win_rate = wins as f64 / trade_count as f64;

    let gross_profit: f64 = trades.iter().filter(|t| t.pnl > 0.0).map(|t| t.pnl).sum();
    let gross_loss: f64 = trades.iter().filter(|t| t.pnl < 0.0).map(|t| t.pnl.abs()).sum();
    let profit_factor = if gross_loss == 0.0 { f64::INFINITY } else { gross_profit / gross_loss };

    let mut equity = initial_cash;
    let mut peak = equity;
    let mut max_drawdown: f64 = 0.0;
    let pnls: Vec<f64> = trades.iter().map(|t| t.pnl).collect();

    for &pnl in &pnls {
        equity += pnl;
        if equity > peak {
            peak = equity;
        }
        let dd = (peak - equity) / peak;
        if dd > max_drawdown {
            max_drawdown = dd;
        }
    }

    let final_equity = equity;

    let mean_pnl = pnls.iter().sum::<f64>() / pnls.len() as f64;
    let variance = pnls.iter().map(|p| (p - mean_pnl).powi(2)).sum::<f64>() / pnls.len() as f64;
    let std_pnl = variance.sqrt();
    let sharpe_ratio = if std_pnl == 0.0 { 0.0 } else { mean_pnl / std_pnl * 252.0_f64.sqrt() };

    BacktestStats {
        trade_count,
        win_rate,
        total_pnl,
        profit_factor,
        max_drawdown,
        sharpe_ratio,
        final_equity,
    }
}
