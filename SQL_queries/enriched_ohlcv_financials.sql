SELECT
    g.symbol,
    p.company_name,
    p.industry,
    g.window_start,
    g.open,
    g.high,
    g.low,
    g.close,
    g.volume,
    g.trade_count,
    g.vwap,
    f.pe_ttm,
    f.eps_ttm,
    f.revenue_growth_ttm_yoy,
    f.net_profit_margin_ttm,
    f.roe_ttm,
    f.beta
FROM stockmarket_live_db.liveohlcv_1min g
LEFT JOIN stockmarket_live_db.company_profiles p
    ON g.symbol = p.symbol
LEFT JOIN stockmarket_live_db.basic_financials f
    ON g.symbol = f.symbol
ORDER BY g.window_start DESC, g.symbol
LIMIT 50;