CREATE OR REPLACE VIEW stockmarket_live_db.enriched_market_data AS

WITH latest_quotes AS (
    SELECT *
    FROM (
        SELECT
            q.*,
            ROW_NUMBER() OVER (
                PARTITION BY symbol
                ORDER BY collected_at DESC
            ) AS rn
        FROM stockmarket_live_db.quote_snapshots q
    )
    WHERE rn = 1
)

SELECT
    g.symbol,

    p.company_name,
    p.industry,
    p.exchange,
    p.country,

    g.window_start,
    g.window_end,
    g.open,
    g.high,
    g.low,
    g.close,
    g.volume,
    g.trade_count,
    g.vwap,

    q.current_price,
    q.day_open,
    q.day_high,
    q.day_low,
    q.previous_close,
    q.percent_change,
    q.collected_at AS quote_collected_at,

    f.pe_ttm,
    f.eps_ttm,
    f.eps_growth_ttm_yoy,
    f.revenue_growth_ttm_yoy,
    f.gross_margin_ttm,
    f.net_profit_margin_ttm,
    f.roe_ttm,
    f.current_ratio,
    f.debt_to_equity,
    f.beta,
    f.week_52_high,
    f.week_52_low

FROM stockmarket_live_db.liveohlcv_1min g

LEFT JOIN stockmarket_live_db.company_profiles p
    ON g.symbol = p.symbol

LEFT JOIN stockmarket_live_db.basic_financials f
    ON g.symbol = f.symbol

LEFT JOIN latest_quotes q
    ON g.symbol = q.symbol;