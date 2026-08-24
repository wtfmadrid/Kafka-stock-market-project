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

    g.window_start,
    g.close,
    g.vwap,
    g.volume,

    q.current_price,
    q.day_open,
    q.day_high,
    q.day_low,
    q.previous_close,
    q.percent_change,

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

LEFT JOIN latest_quotes q
    ON g.symbol = q.symbol

ORDER BY g.window_start DESC, g.symbol
LIMIT 50;