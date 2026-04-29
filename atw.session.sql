-- Valid SQL templates for ATW tables.
-- Edit values, then run statements independently.
-- 1) Daily market row
INSERT INTO bourse_daily (
        seance,
        instrument,
        ticker,
        ouverture,
        dernier_cours,
        plus_haut,
        plus_bas,
        nb_titres,
        volume,
        nb_transactions,
        capitalisation
    )
VALUES (
        DATE '2026-04-29',
        'ATW',
        'ATW',
        690.00,
        697.00,
        700.00,
        689.00,
        150000,
        104550000.00,
        1220,
        149950000000.00
    ) ON CONFLICT (seance) DO NOTHING;
-- 2) News row (id is BIGSERIAL; do not provide it)
INSERT INTO news (
        date,
        ticker,
        title,
        source,
        url,
        full_content,
        query_source,
        signal_score,
        is_atw_core,
        scraping_date
    )
VALUES (
        TIMESTAMPTZ '2026-04-29 11:45:00+01',
        'ATW',
        'ATW update',
        'Medias24',
        'https://example.com/atw-news-2026-04-29',
        'Short content here',
        'manual',
        72,
        1,
        NOW()
    ) ON CONFLICT (url) DO NOTHING;
-- 3) Orderbook snapshot row (single table for all days)
INSERT INTO bourse_orderbook (
        snapshot_ts,
        ticker,
        bid1_orders,
        bid2_orders,
        bid3_orders,
        bid4_orders,
        bid5_orders,
        bid1_qty,
        bid2_qty,
        bid3_qty,
        bid4_qty,
        bid5_qty,
        bid1_price,
        bid2_price,
        bid3_price,
        bid4_price,
        bid5_price,
        ask1_price,
        ask2_price,
        ask3_price,
        ask4_price,
        ask5_price,
        ask1_qty,
        ask2_qty,
        ask3_qty,
        ask4_qty,
        ask5_qty,
        ask1_orders,
        ask2_orders,
        ask3_orders,
        ask4_orders,
        ask5_orders
    ) ON CONFLICT (snapshot_ts) DO NOTHING;