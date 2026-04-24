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
        'seance:date',
        'instrument:text',
        'ticker:text',
        ouverture :numeric,
        INSERT INTO news (
            id,
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
                'id:bigint',
                'date:timestamp with time zone',
                'ticker:text',
                'title:text',
                'source:text',
                'url:text',
                'full_content:text',
                'query_source:text',
                signal_score :integer,
                'is_atw_core:smallint',
                'scraping_date:timestamp with time zone'
            );
dernier_cours :numeric,
plus_haut :numeric,
plus_bas :numeric,
nb_titres :numeric,
volume :numeric,
nb_transactions :integer,
capitalisation :numeric
);