INSERT OR REPLACE INTO coffee_logs
    SELECT DATETIME("Timestamp", 'unixepoch') AS brew_date,
        Recipe AS recipe,
        Coffee AS coffee_grams,
        "Score (out of 5)" AS score,
        Bean AS bean,
        Grinder AS grinder,
        Grind AS grind,
        Flavor AS flavor,
        Balance AS balance
    FROM raw_logs
    ORDER BY Timestamp DESC;
