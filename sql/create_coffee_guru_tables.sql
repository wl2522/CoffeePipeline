-- Create the tables that store the Coffee Guru app logs
CREATE TABLE IF NOT EXISTS raw_logs (
    "Timestamp" INT PRIMARY KEY,
    "Date" VARCHAR(16),
    "Time" TIMESTAMP,
    Latitude FLOAT,
    Longitude FLOAT,
    Method VARCHAR(32),
    Recipe VARCHAR(32),
    Coffee VARCHAR(16),
    "Score (out of 5)" INT,
    Bean VARCHAR(32),
    Grinder VARCHAR(32),
    Grind INT,
    Flavor VARCHAR(32),
    Balance VARCHAR(32)
 );

CREATE TABLE IF NOT EXISTS coffee_logs (
    brew_date INT PRIMARY KEY,
    recipe VARCHAR(32),
    coffee_grams INT,
    score INT,
    bean VARCHAR(32),
    grinder VARCHAR(32),
    grind INT,
    flavor VARCHAR(32),
    balance VARCHAR(32)
);
