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
    grind INT,
    flavor VARCHAR(32),
    balance VARCHAR(32)
);

-- -- Create the tables that store the Beanconqueror app logs
CREATE TABLE IF NOT EXISTS beanconqueror_beans (
    name VARCHAR(64),
    roaster VARCHAR(64),
    uuid VARCHAR(64) PRIMARY KEY,
    roast VARCHAR(32),
    bean_mix VARCHAR(32),
    decaffeinated INTEGER,
    bean_roasting_type VARCHAR(32),
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS beanconqueror_brews (
    uuid VARCHAR(64) PRIMARY KEY,
    grind_size INTEGER,
    grind_weight INTEGER,
    method_of_preparation INTEGER,
    mill INTEGER,
    bean VARCHAR(64),
    brew_temperature INTEGER,
    brew_time INTEGER,
    note TEXT,
    rating INTEGER,
    coffee_first_drip_time INTEGER,
    coffee_blooming_time INTEGER,
    brew_beverage_quantity INTEGER,
    brew_beverage_quantity_type VARCHAR(16),
    method_of_preparation_tools TEXT,
    favourite INTEGER,
    best_brew INTEGER,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS beanconqueror_grinders (
    name VARCHAR(64),
    uuid VARCHAR(64) PRIMARY KEY,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS beanconqueror_methods (
    name VARCHAR(64),
    uuid VARCHAR(64) PRIMARY KEY,
    type VARCHAR(32),
    style_type VARCHAR(32),
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS beanconqueror_method_tools (
    name VARCHAR(64),
    uuid VARCHAR(64) PRIMARY KEY,
    preparation_method_name VARCHAR(64),
    updated_at TIMESTAMP
);

