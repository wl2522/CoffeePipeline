-- Create the tables that store the Beanconqueror app logs
CREATE TABLE IF NOT EXISTS beanconqueror_beans (
    name VARCHAR(32),
    roaster VARCHAR(32),
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
    method_of_preparation VARCHAR(32),
    mill INTEGER,
    bean VARCHAR(32),
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
    name VARCHAR(32),
    uuid VARCHAR(64) PRIMARY KEY,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS beanconqueror_methods (
    name VARCHAR(32),
    uuid VARCHAR(64) PRIMARY KEY,
    type VARCHAR(32),
    style_type VARCHAR(32),
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS beanconqueror_method_tools (
    name VARCHAR(32),
    uuid VARCHAR(64) PRIMARY KEY,
    preparation_method_name VARCHAR(32),
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS beanconqueror_logs (
    uuid VARCHAR(64) PRIMARY KEY,
    updated_at TIMESTAMP,
    grinder VARCHAR(32),
    grind_size INTEGER,
    grind_weight INTEGER,
    method VARCHAR(32),
    roaster VARCHAR(32),
    bean VARCHAR(32),
    brew_temperature INTEGER,
    brew_time INTEGER,
    rating INTEGER,
    flavor VARCHAR(32),
    balance VARCHAR(32),
    coffee_first_drip_time INTEGER,
    coffee_blooming_time INTEGER,
    brew_beverage_quantity INTEGER,
    brew_beverage_quantity_type VARCHAR(16),
    method_of_preparation_tools TEXT,
    favourite INTEGER,
    best_brew INTEGER
);
