CREATE TABLE IF NOT EXISTS unemployment_rate(
    year INT NOT NULL
    month INT NOT NULL
    unemployment_rate FLOAT NOT NULL
    PRIMARY KEY (year, month)
);