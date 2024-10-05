-- Create the database
CREATE DATABASE financial_data;

-- Table for stock data
CREATE TABLE stock_data (
    id SERIAL PRIMARY KEY,
    stock_symbol VARCHAR(10),
    date DATE,
    open_price NUMERIC,
    high_price NUMERIC,
    low_price NUMERIC,
    close_price NUMERIC,
    volume BIGINT
);

-- Insert sample data into stock_data
select * from stock_data;