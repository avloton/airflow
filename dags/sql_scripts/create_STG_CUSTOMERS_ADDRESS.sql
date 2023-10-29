DROP TABLE if exists STG_CUSTOMERS_ADDRESS;
CREATE TABLE if not exists STG_CUSTOMERS_ADDRESS(
customer_id text,
address text,
postcode text,
state text,
country text,
property_valuation text);