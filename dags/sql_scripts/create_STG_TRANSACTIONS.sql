DROP TABLE if exists STG_TRANSACTIONS;
CREATE TABLE if not exists STG_TRANSACTIONS(
transaction_id text,
product_id text,
customer_id text,
transaction_date text,
online_order text,
order_status text,
brand text,
product_line text,
product_class text,
product_size text,
list_price text,
standard_cost text,
product_first_sold_date text);