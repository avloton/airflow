CREATE TABLE if not exists DWH_PRODUCTS(
product_id integer primary key,
brand text,
product_line text,
product_class text,
product_size text,
list_price real,
standard_cost real,
product_first_sold_date text,
create_dt timestamp,
update_dt timestamp
);
