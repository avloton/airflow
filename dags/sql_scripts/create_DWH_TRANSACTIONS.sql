CREATE TABLE if not exists DWH_TRANSACTIONS(
    transation_id integer primary key,
    product_id integer,
    customer_id integer,
    transaction_date date,
    online_order boolean,
    order_status text,
    create_dt timestamp,
    update_dt timestamp
);