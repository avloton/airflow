INSERT INTO DWH_TRANSACTIONS (
        transation_id,
        product_id,
        customer_id,
        transaction_date,
        online_order,
        order_status
    )
SELECT cast(transaction_id as integer),
    cast(product_id as integer),
    cast(customer_id as integer),
    to_date_or_return_null(transaction_date) as transaction_date,
    to_integer_or_return_null(online_order) as online_order,
    order_status
FROM stg_transactions on conflict do nothing;