with t as (select 
	distinct on (stg.product_id)
	cast(stg.product_id as integer),
    stg.brand,
    stg.product_line,
    stg.product_class,
    stg.product_size,
    to_real_or_return_null(stg.list_price) as list_price,
    to_real_or_return_null(stg.standard_cost) as standard_cost,
    stg.product_first_sold_date 
    FROM STG_TRANSACTIONS stg)
merge into DWH_PRODUCTS p
using t on t.product_id = p.product_id 
when matched then update set brand = t.brand, product_line = t.product_line, product_class = t.product_class, product_size = t.product_size, list_price = t.list_price, standard_cost = t.standard_cost, product_first_sold_date = t.product_first_sold_date
when not matched then insert (product_id, brand, product_line, product_class, product_size, list_price, standard_cost, product_first_sold_date) values (t.product_id, t.brand, t.product_line, t.product_class, t.product_size, t.list_price, t.standard_cost, t.product_first_sold_date);
