INSERT INTO DWH_CUSTOMERS
(
    customer_id,
    first_name,
    last_name,
    gender,
    past_3_years_bike_related_purchases,
    DOB,
    job_title,
    job_industry_category,
    wealth_segment,
    deceased_indicator,
    owns_car,
    tenure,
    address,
    postcode,
    state,
    country,
    property_valuation
)
SELECT
    cast(stgc.customer_id as integer),
    stgc.first_name,
    stgc.last_name,
    stgc.gender,
    stgc.past_3_years_bike_related_purchases,
    to_date_or_return_null(stgc.DOB) as DOB,
    stgc.job_title,
    stgc.job_industry_category,
    stgc.wealth_segment,
    stgc.default_x,
    stgc.owns_car,
    stgc.tenure,
    stga.address,
    stga.postcode,
    stga.state,
    stga.country,
    stga.property_valuation
FROM stg_customers stgc LEFT JOIN stg_customers_address stga ON stga.customer_id = stgc.customer_id
on conflict do nothing;
