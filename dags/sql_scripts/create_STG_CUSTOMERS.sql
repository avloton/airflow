DROP TABLE if exists STG_CUSTOMERS;
CREATE TABLE if not exists STG_CUSTOMERS(
    customer_id text,
    first_name text,
    last_name text,
    gender text,
    past_3_years_bike_related_purchases text,
    DOB text,
    job_title text,
    job_industry_category text,
    wealth_segment text,
    deceased_indicator text,
    default_x text,
    owns_car text,
    tenure text
);