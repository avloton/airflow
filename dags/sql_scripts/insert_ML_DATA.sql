insert into ml_data (
        online_order,
        gender,
        past_3_years_bike_related_purchases,
        age,
        job_industry_category,
        wealth_segment,
        owns_car
    )
select *
from (
        select case
                when dt.online_order = true then 1
                when dt.online_order = false then 0
            end as online_order,
            case
                when dc.gender like 'F%' then 0
                when dc.gender like 'M%' then 1
            end as gender,
            dc.past_3_years_bike_related_purchases::integer,
            now()::date - dc.dob as age,
            dc.job_industry_category,
            dc.wealth_segment,
            case
                when dc.owns_car = 'Yes' then 1
                when dc.owns_car = 'No' then 0
            end as owns_car
        from dwh_transactions dt
            inner join dwh_customers dc on dc.customer_id = dt.customer_id
    ) t
where t.online_order is not null
    and t.gender is not null;