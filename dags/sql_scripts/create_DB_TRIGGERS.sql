create or replace trigger trigger1_dwh_customers before
insert on dwh_customers for each row execute function update_create_dt_update_dt();

create or replace trigger trigger2_dwh_customers before
update on dwh_customers for each row execute function update_update_dt();

create or replace trigger trigger1_dwh_products before
insert on dwh_products for each row execute function update_create_dt_update_dt();

create or replace trigger trigger2_dwh_products before
update on dwh_products for each row execute function update_update_dt();

create or replace trigger trigger1_dwh_transactions before
insert on dwh_transactions for each row execute function update_create_dt_update_dt();

create or replace trigger trigger2_dwh_transactions before
update on dwh_transactions for each row execute function update_update_dt();