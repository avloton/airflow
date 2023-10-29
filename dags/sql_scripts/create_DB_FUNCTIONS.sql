create or replace function update_update_dt() returns trigger as $$
begin
	new.update_dt := current_timestamp;
	return new;
end;
$$ language plpgsql;

create or replace function update_create_dt_update_dt() returns trigger as $$
begin
	new.update_dt := current_timestamp;
	new.create_dt := current_timestamp;
	return new;
end;
$$ language plpgsql;

create or replace function to_date_or_return_null(str varchar)
returns date language plpgsql as $$
begin
    return to_date(str, 'YYYY-MM-DD');
exception
    when others then return null;
end $$;

CREATE OR REPLACE FUNCTION to_integer_or_return_null(str character varying)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
begin
    return str::numeric::integer;
exception
    when others then return null;
end $function$;

CREATE OR REPLACE FUNCTION to_real_or_return_null(str character varying)
 RETURNS real
 LANGUAGE plpgsql
AS $function$
begin
	if str = 'nan' then return null;
	end if;
    return cast(str as real);
exception
    when others then return null;
end $function$;