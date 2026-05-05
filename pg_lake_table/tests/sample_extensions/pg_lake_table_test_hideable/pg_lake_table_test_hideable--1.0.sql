CREATE SCHEMA test_hideable_nsp;

CREATE FUNCTION test_hideable_nsp.test_func()
 RETURNS int
 LANGUAGE sql
AS $$ SELECT 1 $$;
