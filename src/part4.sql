--1
CREATE
OR REPLACE PROCEDURE destroy_tables() AS $ $ DECLARE row record;
BEGIN FOR row IN
SELECT
  quote_ident(table_name)
FROM
  information_schema.tables
WHERE
  table_name LIKE 'TableName' || '%'
  AND table_schema LIKE 'public' LOOP EXECUTE 'DROP TABLE ' || row.table_name || ' CASCADE ';
END LOOP;
END $ $ LANGUAGE plpgsql;
-- Тестовая транзакция.
CALL destroy_tables();
--2
CREATE
OR REPLACE PROCEDURE get_count(OUT func BIGINT) AS $ $ BEGIN
SELECT
  COUNT(DISTINCT routines.routine_name)
FROM
  information_schema.routines
  LEFT JOIN information_schema.parameters ON routines.specific_name = parameters.specific_name
WHERE
  routines.specific_schema = 'public'
  AND parameters.data_type IS NOT NULL INTO func;
END;
$ $ LANGUAGE plpgsql;
CALL get_count(NULL);
--3
CREATE
OR REPLACE PROCEDURE del_triggers(INOUT result INT) AS $ $ DECLARE row record;
BEGIN FOR row IN
SELECT
  trigger_name || ' ON ' || event_object_table AS tr
FROM
  information_schema.triggers
WHERE
  trigger_schema = 'public' LOOP EXECUTE 'DROP TRIGGER ' || row.tr;
result := result + 1;
END LOOP;
END $ $ LANGUAGE plpgsql;
-- Тестовая транзакция.
CALL del_triggers(0);
--4
CREATE
OR REPLACE FUNCTION get_func(IN find_str VARCHAR) RETURNS TABLE (
  routine_name VARCHAR,
  routine_type VARCHAR,
  routine_definition VARCHAR
) AS $ $ BEGIN RETURN QUERY
SELECT
  r.routine_name :: VARCHAR,
  r.routine_type :: VARCHAR,
  r.routine_definition :: VARCHAR
FROM
  information_schema.routines r
WHERE
  r.specific_schema = 'public'
  AND r.routine_definition LIKE '%' || find_str || '%';
END $ $ LANGUAGE plpgsql;
SELECT
  *
FROM
  get_func('em');