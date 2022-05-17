SELECT CONCAT(specific_schema, '.', routine_name)
FROM information_schema.routines
WHERE specific_catalog = 'addressbasepremium'
  AND specific_schema = 'public'
  AND routine_name LIKE 'create_nonuk_materialized_view_for_%';
