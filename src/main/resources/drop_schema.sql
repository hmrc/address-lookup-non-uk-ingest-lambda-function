DROP SCHEMA IF EXISTS __schema__ CASCADE;
DELETE FROM public.nonuk_address_lookup_status
WHERE host_schema like '__schema__-%';
