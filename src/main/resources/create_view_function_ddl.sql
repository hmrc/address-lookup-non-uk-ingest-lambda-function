-- No need to drop first as the schema should be new and empty
-- DROP MATERIALIZED VIEW IF EXISTS __schema__.__table__;
CREATE OR REPLACE FUNCTION create_nonuk_materialized_view_for___table__()
    RETURNS BOOLEAN
    LANGUAGE plpgsql
AS
$$
DECLARE
    schema_id varchar := format('%I-%I', '__schema__', '__table__');
    raw_table_name varchar := format('raw_%I', '__table__');
BEGIN
EXECUTE format('SET search_path = %s', '__schema__' );

INSERT INTO public.nonuk_address_lookup_status(host_schema, status, timestamp)
VALUES(schema_id, 'materialized_view: started', now());

CREATE MATERIALIZED VIEW __schema__.__table__
AS
SELECT rt.id                                                                                      AS uid,
       NULLIF(REPLACE(BTRIM((rt.data::json -> 'properties' ->> 'id')::text), '""', ''), '')       AS id,
       NULLIF(REPLACE(BTRIM((rt.data::json -> 'properties' ->> 'hash')::text), '""', ''), '')     AS hash,
       NULLIF(REPLACE(BTRIM((rt.data::json -> 'properties' ->> 'number')::text), '""', ''), '')   AS number,
       NULLIF(REPLACE(BTRIM((rt.data::json -> 'properties' ->> 'street')::text), '""', ''), '')   AS street,
       NULLIF(REPLACE(BTRIM((rt.data::json -> 'properties' ->> 'unit')::text), '""', ''), '')     AS unit,
       NULLIF(REPLACE(BTRIM((rt.data::json -> 'properties' ->> 'city')::text), '""', ''), '')     AS city,
       NULLIF(REPLACE(BTRIM((rt.data::json -> 'properties' ->> 'district')::text), '""', ''), '') AS district,
       NULLIF(REPLACE(BTRIM((rt.data::json -> 'properties' ->> 'region')::text), '""', ''), '')   AS region,
       NULLIF(REPLACE(BTRIM((rt.data::json -> 'properties' ->> 'postcode')::text), '""', ''), '') AS postcode,
       to_tsvector('english'::regconfig, array_to_string(
           ARRAY [
               NULLIF(REPLACE(BTRIM((rt.data::json -> 'properties' ->> 'number')::text), '""', ''), ''),
               NULLIF(REPLACE(BTRIM((rt.data::json -> 'properties' ->> 'street')::text), '""', ''), ''),
               NULLIF(REPLACE(BTRIM((rt.data::json -> 'properties' ->> 'unit')::text), '""', ''), ''),
               NULLIF(REPLACE(BTRIM((rt.data::json -> 'properties' ->> 'city')::text), '""', ''), ''),
               NULLIF(REPLACE(BTRIM((rt.data::json -> 'properties' ->> 'district')::text), '""', ''), ''),
               NULLIF(REPLACE(BTRIM((rt.data::json -> 'properties' ->> 'region')::text), '""', ''), ''),
               NULLIF(REPLACE(BTRIM((rt.data::json -> 'properties' ->> 'postcode')::text), '""', ''), '')],
           ' '::text))                                                                                    AS address_lookup_ft_col
FROM __schema__.raw___table__ rt;

UPDATE public.nonuk_address_lookup_status
SET status    = 'materialized_view: created',
    timestamp = now()
WHERE host_schema = schema_id;

CREATE INDEX IF NOT EXISTS address_lookup___table___ft_col_idx
 ON __schema__.__table__ USING gin (address_lookup_ft_col);

UPDATE public.nonuk_address_lookup_status
SET status    = 'materialized_view: index_created',
    timestamp = now()
WHERE host_schema = schema_id;

DROP VIEW IF EXISTS public.__table__;
CREATE VIEW public.__table__
AS
SELECT uid,
       id,
       hash,
       number,
       street,
       unit,
       city,
       district,
       region,
       postcode,
       address_lookup_ft_col
FROM __schema__.__table__;
GRANT SELECT ON public.__table__ TO addresslookupreader;

UPDATE public.nonuk_address_lookup_status
SET status    = 'materialized_view: public_alias_created',
    timestamp = now()
WHERE host_schema = schema_id;

RETURN TRUE;
EXCEPTION
    WHEN OTHERS THEN
UPDATE public.nonuk_address_lookup_status
SET status    = 'materialized_view: errored',
    error_message = SQLERRM,
    timestamp = now()
WHERE host_schema = schema_id;
END;
$$
