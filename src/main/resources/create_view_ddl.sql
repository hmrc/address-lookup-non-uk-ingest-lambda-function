DROP MATERIALIZED VIEW IF EXISTS __schema__.__table__;
CREATE MATERIALIZED VIEW __schema__.__table__
AS
SELECT rt.id                     AS uid,
       NULLIF(replace(btrim((rt.data::json -> 'properties' ->> 'id')::text), '""', ''), '')       AS id,
       NULLIF(replace(btrim((rt.data::json -> 'properties' ->> 'hash')::text), '""', ''), '')     AS hash,
       NULLIF(replace(btrim((rt.data::json -> 'properties' ->> 'number')::text), '""', ''), '')   AS number,
       NULLIF(replace(btrim((rt.data::json -> 'properties' ->> 'street')::text), '""', ''), '')   AS street,
       NULLIF(replace(btrim((rt.data::json -> 'properties' ->> 'unit')::text), '""', ''), '')     AS unit,
       NULLIF(replace(btrim((rt.data::json -> 'properties' ->> 'city')::text), '""', ''), '')     AS city,
       NULLIF(replace(btrim((rt.data::json -> 'properties' ->> 'district')::text), '""', ''), '') AS district,
       NULLIF(replace(btrim((rt.data::json -> 'properties' ->> 'region')::text), '""', ''), '')   AS region,
       NULLIF(replace(btrim((rt.data::json -> 'properties' ->> 'postcode')::text), '""', ''), '') AS postcode,
       to_tsvector('english'::regconfig, array_to_string(
               ARRAY [
                   NULLIF(replace(btrim((rt.data::json -> 'properties' ->> 'number')::text), '""', ''), ''),
                   NULLIF(replace(btrim((rt.data::json -> 'properties' ->> 'street')::text), '""', ''), ''),
                   NULLIF(replace(btrim((rt.data::json -> 'properties' ->> 'unit')::text), '""', ''), ''),
                   NULLIF(replace(btrim((rt.data::json -> 'properties' ->> 'city')::text), '""', ''), ''),
                   NULLIF(replace(btrim((rt.data::json -> 'properties' ->> 'district')::text), '""', ''), ''),
                   NULLIF(replace(btrim((rt.data::json -> 'properties' ->> 'region')::text), '""', ''), ''),
                   NULLIF(replace(btrim((rt.data::json -> 'properties' ->> 'postcode')::text), '""', ''), '')],
               ' '::text))       AS address_lookup_ft_col
FROM __schema__.raw___table__ rt;

-- CREATE INDEX IF NOT EXISTS address_lookup_ft_col_idx
--     ON __schema__.__table__ USING gin (address_lookup_ft_col);
