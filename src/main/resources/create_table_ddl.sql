DROP MATERIALIZED VIEW IF EXISTS __schema__.__table__;
CREATE MATERIALIZED VIEW __schema__.__table__
AS
SELECT rt.id                     AS uid,
       rt.data::json -> 'properties' -> 'id'       AS id,
       rt.data::json -> 'properties' -> 'hash'     AS hash,
       rt.data::json -> 'properties' -> 'number'   AS number,
       rt.data::json -> 'properties' -> 'street'   AS street,
       rt.data::json -> 'properties' -> 'unit'     AS unit,
       rt.data::json -> 'properties' -> 'city'     AS city,
       rt.data::json -> 'properties' -> 'district' AS district,
       rt.data::json -> 'properties' -> 'region'   AS region,
       rt.data::json -> 'properties' -> 'postcode' AS postcode,
       to_tsvector('english'::regconfig, array_to_string(
               ARRAY [
                   NULLIF(btrim((rt.data::json -> 'properties' -> 'street')::text), ''),
                   NULLIF(btrim((rt.data::json -> 'properties' -> 'unit')::text), ''),
                   NULLIF(btrim((rt.data::json -> 'properties' -> 'city')::text), ''),
                   NULLIF(btrim((rt.data::json -> 'properties' -> 'district')::text), ''),
                   NULLIF(btrim((rt.data::json -> 'properties' -> 'region')::text), ''),
                   NULLIF(btrim((rt.data::json -> 'properties' -> 'postcode')::text), '')],
               ' '::text))       AS address_lookup_ft_col
FROM __schema__.raw___table__ rt;

CREATE INDEX IF NOT EXISTS address_lookup_ft_col_idx
    ON __schema__.__table__ USING gin (address_lookup_ft_col);
