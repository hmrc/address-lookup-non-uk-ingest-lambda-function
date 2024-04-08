DROP TABLE IF EXISTS __schema__.expanded_raw___table__ CASCADE;
CREATE TABLE __schema__.expanded_raw___table__
(
    uid          integer not null,
    ingested_at timestamp default CURRENT_TIMESTAMP,
    id text,
    number text,
    street text,
    unit text,
    city text,
    district text,
    region text,
    postcode text,
    hash text,
    cip_hash text
);
