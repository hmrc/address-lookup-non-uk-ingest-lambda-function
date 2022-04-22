CREATE TABLE IF NOT EXISTS public.nonuk_address_lookup_status (
    host_schema     VARCHAR NOT NULL PRIMARY KEY,
    status          VARCHAR NOT NULL,
    error_message   VARCHAR NULL,
    timestamp       TIMESTAMP NOT NULL
    );
GRANT SELECT ON public.nonuk_address_lookup_status TO addresslookupreader;
