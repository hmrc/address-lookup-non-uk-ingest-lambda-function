SELECT  DISTINCT substr(host_schema, 0, position('-' in host_schema))
FROM    public.nonuk_address_lookup_status
WHERE   substr(host_schema, 0, position('-' in host_schema)) NOT IN (
    SELECT      substr(host_schema, 0, position('-' in host_schema))
    FROM        public.nonuk_address_lookup_status
    WHERE       status = 'finalised'
    ORDER BY    timestamp DESC
    LIMIT 1
);
