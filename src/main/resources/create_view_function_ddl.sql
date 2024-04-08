CREATE OR REPLACE FUNCTION create_non_uk_address_lookup_materialized_view(the_schema_name VARCHAR, the_country VARCHAR) RETURNS BOOLEAN LANGUAGE plpgsql AS $$ DECLARE BEGIN EXECUTE format('SET search_path = %I', the_schema_name); EXECUTE format('
            DROP MATERIALIZED VIEW IF EXISTS  %1$I.%2$I;
            CREATE MATERIALIZED VIEW %1$I.%2$I AS
            WITH dups as (SELECT *,
                ROW_NUMBER() OVER (
                PARTITION BY cip_hash ORDER BY id DESC) AS rn
                FROM %1$I.expanded_raw_%2$I)
            SELECT rt.id                        AS uid,
               id       AS id,
               hash     AS hash,
               cip_hash     AS cip_hash,
               CONCAT(UPPER(''%2$I''), cip_hash)     AS cip_id,
               number   AS number,
               street   AS street,
               unit     AS unit,
               city     AS city,
               district AS district,
               region   AS region,
               postcode AS postcode,
               to_tsvector(''english''::regconfig, ' || 'array_to_string(
                   ARRAY [number,street,unit,city,district,region,postcode],'' ''::text)) AS nonuk_address_lookup_ft_col
            FROM dups as rt WHERE rn = 1;

            CREATE INDEX IF NOT EXISTS address_lookup_%2$I_ft_col_idx
             ON %1$I.%2$I USING gin (nonuk_address_lookup_ft_col);

            DROP VIEW IF EXISTS public.%2$I CASCADE;
            CREATE VIEW public.%2$I
            AS
            SELECT uid,
                   id,
                   hash,
                   cip_hash,
                   cip_id,
                   number,
                   street,
                   unit,
                   city,
                   district,
                   region,
                   postcode,
                   nonuk_address_lookup_ft_col
            FROM %1$I.%2$I;

            GRANT SELECT ON public.%2$I TO addresslookupreader;

            UPDATE public.nonuk_address_lookup_status
            SET status    = ''completed'',
                timestamp = now()
            WHERE host_schema = ''%1$I-%2$I'';', the_schema_name, the_country);
    RETURN TRUE;
EXCEPTION
    WHEN OTHERS THEN
        UPDATE public.nonuk_address_lookup_status
        SET status    = 'errored',
            error_message = format('[%I] - %I', SQLSTATE, SQLERRM),
            timestamp = now()
        WHERE host_schema = format('%I-%I', the_schema_name, the_country);

        RETURN FALSE;
END;
$$
