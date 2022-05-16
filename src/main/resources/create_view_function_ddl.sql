CREATE OR REPLACE FUNCTION create_materialized_view(the_schema_name VARCHAR, the_country VARCHAR) RETURNS BOOLEAN
    LANGUAGE plpgsql
AS
$$
DECLARE
BEGIN
    EXECUTE format('SET search_path = %I', the_schema_name);

    EXECUTE format('
            DROP MATERIALIZED VIEW IF EXISTS  %1$I.%2$I;
            CREATE MATERIALIZED VIEW %1$I.%2$I AS
            SELECT rt.id                                                                                      AS uid,
               NULLIF(replace(btrim((rt.data::json -> ''properties'' ->> ''id'')::text), ''""'', ''''), '''')       AS id,
               NULLIF(replace(btrim((rt.data::json -> ''properties'' ->> ''hash'')::text), ''""'', ''''), '''')     AS hash,
               CONCAT(UPPER(''%2$I''), NULLIF(REPLACE(BTRIM((rt.data::json -> ''properties'' ->> ''hash'')::text), ''""'', ''''), ''''))     AS cip_id,
               NULLIF(replace(btrim((rt.data::json -> ''properties'' ->> ''number'')::text), ''""'', ''''), '''')   AS number,
               NULLIF(replace(btrim((rt.data::json -> ''properties'' ->> ''street'')::text), ''""'', ''''), '''')   AS street,
               NULLIF(replace(btrim((rt.data::json -> ''properties'' ->> ''unit'')::text), ''""'', ''''), '''')     AS unit,
               NULLIF(replace(btrim((rt.data::json -> ''properties'' ->> ''city'')::text), ''""'', ''''), '''')     AS city,
               NULLIF(replace(btrim((rt.data::json -> ''properties'' ->> ''district'')::text), ''""'', ''''), '''') AS district,
               NULLIF(replace(btrim((rt.data::json -> ''properties'' ->> ''region'')::text), ''""'', ''''), '''')   AS region,
               NULLIF(replace(btrim((rt.data::json -> ''properties'' ->> ''postcode'')::text), ''""'', ''''), '''') AS postcode,
               to_tsvector(''english''::regconfig, array_to_string(
                   ARRAY [
                       NULLIF(replace(btrim((rt.data::json -> ''properties'' ->> ''number'')::text), ''""'', ''''), ''''),
                       NULLIF(replace(btrim((rt.data::json -> ''properties'' ->> ''street'')::text), ''""'', ''''), ''''),
                       NULLIF(replace(btrim((rt.data::json -> ''properties'' ->> ''unit'')::text), ''""'', ''''), ''''),
                       NULLIF(replace(btrim((rt.data::json -> ''properties'' ->> ''city'')::text), ''""'', ''''), ''''),
                       NULLIF(replace(btrim((rt.data::json -> ''properties'' ->> ''district'')::text), ''""'', ''''), ''''),
                       NULLIF(replace(btrim((rt.data::json -> ''properties'' ->> ''region'')::text), ''""'', ''''), ''''),
                       NULLIF(replace(btrim((rt.data::json -> ''properties'' ->> ''postcode'')::text), ''""'', ''''), '''')],
                   '' ''::text))                                                                                    AS nonuk_address_lookup_ft_col
            FROM raw_%2$I as rt;

            CREATE INDEX IF NOT EXISTS address_lookup_%2$I_ft_col_idx
             ON %1$I.%2$I USING gin (nonuk_address_lookup_ft_col);

            DROP VIEW IF EXISTS public.%2$I CASCADE;
            CREATE VIEW public.%2$I
            AS
            SELECT uid,
                   id,
                   hash,
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
            error_message = SQLERRM,
            timestamp = now()
        WHERE host_schema = format('%I-%I', '__schema__', '__table__');

        RETURN FALSE;
END;
$$
