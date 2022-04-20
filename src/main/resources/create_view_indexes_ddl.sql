BEGIN;
CREATE INDEX IF NOT EXISTS nonuk_address_lookup___table___ft_col_idx
        ON __schema__.__table__ USING gin (address_lookup_ft_col);
COMMIT;
