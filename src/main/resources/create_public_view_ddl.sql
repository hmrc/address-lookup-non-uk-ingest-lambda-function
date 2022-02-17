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
