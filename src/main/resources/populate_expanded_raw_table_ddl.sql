insert into __schema__.expanded_raw___table__
select   rbm.id
       , rbm.ingested_at
       , coalesce(replace (btrim(pj.id), '""', ''), '')
       , coalesce(replace (btrim(pj.number), '""', ''), '')
       , coalesce(replace (btrim(pj.street), '""', ''), '')
       , coalesce(replace (btrim(pj.unit), '""', ''), '')
       , coalesce(replace (btrim(pj.city), '""', ''), '')
       , coalesce(replace (btrim(pj.district), '""', ''), '')
       , coalesce(replace (btrim(pj.region), '""', ''), '')
       , coalesce(replace (btrim(pj.postcode), '""', ''), '')
       , coalesce(replace (btrim(pj.hash), '""', ''), '')
       , MD5(CONCAT(pj.number, pj.street, pj.unit, pj.city, pj.district, pj.region, pj.postcode))
from __schema__.raw___table__ rbm,
     json_to_record(to_json(rbm.data) -> 'properties') as pj(id text, city text, hash text, unit text, number text, street text, region text, district text, postcode text);
