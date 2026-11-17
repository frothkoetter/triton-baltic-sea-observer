-- NBA Phase 2: ranked Lagebild rows for deterministic alerts (live view).
SELECT
    Data_Source,
    ID,
    harbour_name,
    latitude,
    longitude,
    details,
    "timestamp" AS event_ts
FROM {qualified}.lagebild
WHERE Data_Source IN ('GPS_Jammer', 'REF', 'RUS_Fleet')
   OR LOWER(COALESCE(CAST(details AS varchar), '')) LIKE '%flash%'
   OR LOWER(COALESCE(CAST(details AS varchar), '')) LIKE '%jamming%'
   OR LOWER(COALESCE(CAST(details AS varchar), '')) LIKE '%pipeline%'
   OR LOWER(COALESCE(CAST(details AS varchar), '')) LIKE '%submarine%'
ORDER BY
    CASE Data_Source
        WHEN 'GPS_Jammer' THEN 1
        WHEN 'REF' THEN 2
        WHEN 'RUS_Fleet' THEN 3
        ELSE 9
    END,
    CASE
        WHEN LOWER(COALESCE(CAST(details AS varchar), '')) LIKE '%flash%' THEN 0
        ELSE 1
    END,
    "timestamp" DESC
LIMIT {limit}