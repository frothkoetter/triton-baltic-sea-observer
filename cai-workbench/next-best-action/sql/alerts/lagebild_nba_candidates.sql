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
WHERE Data_Source IN ('GPS_Jammer', 'REF', 'RUS_Fleet', 'DAS_Cable')
   OR LOWER(COALESCE(CAST(details AS varchar), '')) LIKE '%flash%'
   OR LOWER(COALESCE(CAST(details AS varchar), '')) LIKE '%jamming%'
   OR LOWER(COALESCE(CAST(details AS varchar), '')) LIKE '%pipeline%'
   OR LOWER(COALESCE(CAST(details AS varchar), '')) LIKE '%submarine%'
   OR LOWER(COALESCE(CAST(details AS varchar), '')) LIKE '%anchor_drag%'
   OR LOWER(COALESCE(CAST(details AS varchar), '')) LIKE '%cui_sabotage%'
   OR LOWER(COALESCE(CAST(details AS varchar), '')) LIKE '%das%'
ORDER BY
    CASE Data_Source
        WHEN 'GPS_Jammer' THEN 1
        WHEN 'DAS_Cable' THEN 2
        WHEN 'REF' THEN 3
        WHEN 'RUS_Fleet' THEN 4
        ELSE 9
    END,
    CASE
        WHEN LOWER(COALESCE(CAST(details AS varchar), '')) LIKE '%flash%' THEN 0
        ELSE 1
    END,
    "timestamp" DESC
LIMIT {limit}