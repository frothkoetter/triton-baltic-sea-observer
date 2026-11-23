-- =========================================================================================
-- TRINO: SOCMINT VERIFICATION VIA THREAT TILE SKETCHES (no Flink / no heavy spatial join)
-- =========================================================================================
-- Prerequisite for trust upgrades: outlier_detection_trino.sql Step 9 must have run at least once
-- so baltic_threat_tile_sketches has rows (TRINO_JOB=outliers). Empty table ⇒ view/MERGE OK, no corroboration.
--
-- Logic:
--   1. Map post lat/lon → zoom-11 quadkey (same grid as AIS baseline / outliers).
--   2. O(1) tile lookup: severity T-Digest P50/P90 + SetDigest lanes from last 4h threats.
--   3. Upgrade trust_tag UNVERIFIED → SENSOR_CORROBORATED when tile risk crosses thresholds.
--   4. Never upgrade rows already DISINFORMATION_CONFIRMED.
--
-- Run: TRINO_JOB=socmint_sketch ./run_trino_job from cai-workbench/trino-jobs
--      or: ./run_socmint_sketch_verification_trino.sh
-- =========================================================================================

USE iceberg.defense;

-- Empty until outlier job Step 9 runs; required so socmint_sketch_verified can be created.
CREATE TABLE IF NOT EXISTS baltic_threat_tile_sketches (
    quadkey varchar,
    severity_tdigest varbinary,
    outlier_type_set varbinary,
    mmsi_set varbinary,
    sensor_lane_set varbinary,
    event_count bigint,
    sev_p50 double,
    sev_p90 double,
    updated_at timestamp
);

-- Tile-level SOCMINT density (bot-campaign / burst detection without full scan)
CREATE TABLE IF NOT EXISTS socmint_tile_sketches (
    quadkey varchar,
    post_count bigint,
    account_set varbinary COMMENT 'SetDigest of user_username',
    trust_tag_set varbinary COMMENT 'SetDigest of trust_tag values',
    disinfo_count bigint,
    updated_at timestamp
);

DELETE FROM socmint_tile_sketches;

INSERT INTO socmint_tile_sketches (
    quadkey, post_count, account_set, trust_tag_set, disinfo_count, updated_at
)
WITH posts AS (
    SELECT
        bing_tile_quadkey(bing_tile_at(s.latitude, s.longitude, 11)) AS quadkey,
        s.user_username,
        COALESCE(s.trust_tag, 'UNVERIFIED_SOCMINT') AS trust_tag,
        COALESCE(s.disinformation_flag, false) AS disinfo
    FROM social_media_messages s
    WHERE s.latitude BETWEEN 53.0 AND 66.5
      AND s.longitude BETWEEN 9.0 AND 31.0
      AND coalesce(
          try(from_iso8601_timestamp(s.ts)),
          try_cast(replace(s.ts, 'Z', '') AS timestamp)
      ) >= current_timestamp - interval '24' hour
)
SELECT
    quadkey,
    COUNT(*) AS post_count,
    CAST(make_set_digest(user_username) AS varbinary),
    CAST(make_set_digest(trust_tag) AS varbinary),
    COUNT_IF(disinfo) AS disinfo_count,
    current_timestamp
FROM posts
WHERE quadkey IS NOT NULL
GROUP BY quadkey;

CREATE OR REPLACE VIEW socmint_sketch_verified AS
SELECT
    s.user_name,
    s.user_username,
    s.tweet,
    s.ts,
    s.priority,
    s.latitude,
    s.longitude,
    s.platform,
    s.intel_category,
    s.disinformation_flag,
    s.scenario_id,
    s.correlation_hint,
    bing_tile_quadkey(bing_tile_at(s.latitude, s.longitude, 11)) AS quadkey,
    t.sev_p50 AS threat_sev_p50,
    t.sev_p90 AS threat_sev_p90,
    t.event_count AS threat_event_count,
    st.post_count AS socmint_posts_in_tile,
    st.disinfo_count AS socmint_disinfo_in_tile,
    CASE
        WHEN COALESCE(s.disinformation_flag, false) = true
            OR COALESCE(s.trust_tag, '') = 'DISINFORMATION_CONFIRMED'
            THEN 'DISINFORMATION_CONFIRMED'
        WHEN s.intel_category = 'routine'
            THEN 'LOW_CONFIDENCE'
        WHEN t.quadkey IS NOT NULL
            AND (
                t.sev_p90 >= 65
                OR (t.sev_p50 >= 55 AND t.event_count >= 2)
                OR (
                    COALESCE(s.correlation_hint, '') LIKE '%ais%'
                    AND t.event_count >= 1
                    AND t.sev_p50 >= 50
                )
                OR (
                    COALESCE(s.correlation_hint, '') LIKE '%gps%'
                    AND t.sev_p50 >= 50
                    AND t.event_count >= 1
                )
            )
            THEN 'SENSOR_CORROBORATED'
        ELSE COALESCE(s.trust_tag, 'UNVERIFIED_SOCMINT')
    END AS effective_trust_tag,
    CASE
        WHEN COALESCE(s.disinformation_flag, false) = true THEN s.verification_notes
        WHEN t.quadkey IS NULL THEN s.verification_notes
        ELSE concat(
            coalesce(s.verification_notes, ''),
            CASE WHEN s.verification_notes IS NOT NULL AND s.verification_notes <> '' THEN ' | ' ELSE '' END,
            format(
                'Trino sketch tile %s: threat_events=%s sev_p50=%.0f sev_p90=%.0f socmint_posts=%s',
                t.quadkey,
                COALESCE(CAST(t.event_count AS varchar), '0'),
                COALESCE(t.sev_p50, 0),
                COALESCE(t.sev_p90, 0),
                COALESCE(CAST(st.post_count AS varchar), '0')
            )
        )
    END AS effective_verification_notes
FROM social_media_messages s
LEFT JOIN baltic_threat_tile_sketches t
    ON bing_tile_quadkey(bing_tile_at(s.latitude, s.longitude, 11)) = t.quadkey
LEFT JOIN socmint_tile_sketches st
    ON st.quadkey = bing_tile_quadkey(bing_tile_at(s.latitude, s.longitude, 11))
WHERE coalesce(
    try(from_iso8601_timestamp(s.ts)),
    try_cast(replace(s.ts, 'Z', '') AS timestamp)
) >= current_timestamp - interval '24' hour;

-- Persist trust upgrades for Lagebild/NBA (reads social_media_messages.trust_tag)
MERGE INTO social_media_messages AS target
USING (
    SELECT
        user_username,
        ts,
        tweet,
        effective_trust_tag,
        effective_verification_notes
    FROM socmint_sketch_verified v
    WHERE effective_trust_tag IN ('SENSOR_CORROBORATED', 'LOW_CONFIDENCE')
      AND COALESCE(v.disinformation_flag, false) = false
) AS src
ON target.user_username = src.user_username
   AND target.ts = src.ts
   AND target.tweet = src.tweet
WHEN MATCHED AND COALESCE(target.trust_tag, 'UNVERIFIED_SOCMINT') = 'UNVERIFIED_SOCMINT' THEN
    UPDATE SET
        trust_tag = src.effective_trust_tag,
        verification_notes = src.effective_verification_notes,
        priority = CASE
            WHEN src.effective_trust_tag = 'SENSOR_CORROBORATED'
                AND COALESCE(target.priority, 'niedrig') = 'mittel' THEN 'hoch'
            ELSE target.priority
        END;