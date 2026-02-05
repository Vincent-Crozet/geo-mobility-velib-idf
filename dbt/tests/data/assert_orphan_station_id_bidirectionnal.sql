-- dbt/tests/data/assert_no_orphan_stations_bidirectional.sql
{{ config(
    severity='warn',
    store_failures=true,
    tags=['data_quality', 'referential_integrity', 'orphans']
) }}

/*
Detect for the latest extacted data if 
1. a station id is missing in (station or station_status)

FULL OUTER JOIN to provide bidirectionnal evaluation
*/

-- Get latest Station SCD
WITH latest_extraction AS (
    SELECT 
        MAX(extracted_at) AS max_extracted_at
    FROM {{ source('velib', 'stations_scd') }} AS s
    ),

last_station_scd AS (
    SELECT s.station_id,
        s.extracted_at
    FROM {{ source('velib', 'stations_scd') }} AS s
    JOIN latest_extraction le
        ON s.extracted_at = le.max_extracted_at
    ),

-- Get latest Station Status
latest_status_extraction AS (
    SELECT 
        MAX(extracted_at) AS max_extracted_at
    FROM {{ source('velib', 'station_status') }} AS s
    ),

latest_station_status AS (
    SELECT s.station_id,
        s.extracted_at
    FROM {{ source('velib', 'station_status') }} AS s
    JOIN latest_status_extraction le
        ON s.extracted_at = le.max_extracted_at
    ),

-- Comparison
full_comparison AS (
    SELECT 
        COALESCE(scd.station_id, st.station_id) AS station_id,
        scd.extracted_at AS scd_extracted_at,
        st.extracted_at AS status_extracted_at,
        -- Determine orphan type
        CASE 
            WHEN scd.station_id IS NULL THEN 'orphan_in_status'
            WHEN st.station_id IS NULL THEN 'orphan_in_stations'
            ELSE 'matched'
        END AS orphan_type
    FROM last_station_scd scd
    FULL OUTER JOIN latest_station_status st
        ON scd.station_id = st.station_id
),

-- Filter to keep only orphans
orphans AS (
    SELECT 
        station_id,
        scd_extracted_at,
        status_extracted_at,
        orphan_type,
        -- Detailed error message
        CASE orphan_type
            WHEN 'orphan_in_status' THEN 
                FORMAT(
                    'Station %s has status data (extracted at %s) but does NOT exist in stations_scd',
                    station_id,
                    status_extracted_at
                )
            WHEN 'orphan_in_stations' THEN 
                FORMAT(
                    'Station %s (%s) exists in stations_scd  but has NO status data at extraction time %s',
                    station_id,
                    scd_extracted_at
                )
        END AS error_message,
        NOW() AS checked_at
    FROM full_comparison
    WHERE orphan_type != 'matched'
)

-- Output
SELECT 
    station_id,
    scd_extracted_at,
    status_extracted_at,
    orphan_type,
    error_message,
    checked_at
FROM orphans
ORDER BY 
    orphan_type DESC,  -- orphan_in_status first (more critical)
    station_id