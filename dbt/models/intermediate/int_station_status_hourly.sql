-- dbt/models/intermediate/int_station_status_hourly.sql

/*
Hourly aggregation of station status data
Utilise DATE_TRUNC pour créer des slots horaires

💡 Gère les heures partielles : agrège les données disponibles
*/

WITH hourly_slots AS (
    SELECT 
        station_id,
        DATE_TRUNC('hour', extracted_at) AS hour_slot,
        
        -- Agrégations
        COUNT(*) AS snapshot_count,
        AVG(num_bikes_available) AS avg_bikes_available,
        MIN(num_bikes_available) AS min_bikes_available,
        MAX(num_bikes_available) AS max_bikes_available,
        
        AVG(num_docks_available) AS avg_docks_available,
        MIN(num_docks_available) AS min_docks_available,
        MAX(num_docks_available) AS max_docks_available,
        
        -- Taux de disponibilité moyen
        AVG(
            CASE 
                WHEN (num_bikes_available + num_docks_available) > 0 
                THEN num_bikes_available::NUMERIC / (num_bikes_available + num_docks_available)
                ELSE 0 
            END
        ) AS avg_availability_rate,
        
        -- Proportion du temps en état critique
        AVG(
            CASE 
                WHEN num_bikes_available < capacity * 0.1 
                  OR num_docks_available < capacity * 0.1 
                THEN 1.0 
                ELSE 0.0 
            END
        ) AS critical_time_ratio,
        
        -- Timestamps
        MIN(extracted_at) AS first_snapshot_at,
        MAX(extracted_at) AS last_snapshot_at,
        MAX(capacity) AS capacity  -- Assume stable dans l'heure
        
    FROM {{ ref('stg_velib_station_status') }}
    GROUP BY 
        station_id, 
        DATE_TRUNC('hour', extracted_at)
)


SELECT 
    station_id,
    hour_slot,
    snapshot_count,
    
    -- Métriques de disponibilité
    ROUND(avg_bikes_available, 1) AS avg_bikes_available,
    min_bikes_available,
    max_bikes_available,
    
    ROUND(avg_docks_available, 1) AS avg_docks_available,
    min_docks_available,
    max_docks_available,
    
    ROUND(avg_availability_rate * 100, 2) AS avg_availability_pct,
    ROUND(critical_time_ratio * 100, 2) AS critical_time_pct,
    
    -- Métadonnées
    first_snapshot_at,
    last_snapshot_at,
    capacity,
    
    -- Flag : heure complète ou partielle ?
    CASE 
        WHEN snapshot_count >= 10 THEN true  -- Au moins 10 snapshots (50 min)
        ELSE false 
    END AS is_complete_hour,
    
    -- Enrichissement temporel
    EXTRACT(HOUR FROM hour_slot) AS hour_of_day,
    EXTRACT(DOW FROM hour_slot) AS day_of_week,
    CASE 
        WHEN EXTRACT(DOW FROM hour_slot) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    CASE 
        WHEN EXTRACT(HOUR FROM hour_slot) BETWEEN 7 AND 9 THEN 'Morning Rush'
        WHEN EXTRACT(HOUR FROM hour_slot) BETWEEN 17 AND 19 THEN 'Evening Rush'
        WHEN EXTRACT(HOUR FROM hour_slot) BETWEEN 22 AND 6 THEN 'Night'
        ELSE 'Off-Peak'
    END AS time_period
    
FROM hourly_slots