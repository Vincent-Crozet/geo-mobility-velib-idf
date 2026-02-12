-- dbt/models/marts/core/dim_station.sql

{{
  config(
    materialized='table',
    unique_key='station_id',
    indexes=[
      {'columns': ['station_id'], 'unique': true},
      {'columns': ['commune_code']},
      {'columns': ['current_validity']},
      {'columns': ['station_id', 'valid_from', 'valid_to']}
    ]
  )
}}

/*
Granularity    : historical data of stations
Natural key    : station_id + valid_from + valid_to (historique complet)

Enrichissements :
- geographical (Zip code + commune) + commune population: int_station_historical_geo_enriched (SCD2-aware, grain station_id + valid_from)
- Population 500m                   : int_station_pop_500m     (SCD2-aware, grain station_id + valid_from)
- Operationnal                      : caracterization with quartile

*/

WITH geo_enriched AS (
    SELECT
        g.*,

        -- Population 500m : grain (station_id + valid_from), SCD2-aware
        pop.population_500m,
        pop.pop_point_count_500m,

        -- Quartile de capacité calculé sur les stations actives uniquement
        -- Les versions historiques reçoivent NULL (comportement attendu)
        CASE
            WHEN cq.capacity_quartile = 1 THEN 'Q1-Small'
            WHEN cq.capacity_quartile = 2 THEN 'Q2-Medium'
            WHEN cq.capacity_quartile = 3 THEN 'Q3-Large'
            ELSE 'Q4-XLarge'
        END AS station_size_category,

        -- Ratio population communale / capacité
        CASE
            WHEN g.commune_population IS NOT NULL AND g.capacity > 0
            THEN g.commune_population::NUMERIC / g.capacity
            ELSE NULL
        END AS commune_population_per_bike,

        -- Ratio population locale 500m / capacité
        CASE
            WHEN pop.population_500m > 0 AND g.capacity > 0
            THEN pop.population_500m::NUMERIC / g.capacity
            ELSE NULL
        END AS local_population_per_bike,

        -- Catégorisation basée sur la densité locale 500m
        CASE
            WHEN pop.population_500m > 5000 THEN 'Urban Core'
            WHEN pop.population_500m > 2000 THEN 'Urban'
            WHEN pop.population_500m > 500  THEN 'Suburban'
            ELSE 'Peripheral'
        END AS area_type

    FROM {{ ref('int_station_historical_geo_enriched') }} g
    -- Population 500m : même grain (station_id + valid_from)
    LEFT JOIN {{ ref('int_station_pop_500m') }} pop
        ON  pop.station_id = g.station_id
        AND pop.valid_from  = g.valid_from
    -- Quartile sur stations actives uniquement
    LEFT JOIN (
        SELECT
            station_id,
            NTILE(4) OVER (ORDER BY capacity) AS capacity_quartile
        FROM {{ ref('stg_velib_station_current') }}
    ) cq ON cq.station_id = g.station_id
)

SELECT
    -- Clés
    station_id,
    station_code,

    -- Attributs descriptifs
    name AS station_name,
    capacity,
    station_size_category,
    geometry,

    -- Enrichissement géographique commune
    commune_name,
    commune_code,
    commune_population,
    ROUND(commune_population_per_bike, 0) AS commune_population_per_bike,

    -- Population locale 500m (statique, haute résolution, SCD2-aware)
    population_500m,
    ROUND(local_population_per_bike, 0)   AS local_population_per_bike,

    -- Catégorisation
    area_type,

    -- Métadonnées opérationnelles
    rental_methods,

    -- SCD Type 2
    valid_from,
    valid_to,
    current_validity,

    -- Flags qualité
    CASE WHEN commune_name       IS NOT NULL THEN true ELSE false END AS has_geo_enrichment,
    CASE WHEN commune_population IS NOT NULL THEN true ELSE false END AS has_commune_population,
    CASE WHEN population_500m    > 0         THEN true ELSE false END AS has_local_population_500m

FROM geo_enriched
ORDER BY current_validity DESC, station_id