-- dbt/models/marts/core/dim_stations.sql

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

Grain : Une ligne par version de station (historisation des changements)
Clé primaire : station_id (pour version actuelle uniquement)
Clé naturelle : station_id + valid_from + valid_to (pour historique complet)

Enrichissements :
- Géographique : commune, population
- Opérationnel : capacité, type de station
- Temporel : dates de validité (SCD2)
*/

WITH current_stations AS (
    SELECT
        station_id,
        station_code,
        name,
        capacity,
        geometry,
        rental_methods,
        
        -- Métadonnées SCD2
        valid_from,
        valid_to,
        current_validity
    FROM {{ ref('stg_velib_station_historical') }}
),

geo_enriched AS (
    SELECT
        cs.*,
        
        -- Enrichissement géographique
        c.nomcom AS commune_name,
        c.insee AS commune_code,
        p.population,
        
        -- Calculs dérivés
        CASE 
            WHEN cq.capacity_quartile = 1 THEN 'Q1-Small'
            WHEN cq.capacity_quartile = 2 THEN 'Q2-Medium'
            WHEN cq.capacity_quartile = 3 THEN 'Q3-Large'
            ELSE 'Q4-XLarge'
        END AS station_size_category
        
        CASE 
            WHEN p.population IS NOT NULL 
             AND cs.capacity > 0 
            THEN p.population::NUMERIC / cs.capacity
            ELSE NULL
        END AS population_per_bike,
        
        -- Flag station urbaine vs périphérique (basé sur densité population)
        CASE 
            WHEN p.population > 50000 THEN 'Urban Core'
            WHEN p.population > 20000 THEN 'Urban'
            WHEN p.population > 5000 THEN 'Suburban'
            ELSE 'Peripheral'
        END AS area_type
        
    FROM current_stations cs
    LEFT JOIN {{ ref('stg_geo_communes_idf') }} c
        ON ST_Within(cs.geometry, c.geometry)
    LEFT JOIN {{ ref('stg_geo_pop_communes') }} p
        ON c.nomcom = p.nomcom
)

SELECT
    -- Clés
    station_id,
    station_code,
    
    -- Attributs descriptifs
    name AS station_name,
    capacity,
    station_size,
    station_size_category,
    -- geometry
    geometry,
    
    -- Enrichissement géographique
    commune_name,
    commune_code,
    population,
    area_type,
    ROUND(population_per_bike, 0) AS population_per_bike,
    
    -- Métadonnées opérationnelles
    rental_methods,
    
    -- SCD Type 2 : historisation
    valid_from,
    valid_to,
    current_validity,
    
    -- Flags qualité
    CASE 
        WHEN commune_name IS NOT NULL THEN true
        ELSE false
    END AS has_geo_enrichment,
    
    CASE 
        WHEN population IS NOT NULL THEN true
        ELSE false
    END AS has_population_data
    

FROM geo_enriched

-- Ordre par défaut : stations actives en premier
ORDER BY current_validity DESC, station_id