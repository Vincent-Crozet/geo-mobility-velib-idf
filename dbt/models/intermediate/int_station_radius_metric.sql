WITH stations_lambert AS (
    -- Projection station into metric CRS
    SELECT
        station_id,
        ST_Transform(geometry, 2154) AS geometry_2154
    FROM {{ ref('stg_velib_station_current') }}
),

-- ─── 1. Compute population within 500m ────────────────────────────────────
pop_points AS (
    SELECT
        fid,
        population,
        geometry  -- déjà en EPSG:2154 (cf. source)
    FROM {{ ref('stg_geo_pop_idf') }}
),

pop_500m AS (
    SELECT
        s.station_id,
        COALESCE(SUM(p.population), 0)  AS population_500m,
        COUNT(p.fid)                    AS pop_point_count_500m  -- utile pour le debug
    FROM stations_lambert s
    LEFT JOIN pop_points p
        ON ST_DWithin(s.geometry_2154, p.geometry, 500)
    GROUP BY s.station_id
),

-- ─── 2. Bikes available within 500m ───────────
/*
  On joint les statuts temps-réel les plus récents de chaque station voisine.
  On exclut la station elle-même (station_id != neighbor.station_id).
  On utilise le dernier snapshot disponible par station voisine.
*/
latest_status AS (
    SELECT DISTINCT ON (station_id)
        station_id,
        num_bikes_available,
        mechanical_available,
        ebikes_available,
        num_docks_available
    FROM {{ ref('stg_velib_station_status') }}
    ORDER BY station_id, extracted_at DESC
),

neighbor_stations_lambert AS (
    -- Même reprojection pour les voisines
    SELECT
        s.station_id,
        ST_Transform(s.geometry, 2154) AS geometry_2154
    FROM {{ ref('stg_velib_station_current') }} s
),

bikes_500m AS (
    SELECT
        s.station_id,
        COALESCE(SUM(st.num_bikes_available), 0)  AS bikes_available_500m,
        COALESCE(SUM(st.mechanical_available), 0) AS mechanical_available_500m,
        COALESCE(SUM(st.ebikes_available), 0)     AS ebikes_available_500m,
        COALESCE(SUM(st.num_docks_available), 0)     AS docks_available_500m,
        COUNT(n.station_id)                        AS neighbor_station_count_500m
    FROM stations_lambert s
    -- Stations voisines dans un rayon de 500m (station cible exclue)
    LEFT JOIN neighbor_stations_lambert n
        ON  ST_DWithin(s.geometry_2154, n.geometry_2154, 500)
        AND n.station_id != s.station_id
    -- Statut temps-réel des voisines
    LEFT JOIN latest_status st
        ON st.station_id = n.station_id
    GROUP BY s.station_id
)

-- ─── Assemblage final ────────────────────────────────────────────────────────
SELECT
    s.station_id,

    -- Population locale
    p.population_500m,
    p.pop_point_count_500m,

    -- Vélos voisins
    b.bikes_available_500m,
    b.mechanical_available_500m,
    b.ebikes_available_500m,
    b.docks_available_500m,
    b.neighbor_station_count_500m

FROM stations_lambert s
LEFT JOIN pop_500m  p ON p.station_id = s.station_id
LEFT JOIN bikes_500m b ON b.station_id = s.station_id
