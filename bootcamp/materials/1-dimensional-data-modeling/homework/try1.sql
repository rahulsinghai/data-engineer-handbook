select *
from actor_films
WHERE actor = '50 Cent'
ORDER BY year
limit 10;

DROP TABLE IF EXISTS actors;
DROP TYPE film_stats;

CREATE TYPE film_stats AS (
    film TEXT,
    votes INTEGER,
    rating REAL,
    filmid TEXT
);

CREATE TYPE quality_class AS ENUM('star', 'good', 'average', 'bad');

CREATE TABLE actors (
                        actor TEXT,
                        films film_stats[],
                        quality_class quality_class,
                        current_year INTEGER,
                        is_active BOOLEAN,
                        PRIMARY KEY(actor, current_year)
);

ALTER TABLE IF EXISTS actors
    OWNER to postgres;

select * from actors limit 10;

-- INSERT INTO actors
WITH
    min_year AS (
        SELECT MIN(year) AS year
        FROM actor_films
    ),
    max_year AS (
        SELECT MAX(year) AS year
        FROM actor_films
    ),
    years_range AS (
        SELECT *
        FROM GENERATE_SERIES(
                     (SELECT year FROM min_year),
                     (SELECT year FROM max_year)
             ) AS year
    ),
    a AS ( -- actors and their first year
        SELECT
            actorid,
            actor,
            MIN(year) AS first_year
        FROM actor_films
        WHERE actor = '50 Cent'
        GROUP BY actorid, actor
    ),
    actorids_and_years AS ( -- actorids and years after their first year
        SELECT *
        FROM a
                 JOIN years_range y
                      ON a.first_year <= y.year
    ),
    duplicated_years_films AS (
        SELECT
            aay.actorid,
            aay.actor,
            aay.year AS current_year,
            ARRAY_REMOVE(  -- Remove all occurrences of NULL from the array
                    ARRAY_AGG(  -- Aggregate all rows into an array
                            CASE
                                WHEN af.year IS NOT NULL
                                    THEN ROW(
                                    af.film,
                                    af.votes,
                                    af.rating,
                                    af.filmid
                                    )::film_stats
                                END
                    )
                            OVER (PARTITION BY aay.actorid ORDER BY COALESCE(aay.year, af.year)),  -- COALESCE: Returns the first of its arguments that is not null. Null is returned only if all arguments are null. It is often used to substitute a default value for null values when data is retrieved for display.
                    NULL
            ) AS films
        FROM actorids_and_years aay
                 LEFT JOIN actor_films af
                           ON aay.actorid = af.actorid
                               AND aay.year = af.year
        ORDER BY aay.actorid, aay.year
    ),
    windowed AS (
        SELECT *, row_number() over (PARTITION BY actorid, current_year) AS row_num
        FROM duplicated_years_films
    ),
    static AS (
        SELECT
            actorid,
            actor
        FROM actor_films
        GROUP BY actorid, actor
    )

SELECT
    w.actorid,
    w.actor,
    films,
    CASE
        WHEN (films[CARDINALITY(films)]::film_stats).rating > 20 THEN 'star'
        WHEN (films[CARDINALITY(films)]::film_stats).rating > 15 THEN 'good'
        WHEN (films[CARDINALITY(films)]::film_stats).rating > 10 THEN 'average'
        ELSE 'bad'
        END::quality_class AS quality_class,
        w.year - (films[CARDINALITY(films)]::film_stats).year as years_since_last_active,
    w.year,
    (films[CARDINALITY(films)]::film_stats).year = year AS is_active
FROM windowed w
    JOIN static s
ON w.actorid = s.actorid
    AND w.actor = s.actor;

--
--     windowed AS (
-- WITH last_year AS (
--     SELECT * FROM actor_films
--     WHERE year = 1969
-- ),
-- this_year AS (
--     SELECT * FROM actor_films
--     WHERE year = 1970
-- )
--
-- SELECT
--     COALESCE(t.actor, y.actor) AS actor,
--     CASE WHEN y.film_stats IS NULL
--         THEN ARRAY[ROW(
--             t.filmid,
--             t.film,
--             t.votes,
--             t.rating
--             )::film_stats]
--          WHEN t.year IS NOT NULL THEN y.film_stats || ARRAY[ROW(
--              t.filmid,
--              t.film,
--              t.votes,
--              t.rating
--              )::film_stats]
--          ELSE y.film_stats
--         END AS film_stats,
--     CASE
--         WHEN t.year IS NOT NULL THEN
--             CASE WHEN t.pts > 20 THEN 'star'
--                  WHEN t.pts > 15 THEN 'good'
--                  WHEN t.pts > 10 THEN 'average'
--                  ELSE 'bad'
--                 END::quality_class
--         ELSE y.quality_class
--         END AS quality_class,
--     CASE
--         WHEN t.year IS NOT NULL THEN 0
--         ELSE y.years_since_last_year + 1
--         END AS years_since_last_year,
--     COALESCE(t.year, y.current_year + 1) AS current_year,
--     t.year IS NOT NULL AS is_active
-- FROM this_year t FULL OUTER JOIN last_year y
--     ON t.actor = y.actor;