select *
from actor_films
WHERE actor = '50 Cent'
ORDER BY year
limit 10;

DROP TABLE IF EXISTS actors;
DROP TYPE films;

CREATE TYPE films AS (
    filmid TEXT,
    film TEXT,
    year INTEGER,
    votes INTEGER,
    rating REAL
);

CREATE TYPE quality_class AS ENUM('star', 'good', 'average', 'bad');

CREATE TABLE actors (
    actorid TEXT,
    actor TEXT,
    current_year INTEGER,
    films films[],
    quality_class quality_class,
    is_active BOOLEAN,
    PRIMARY KEY(actorid, current_year)
);

ALTER TABLE IF EXISTS actors
    OWNER to postgres;

select * from actors limit 10;

-- 2 - Cumulative table generation query
INSERT INTO actors (
    WITH
    min_year AS (
        SELECT MIN(year) AS year
        FROM actor_films
    ), -- SELECT * from min_year
    max_year AS (
        SELECT MAX(year) AS year
        FROM actor_films
    ),
    years AS (
        SELECT *
        FROM GENERATE_SERIES(
                     (SELECT year FROM min_year),
                     (SELECT year FROM max_year)
             ) AS year
    ), -- SELECT * from years;
    actors_first_year AS ( -- actors and their first year
        SELECT actor, actorid,
            MIN (year) as first_year
        FROM actor_films
        -- WHERE actor = 'Aaron Paul'
        GROUP BY actor, actorid
    ), -- SELECT * FROM actors_first_year;
    actors_and_years AS ( -- actorids and all years after their first year
        SELECT *
        FROM actors_first_year
            JOIN years y ON actors_first_year.first_year <= y.year
    ), -- SELECT * FROM actors_and_years;
    actors_and_films AS (
        SELECT DISTINCT ON (ay.actorid, ay.year)
            ay.actor, ay.actorid,
            ay.year,
            ARRAY_REMOVE (  -- Remove all occurrences of NULL from the array
                ARRAY_AGG (  -- Aggregate all rows into an array
                    CASE
                        WHEN af.year is NOT NULL
                            THEN ROW(af.filmid, af.film, af.year, af.votes, af.rating)::films
                        END
                ) OVER (PARTITION BY ay.actorid ORDER BY COALESCE(ay.year, af.year)),  -- COALESCE: Returns the first of its arguments that is not null. Null is returned only if all arguments are null. It is often used to substitute a default value for null values when data is retrieved for display.
                NULL
            ) AS films
        FROM actors_and_years ay
             LEFT JOIN actor_films af
                   ON ay.actorid = af.actorid
                       AND ay.year = af.year
        ORDER BY ay.actorid, ay.year
    ), -- SELECT * FROM actors_and_films;
    last_active_year AS (
        SELECT actorid,
            actor,
            year,
            films[CARDINALITY(films)].year AS last_active_year
        FROM actors_and_films
    ), -- SELECT * FROM last_active_year;
    actors_and_active_years AS (
        SELECT
            af.actorid,
            af.actor,
            af.year as current_year,
            af.films,
            CASE
                WHEN lay.last_active_year = af.year THEN True
                ELSE False
            END as is_active
        FROM actors_and_films AS af
            JOIN last_active_year as lay
        ON af.actorid = lay.actorid AND af.year = lay.year
        ORDER BY af.actor
    ), --  SELECT * FROM actors_and_active_years;
    actors_with_ratings AS (
        SELECT
            aaf.actorid,
            aaf.current_year,
            AVG(movies.rating)::real AS rating
        FROM actors_and_active_years AS aaf
            JOIN last_active_year AS lay
                ON lay.actorid = aaf.actorid AND lay.year = aaf.current_year
            CROSS JOIN LATERAL UNNEST(aaf.films) AS movies
        WHERE lay.last_active_year = movies.year
        GROUP BY aaf.actorid, aaf.current_year
    ) -- SELECT * FROM actors_with_ratings;

    SELECT
        aaf.actorid,
        aaf.actor,
        aaf.current_year,
        aaf.films,
        CASE
            WHEN ar.rating > 8 THEN 'star'
            WHEN ar.rating > 7 THEN 'good'
            WHEN ar.rating > 6 THEN 'average'
            ELSE 'bad'
        END::quality_class AS rating,
        aaf.is_active
    FROM actors_and_active_years AS aaf
        JOIN actors_with_ratings AS ar
            ON aaf.actorid = ar.actorid AND aaf. current_year = ar. current_year);

SELECT *
FROM actors
WHERE actor = 'Aaron Paul'
LIMIT 10;


-- 3 - Slow changing dimension table generation query
DROP TABLE IF EXISTS actors_history_scd;

CREATE TABLE actors_history_scd (
    actorid TEXT, is_active BOOLEAN, quality_class quality_class, current_year INTEGER, start_date INTEGER, end_date INTEGER
);

-- 4 - Full backfill
WITH
    with_previous AS (
        SELECT actorid,
              current_year,
               LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_quality_class,
              quality_class,
               LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY current_year)     AS previous_is_active,
              is_active
        FROM actors
        WHERE current_year < (SELECT MAX(year) AS year FROM actor_films)
    ),
    with_change_indicator AS (
        SELECT *,
               CASE
                   WHEN quality_class <> previous_quality_class THEN 1
                   WHEN is_active <> previous_is_active THEN 1
                   ELSE 0
                   END AS change_indicator
        FROM with_previous
    ),
    with_streaks AS (
        SELECT *,
               SUM (change_indicator) OVER (PARTITION BY actorid ORDER BY current_year) AS streak_indicator
        FROM with_change_indicator
        ORDER BY actorid, streak_indicator DESC
    )

SELECT 
