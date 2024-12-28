# Dimensional Data Modelling - Idempotent Pipelines & Slowly changing dimension

## Table of Contents

- [Idempotent pipelines](#idempotent-pipelines)
- [Slowly-changing dimensions](#should-you-model-as-slowly-changing-dimensions)
- [Workshop/SCD Lab](#workshopscd-lab)

## Idempotent Pipelines

Your pipeline should produces the same results regardless of when it’s run.
Idempotent definition: Denoting an element of a set which is unchanged in value when multiplied or otherwise operated on by itself

Pipelines should produce the same results
- Production & backfill data are the same after pipeline has run
- Regardless of the day you run it
- Regardless of how many times you run it
- Regardless of the hour that you run it

### Why is troubleshooting non-idempotent pipelines hard?

- Silent failure!
- You only see it when you get data inconsistencies and a data analyst yells at you

### What can make a pipeline not idempotent

- INSERT INTO without TRUNCATE
    - Use MERGE or INSERT OVERWRITE every time please
- Using Start_date > without a corresponding end_date <
- Not using a full set of partition sensors
    - (pipeline might run when there is no/partial data)
- Airflow: Not using depends_on_past for cumulative pipelines. In cumulative pipelines, today's data is JOINed with yesterday's data.
- Relying on the “latest” partition of a not properly modeled SCD table
    - dim_all_fake_accounts only used todays data
    - So much pain at Facebook, DAILY DIMENSIONS AND “latest” partition is a very bad idea
    - Cumulative table design AMPLIFIES this bug
- Relying on the “latest” partition of anything else

### The pains of not having idempotent pipelines

- Back-filling causes inconsistencies between the old and restated data
- Very hard to troubleshoot bugs
- Unit testing cannot replicate the production behavior
- Silent failures

## Should you model as Slowly Changing Dimensions?

Slowly Changing Dimension is a dimension that changes over time. e.g., age, mobile version, country of residence

### SCDs are inherently non-idempotent.

Solution is **Daily Dimensions**: Storge is cheap, so design it such way that we store data for each interval.
<br>Every day you will store your age: Today I am this age, tomorrow this age.

### Why do dimensions change?

- Someone decides they hate iPhone and want Android now
- Someone migrates from team dog to team cat
- Someone migrates from USA to another country

**Rapidly changing dimension**: Changes frequently. E.g., hear-rate

- Max, the creator of Airﬂow HATES SCD data modeling
    - Link to Max’s article about why SCD’s SUCK
- What are the options here?
    - Latest snapshot
    - Daily/Monthly/Yearly snapshot
    - SCD
- How slowly changing are the dimensions you’re modeling?

### 3 ways to model dimensions that change

- Singular snapshots
    - Latest snapshot: Only have latest value for that dimension. Problem is backfill might break it, as old backfill might not have value stored for that dimension.
    - BE CAREFUL SINCE THESE ARE NOT IDEMPOTENT
- Daily partitioned snapshots: Max's is all about daily snapshots. Backfill will work in this case, because the backfill will have that day's value (age of user).
- SCD: Collapsing all those daily snapshots based on rows that have same value for that dimension. Instead of having 365 rows of saying I am 41, I will have one row saying I am 41 from September 29 of one year to another. Gives more compression.

### The types of Slowly Changing Dimensions

- Type 0: Aren't actually slowly changing (e.g. birth date)
- Type 1: You only care about the latest value
    - NEVER USE THIS TYPE BECAUSE IT MAKES YOUR PIPELINES NOT IDEMPOTENT ANYMORE
- **Type 2**
    - You care about what the value was from “start_date” to “end_date”
    - Current values usually have either an is_current Boolean column OR an end_date that is:
        - NULL
        - Far into the future like 9999-12-31
    - Hard to use: Since there’s more than 1 row per dimension, you need to be careful about filtering on time
    - MY FAVORITE TYPE OF SCD. The only type of SCD that is purely IDEMPOTENT
- Type 3
    - You only care about “original” and “current”
    - No information, when value changed. Multiple changes are all lost.
    - Benefits: You only have 1 row per dimension
    - Drawbacks: You lose the history in between original and current
    - Is this idempotent? Partially, which means it’s not

### Which types are idempotent?

- Type 0 and Type 2 are idempotent
    - Type 0 is because the values are unchanging
    - Type 2 is but you need to be careful with how you use the start_date and end_date syntax!
- Type 1 isn’t idempotent
    - If you backfill with this dataset, you’ll get the dimension as it is now, not as it was then!
- Type 3 isn’t idempotent
    - If you backfill with this dataset, it’s impossible to know when to pick “original” vs “current” and you’ll either

### SCD Type 2 Backfill Loading

- Load the entire history in one query
    - Ineﬃcient but nimble
    - 1 query and you’re done
- Incrementally load the data after the previous SCD is generated
    - Has the same “depends_on_past” constraint
    - Eﬃcient but cumbersome

## Workshop/SCD Lab

- We will create an SCD Type 2 table, with 2 columns: **start_season** & **end_season**
- **current_season** should be end of columns list and acts as date column for partitions.

```sql
CREATE TABLE IF NOT EXISTS public.players_scd (
	player_name TEXT,
	scoring_class scoring_class,
	is_active BOOLEAN,
	streak_identifier INTEGER,
	start_season INTEGER,
	end_season INTEGER,
	current_season INTEGER,
	PRIMARY KEY (player_name, start_season)
)

SELECT player_name, scoring_class, is_active
FROM players
WHERE current_season = 2022;
```

We wanna see how Michael Jordan retired and then he came back, and see how his records changed over time.
1. First, we will write a query that will create an SCD table by looking at all the history.

   [scd_generation_query](./lecture-lab/scd_generation_query.sql)
    
    ```sql
    # We will use LAG Window function to check if player's dimension changed in 2 consecutive years
    WITH with_previous AS (
        SELECT
            player_name,
            current_season,
            scoring_class,
            is_active
            LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER_BY current_season) AS previous_scoring_class,
            LAG(is_active, 1) OVER (PARTITION BY player_name ORDER_BY current_season) AS previous_is_active
        FROM players
    ),
        # Create an identifier to identify when a change happens (good to bad, vice-versa OR active to inactive) 
        with_indicators AS (
            SELECT *,
                CASE
                    WHEN scoring_class <> previous_scoring_class THEN 1
                    WHEN is_active <> previous_is_active THEN 1
                    ELSE 0
                END AS change_indicator,
            FROM with_previous
        ),
        # Create a streak: Once change happens, the streak will increment its value
        with_streaks AS (
            SELECT *,
                SUM(change_indicator) OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
            FROM with_indicators
        )
        
        # We will aggregate on with_streaks to collapse all those records
        SELECT player_name,
            scoring_class,
            is_active,
            streak_identifier,
            MIN(current_season) AS start_season,
            MAX(current_season) AS end_season,
        FROM with_streaks
        GROUP BY player_name, streak_identifier, is_active, scoring_class
        ORDER BY player_name, streak_identifier
    ```

2. Take an SCD table and build on top of it incrementally.
   We will add a filter for 2021 current_season, so that we can build incrementally from there.
   If using Airflow, then this year value will come as a parameter to DAG run:

   [incremental_scd_query](./lecture-lab/incremental_scd_query.sql)

    ```sql
    INSERT INTO players_scd
    # We will use LAG Window function to check if player's dimension changed in 2 consecutive years
    WITH with_previous AS (
        SELECT
            player_name,
            current_season,
            scoring_class,
            is_active
            LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER_BY current_season) AS previous_scoring_class,
            LAG(is_active, 1) OVER (PARTITION BY player_name ORDER_BY current_season) AS previous_is_active
        FROM players
        WHERE current_season <= 2021
    ),
        # Create an identifier to identify when a change happens (good to bad, vice-versa OR active to inactive) 
        with_indicators AS (
            SELECT *,
                CASE
                    WHEN scoring_class <> previous_scoring_class THEN 1
                    WHEN is_active <> previous_is_active THEN 1
                    ELSE 0
                END AS change_indicator,
            FROM with_previous
        ),
        # Create a streak: Once change happens, the streak will increment its value
        with_streaks AS (
            SELECT *,
                SUM(change_indicator) OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
            FROM with_indicators
        )
        
        # We will aggregate on with_streaks to collapse all those records
        SELECT player_name,
            scoring_class,
            is_active,
            streak_identifier,
            MIN(current_season) AS start_season,
            MAX(current_season) AS end_season,
            2021 AS current_season
        FROM with_streaks
        GROUP BY player_name, streak_identifier, is_active, scoring_class
        ORDER BY player_name, streak_identifier
   
    SELECT * FROM players_scd;
    ```

Problem with this query is that it can get OOM, if too many players are changing every year, as that will blow up the cardinality.
Will fail if we have got billions of users.

Following query processes 20X less data.
It incrementally adds more & more players:

```sql
DROP TABLE IF EXISTS public.players_scd;

CREATE TABLE IF NOT EXISTS public.players_scd (
	player_name TEXT,
	scoring_class scoring_class,
	is_active BOOLEAN,
	streak_identifier INTEGER,
	start_season INTEGER,
	end_season INTEGER,
	current_season INTEGER,
	PRIMARY KEY (player_name, start_season)
)

CREATE TYPE scd_type AS (
    scoring_class scoring_class,
    is_active BOOLEAN,
    start_season INTEGER,
    end_season INTEGER
)

WITH last_season_scd AS (
    SELECT * FROM players_scd
    WHERE current_season = 2021
        AND end_season = 2021
),
    historical_scd AS (
        SELECT
            player_name,
            scoring_class,
            is_active,
            start_season,
            end_season
        FROM players_scd
        WHERE current_season = 2021
            AND end_season < 2021
    ),
    this_season_data AS (
        SELECT * FROM players
        WHERE current_season = 2022
    ),
    unchanged_records AS (
        SELECT ts.player_name,
            ts.scoring_class,
            ts.is_active 
            ls.start_session,
            ts.current_season AS end_season 
        FROM this_season_data ts
            JOIN last_season_scd ls
                ON ts.player_name = ls.player_name
        WHERE ts.scoring_class = ls.scoring_class
            AND ts.is_active = ls.is_active
    ),
    changed_records AS (
        SELECT ts.player_name,
            UNNEST(
                ARRAY[
                    ROW(
                        ls.scoring_class,
                        ls.is_active,
                        ls.start_season,
                        ls.end_season
                    )::scd_type,
                    ROW(
                        ts.scoring_class,
                        ts.is_active,
                        ts.current_season,
                        ts.current_season
                    )::scd_type
                ]
            )                       
        FROM this_season_data ts
            LEFT JOIN last_season_scd ls
                ON ts.player_name = ls.player_name
        WHERE (ts.scoring_class <> ls.scoring_class
            OR ts.is_active <> ls.is_active
            # OR ls.player_name IS NULL   # New players
        )        
    ),
    unnested_changed_records AS (
        SELECT player_name,
            (records::scd_type).scoring_class
            (records::scd_type).is_active
            (records::scd_type).start_season
            (records::scd_type).end_season
        FROM changed_records
    ),
    new_records AS (
        SELECT
             ts.player_name,
             ts.scoring_class,
             ts.is_active,
             ts.current_season AS start_season,
             ts.current_season AS end_season,
        FROM this_season_data ts
        LEFT JOIN last_season_scd ls
            ON ts.player_name = ls.player_name
        WHERE ls.player_name IS NULL
    )

SELECT * FROM historical_scd
UNION ALL
SELECT * FROM unchanged_records
UNION ALL
SELECT * FROM unnested_changed_records
UNION ALL
SELECT * FROM new_records;
```

We use `UNNEST(ARRAY[])` so that we can create 2 rows from an array in result.
Problems with this query:
- Has to be sequential.
- is_active & scoring_class can not be null
