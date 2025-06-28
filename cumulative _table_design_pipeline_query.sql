
--To retrieve data from actor_films
select * from actor_films;

-- creating films struct 
create type films as(
film TEXT,
votes INTEGER,
rating REAL,
filmid TEXT
)

-- to create enumerated quality_class for star, good, average, bad
create type quality_class as enum('star','good','average','bad');


-- creating actors table
CREATE TABLE actors (
    actor TEXT,
    actorid TEXT,
    films films[],
    quality_class quality_class,
    is_active BOOLEAN,
    this_year INT,
    PRIMARY KEY (actorid, this_year)
);


-- cumulative table design query where we add films of an actor per year 

INSERT INTO actors
WITH years AS (
    SELECT * FROM GENERATE_SERIES(1970, 2022) AS year
), p AS (	
SELECT
        actor,
        actorid,
        MIN(year) AS first_year,
		row_number() over(partition by actor order by actorid) as row_number
    FROM actor_films
    GROUP BY actor, actorid
), actor_and_years AS (
    SELECT *
    FROM p
    JOIN years y ON p.first_year <= y.year where row_number = 1
), aggregated AS (
    SELECT
        a.actor,
        a.actorid,
        y.year,
        ARRAY_AGG(
            ROW(f.film, f.votes, f.rating, f.filmid)::films
        ) FILTER (WHERE f.film IS NOT NULL) AS films
    FROM actor_and_years a
    JOIN years y ON a.year = y.year
    LEFT JOIN actor_films f
        ON a.actor = f.actor AND a.year = f.year
    GROUP BY a.actor, a.actorid, y.year
)
SELECT
    actor,
    actorid,
    COALESCE(films, ARRAY[]::films[]) AS films,
    CASE
        WHEN CARDINALITY(COALESCE(films, ARRAY[]::films[])) = 0 THEN 'bad'
        WHEN (films[CARDINALITY(films)]::films).rating > 8.0 THEN 'star'
        WHEN (films[CARDINALITY(films)]::films).rating > 7.0 THEN 'good'
        WHEN (films[CARDINALITY(films)]::films).rating > 6.0 THEN 'average'
        ELSE 'bad'
    END::quality_class AS quality_class,
    CARDINALITY(COALESCE(films, ARRAY[]::films[])) > 0 AS is_active,
    year AS this_year
FROM aggregated;


-- to create table for slowly changin dimensions with actors_history_scd

create table actors_history_scd(
	actor text,
	quality_class quality_class,
	is_active boolean,
	start_date integer,
	end_date integer,
	this_year integer,
	primary key(actor, start_date)
)


-- inserting the actors history with slowly changing dimensions with backfill query from 2021
insert into actors_history_scd
with with_previous as(
select 	
		actor,
		quality_class,
		this_year,
		is_active,
		LAG(quality_class,1) over (partition by actor order by this_year) as previous_quality_class,
		LAG(is_active,1) over (partition by actor order by this_year) as previous_is_active
from actors where this_year <= 2021 
),
with_indicators as(
select *, 
	case when quality_class <> previous_quality_class then 1 
    when is_active <> previous_is_active then 1 
	else 0
	end as change_indicator
from with_previous
),
with_streaks as(
select * ,
		sum(change_indicator) 
				over(partition by actor order by this_year) as streak_identifier
from with_indicators
)
	select actor,
	quality_class,
	is_active,
	min(this_year) as start_date,
	max(this_year) as end_date,
	2021 as this_year
	from with_streaks
	group by actor, streak_identifier, is_active, quality_class
	order by actor, streak_identifier

--to retrieve data from actors_history_scd table
select * from actors_history_scd;


-- creating scd-type for changed records where there is change occured
create type  actors_scd_type as(
quality_class quality_class,
is_active boolean,
start_date integer,
end_date integer
)

-- Incremental fill query for actors_history_scd with slowly changing dimensions

with last_year_scd as(
select * from actors_history_scd
where this_year = 2021
),
historical_scd as(
select 	actor,
		quality_class,
		is_active,
		start_date,
		end_date
from actors_history_scd
where this_year = 2021 and this_year < 2022
),
 this_year_data as(
select * from actors where this_year = 2022
 ),
 unchanged_records as(
	select ty.actor,
		ty.quality_class,
		ty.is_active,
		ly.start_date,
		ty.this_year as end_date
	from this_year_data ty 
	 JOIN last_year_scd ly
	ON ly.actor = ty.actor 
	where ty.quality_class = ly.quality_class
	and ty.is_active = ly.is_active
 ),
 changed_records as(
	select ty.actor,
		ty.quality_class,
		ty.is_active,
		ly.start_date,
		ty.this_year as end_date,
		UNNEST(ARRAY[
			ROW(
				ly.quality_class,
				ly.is_active,
				ly.start_date,
				ly.end_date
			)::actors_scd_type,
			ROW(
				ty.quality_class,
				ty.is_active,
				ty.this_year,
				ty.this_year
			)::actors_scd_type
		]) as records
	from this_year_data ty 
	 LEFT JOIN last_year_scd ly
	ON ly.actor = ty.actor 
	where (ty.quality_class <> ly.quality_class
	or ty.is_active <> ly.is_active)
 ),
 unnested_changed_records as(
select actor, 
	(records::actors_scd_type).quality_class,
	(records::actors_scd_type).is_active,
	(records::actors_scd_type).start_date,
	(records::actors_scd_type).end_date
	from changed_records
 ),
 new_records as(
	select ty.actor,
			ty.quality_class,
			ty.is_active,
			ty.this_year as start_date,
			ty.this_year as end_date
	from this_year_data ty
	LEFT JOIN last_year_scd ly
		ON ty.actor = ly.actor
		where ly.actor is null
 )

select * from historical_scd
 UNION all
select * from unchanged_records
union all
select * from unnested_changed_records
union all
select * from new_records
