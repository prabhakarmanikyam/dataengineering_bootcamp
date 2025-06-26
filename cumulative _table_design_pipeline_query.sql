select * from actor_films;

drop type films;
create type films as(
film TEXT,
votes INTEGER,
rating REAL,
filmid TEXT
)


create type quality_class as enum('star','good','average','bad');

 CREATE TABLE actors (
    actor TEXT,
    actorid TEXT,
  films films[],
     quality_class quality_class,
    is_active BOOLEAN,
    this_year INT,
   PRIMARY KEY (actorid, this_year)
 );


select min(year) from actor_films;

WITH this_year as (
select actor,
	   actorid,
	   year,
	   ARRAY_AGG(ROW(film, votes, rating, filmid)::films) AS film_array,
	   AVG(rating) as avg_rating
	   from actor_films
	   where year = 1971
	   group by actor, actorid, year
), last_year as(
select * from actors where this_year = 1970
)
INSERT into actors
     select 
	 coalesce(ly.actor,ty.actor) as actor,
	 coalesce(ly.actorid,ty.actorid) as actorid,
	 coalesce(ly.films,ARRAY[]::films[]) || coalesce(ty.film_array,ARRAY[]::films[]) as films,
	 CASE 
	 WHEN ty.year is not null then 
	 (case when ty.avg_rating > 8.0 then 'star'
	       when ty.avg_rating > 7.0 then 'good'
		   when ty.avg_rating > 6.0 then 'average'
		   else 'bad' end)::quality_class
		   else ly.quality_class
		   end as quality_class,
	ty.year is not null as is_active,
	coalesce(ty.year,ly.this_year+1) as year

	FROM last_year ly
	FULL OUTER JOIN this_year ty
	ON   ly.actorid = ty.actorid 
	 
  
select * from actors;
