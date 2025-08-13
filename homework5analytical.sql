

-- player status tracking query

create table player_growth_accounting(
player_name text,
current_season INT,
state_change text,
was_active_last_season boolean,
is_active_this_season boolean,
previous_season int,
primary key(player_name, current_season)
)



Insert into player_growth_accounting
With previous_season as(
 	select player_name,
	 current_season as season,
	 is_active
	 FROM players
	 where current_season = 1996
),
 current_season as(
		select player_name,
	 current_season as season,
	 is_active
	 FROM players
	 where current_season = 1997
 )
 select 
    coalesce(c.player_name,p.player_name) as player_name,
	coalesce(c.season,p.season+1) as current_season,
	case 
	 	 when p.player_name is null and c.player_name is not null then 'New'
		  when p.is_active = 'true' and c.is_active = 'false' THEN 'Retired'
		 when p.is_active = 'false' and c.is_active = 'true' THEN 'Returned From Retired'
		 when p.is_active = 'true' and c.is_active = 'true' THEN 'Continued Playing'
		 when p.is_active = 'false' and c.is_active = 'false' THEN 'Stayed Retired'
		 Else 'Unknown'
		 END as state_change,
		 p.is_active as was_active_last_season,
		 c.is_active as is_active_this_season,
		 p.season as previous_season
         FROM current_season c 
		 FULL OUTER JOIN previous_season p
		 ON c.player_name = p.player_name
		 ORDER BY player_name


select * from player_growth_accounting



-- grouping sets query
WITH combined AS (
  SELECT
    gd.game_id,
    gd.team_id,
    gd.team_abbreviation,
    gd.player_id,
    gd.player_name,
    gd.pts,
    g.season,
    g.home_team_id,
    g.visitor_team_id,
    g.home_team_wins
  FROM games g
  JOIN game_details gd
    ON g.game_id = gd.game_id 
)
SELECT
  player_id,
  team_id,
  season,
  SUM(pts) AS score,
     COUNT(
    CASE 
        WHEN team_id = home_team_id AND home_team_wins = 1 THEN 1
        WHEN team_id = visitor_team_id AND home_team_wins = 0 THEN 1
    END
) AS wins

FROM combined
WHERE season is NOT NULL 
GROUP BY GROUPING SETS (
   (player_id,team_id,season) -- player × team
);

-- window functions on game_details queries
