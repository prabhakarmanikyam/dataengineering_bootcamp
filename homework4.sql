-- state change tracking for players
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
