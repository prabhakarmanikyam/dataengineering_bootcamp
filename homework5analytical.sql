

-- player status tracking query

-- players(player_name text, season int, is_active boolean)
WITH states AS (
  SELECT
    player_name,
    season AS current_season,
    is_active AS is_active_this_season,
    LAG(is_active)  OVER (PARTITION BY player_name ORDER BY season) AS was_active_last_season,
    LAG(season)     OVER (PARTITION BY player_name ORDER BY season) AS previous_season
  FROM players
)
SELECT
  player_name,
  current_season,
  CASE
    WHEN was_active_last_season IS NULL AND is_active_this_season = TRUE  THEN 'New'
    WHEN was_active_last_season = TRUE  AND is_active_this_season = FALSE THEN 'Retired'
    WHEN was_active_last_season = FALSE AND is_active_this_season = TRUE  THEN 'Returned From Retired'
    WHEN was_active_last_season = TRUE  AND is_active_this_season = TRUE  THEN 'Continued Playing'
    WHEN was_active_last_season = FALSE AND is_active_this_season = FALSE THEN 'Stayed Retired'
  END AS state_change,
  was_active_last_season,
  is_active_this_season,
  previous_season
FROM states
ORDER BY player_name, current_season;





-- grouping sets query
WITH base AS (
  SELECT
    gd.game_id,
    gd.team_id,
    gd.team_abbreviation,
    gd.player_id,
    gd.player_name,
    gd.pts,
    g.season,
    g.home_team_id, g.visitor_team_id, g.home_team_wins
  FROM game_details gd
  JOIN games g USING (game_id)
)
WITH agg AS (SELECT
  -- label which rollup this row belongs to
  CASE
    WHEN GROUPING(player_id)=0 AND GROUPING(team_id)=0 AND GROUPING(season)=1 THEN 'player_team'
    WHEN GROUPING(player_id)=0 AND GROUPING(season)=0 AND GROUPING(team_id)=1 THEN 'player_season'
    WHEN GROUPING(player_id)=1 AND GROUPING(team_id)=0 AND GROUPING(season)=1 THEN 'team'
  END AS level,

  CASE WHEN GROUPING(player_id)=0 THEN player_id END        AS player_id,
  CASE WHEN GROUPING(player_id)=0 THEN player_name END      AS player_name,
  CASE WHEN GROUPING(team_id)=0  THEN team_id END           AS team_id,
  CASE WHEN GROUPING(team_id)=0  THEN team_abbreviation END AS team_abbreviation,
  CASE WHEN GROUPING(season)=0   THEN season END            AS season,

  SUM(pts) AS total_pts,

  /* wins only meaningful on team-only rows; count each game once per team */
  CASE
    WHEN GROUPING(player_id)=1 AND GROUPING(season)=1 AND GROUPING(team_id)=0
    THEN COUNT(DISTINCT game_id) FILTER (
           WHERE (team_id = home_team_id    AND home_team_wins = 1)
              OR (team_id = visitor_team_id AND home_team_wins = 0)
         )
  END AS wins
FROM base
GROUP BY GROUPING SETS (
  (player_id, player_name, team_id, team_abbreviation),  -- player × team
  (player_id, player_name, season),                      -- player × season
  (team_id, team_abbreviation)                           -- team only
);
)
--

SELECT player_name, team_abbreviation, total_pts
FROM agg
WHERE level = 'player_team'
ORDER BY total_pts DESC
LIMIT 1;


SELECT player_name, season, total_pts
FROM agg
WHERE level = 'player_season'
ORDER BY total_pts DESC
LIMIT 1;

SELECT team_abbreviation, wins
FROM agg
WHERE level = 'team'
ORDER BY wins DESC NULLS LAST
LIMIT 1;

-- window functions on game_details queries
WITH team_games AS (
  SELECT
    g.game_id,
    g.game_date_est,
    t.team_id,
    CASE
      WHEN t.team_id = g.home_team_id    AND g.home_team_wins = 1 THEN 1
      WHEN t.team_id = g.visitor_team_id AND g.home_team_wins = 0 THEN 1
      ELSE 0
    END AS win
  FROM games g
  CROSS JOIN LATERAL (VALUES (g.home_team_id), (g.visitor_team_id)) t(team_id)
),
seq AS (
  SELECT
    team_id, game_id, game_date_est, win,
    SUM(win) OVER (
      PARTITION BY team_id
      ORDER BY game_date_est, game_id
      ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ) AS wins_last_90
  FROM team_games
)
SELECT team_id, wins_last_90
FROM seq
ORDER BY wins_last_90 DESC, game_date_est
LIMIT 1;


WITH lb AS (
  SELECT g.game_date_est, (gd.pts > 10) AS over10
  FROM game_details gd
  JOIN games g USING (game_id)
  WHERE gd.player_name = 'LeBron James'
),
marks AS (
  SELECT
    game_date_est, over10,
    SUM(CASE WHEN over10 THEN 0 ELSE 1 END)
      OVER (ORDER BY game_date_est) AS grp
  FROM lb
),
streaks AS (
  SELECT COUNT(*) AS streak_len
  FROM marks
  WHERE over10
  GROUP BY grp
)
SELECT MAX(streak_len) AS longest_streak
FROM streaks;


WITH lb AS (
  SELECT g.game_date_est, (gd.pts > 10) AS over10
  FROM game_details gd
  JOIN games g USING (game_id)
  WHERE gd.player_name = 'LeBron James'
),
marks AS (
  SELECT
    game_date_est, over10,
    SUM(CASE WHEN over10 THEN 0 ELSE 1 END)
      OVER (ORDER BY game_date_est) AS grp
  FROM lb
),
streaks AS (
  SELECT COUNT(*) AS streak_len
  FROM marks
  WHERE over10
  GROUP BY grp
)
SELECT MAX(streak_len) AS longest_streak
FROM streaks;

