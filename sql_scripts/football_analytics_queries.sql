-- sql_scripts/football_analytics_queries.sql
-- Useful SQL queries for EPL analytics after loading parquet/external tables.

-- 1. Top 10 goal scorers (player_id mapped to player_name via players table)
SELECT p.player_name, g.goals
FROM top_scorers g
JOIN players p ON g.player_id = p.player_id
ORDER BY g.goals DESC
LIMIT 10;

-- 2. Team goals per season
SELECT season, team, team_goals
FROM team_goals
ORDER BY season, team_goals DESC;

-- 3. Matches with highest total goals
SELECT m.match_id, m.home_team, m.away_team, (m.home_score + m.away_score) AS total_goals
FROM matches m
ORDER BY total_goals DESC
LIMIT 10;

-- 4. Home advantage: average home goals vs away goals
SELECT season, AVG(home_score) AS avg_home_goals, AVG(away_score) AS avg_away_goals
FROM matches
GROUP BY season
ORDER BY season;

-- 5. Player head-to-head (goals by player against a specific opponent team)
SELECT p.player_name, COUNT(*) AS goals_against
FROM events e JOIN players p ON e.player_id = p.player_id
JOIN matches m ON e.match_id = m.match_id
WHERE e.event_type = 'goal' AND m.away_team = 'Manchester United'
GROUP BY p.player_name
ORDER BY goals_against DESC
LIMIT 10;
