SELECT * FROM tutorial.national_teams;

ALTER TABLE tutorial.national_teams RENAME COLUMN ï»¿ID TO id;

SELECT * FROM tutorial.football_players;

SELECT
  fp.id,
  first_name,
  last_name,
  national_team_id,
  country,
  games_played
FROM football_players fp
JOIN national_team nt
ON fp.national_team_id = nt.id
ORDER BY fp.id;



WITH fp AS (
  SELECT
  id
    first_name,
    last_name,
    national_team_id,
    games_played
  FROM tutorial.football_players)
  
  select * from fp join tutorial.national_teams on fp.national_team_id= tutorial.national_teams.id
  order by id;
  
  
  