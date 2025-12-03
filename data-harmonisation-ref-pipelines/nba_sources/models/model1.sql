WITH nba_players AS (

  SELECT * 
  
  FROM {{ source('qa_team_prakhar', 'nba_players') }}

),

all_nba_players_sorted AS (

  {#Lists all NBA players in order by their player ID.#}
  SELECT * 
  
  FROM nba_players AS in0
  
  ORDER BY player_id ASC

),

first_nba_player AS (

  {#Fetches details of the first NBA player in the records.#}
  SELECT * 
  
  FROM all_nba_players_sorted AS in0
  
  LIMIT 1

)

SELECT *

FROM first_nba_player
