{{
  config({    
    "materialized": "table",
    "alias": "best_batsman",
    "database": "qa_team",
    "schema": "prakhartargets"
  })
}}

WITH combined_player_data AS (

  SELECT *
  
  FROM {{ ref('ref1__combined_player_data')}}

),

bowler_players AS (

  {#Identifies players categorized as bowlers for focused analysis or reporting.#}
  SELECT * 
  
  FROM combined_player_data AS in0
  
  WHERE category == 'Bowler'

),

bowler_rankings_and_average AS (

  {#Ranks bowlers globally by total wickets and calculates their average wickets per match.#}
  SELECT 
    *,
    ROUND(total_wickets::FLOAT / NULLIF(total_matches, 0), 2) AS average,
    ROW_NUMBER() OVER (ORDER BY total_wickets DESC) AS world_ranking
  
  FROM bowler_players AS in0

),

active_bowler_rankings AS (

  {#Highlights bowlers with at least one wicket, focusing on active performers.#}
  SELECT * 
  
  FROM bowler_rankings_and_average AS in0
  
  WHERE total_wickets > 0

),

ranked_bowler_stats AS (

  {#Ranks active bowlers by total wickets taken, highlighting top performers.#}
  SELECT * 
  
  FROM active_bowler_rankings AS in0
  
  ORDER BY total_wickets DESC NULLS LAST

)

SELECT *

FROM ranked_bowler_stats
