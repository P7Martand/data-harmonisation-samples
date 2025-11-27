{{
  config({    
    "materialized": "table",
    "alias": "best_bowlers",
    "database": "qa_team",
    "schema": "prakhartargets"
  })
}}

WITH combined_player_data AS (

  SELECT *
  
  FROM {{ ref('ref1__combined_player_data')}}

),

batsman_players AS (

  {#Selects players categorized as batsmen for focused analysis or reporting.#}
  SELECT * 
  
  FROM combined_player_data AS in0
  
  WHERE category == 'Batsman'

),

batsman_rankings_with_average AS (

  {#Ranks batsmen globally based on total scores and calculates their average performance per match.#}
  SELECT 
    *,
    ROUND(total_scores::FLOAT / NULLIF(total_matches, 0), 2) AS average,
    ROW_NUMBER() OVER (ORDER BY total_scores DESC) AS world_ranking
  
  FROM batsman_players AS in0

),

active_batsman_rankings AS (

  {#Lists batsmen with non-zero scores for ranking analysis.#}
  SELECT * 
  
  FROM batsman_rankings_with_average AS in0
  
  WHERE total_scores > 0

),

ranked_batsmen AS (

  {#Ranks active batsmen by their total scores, highlighting top performers.#}
  SELECT * 
  
  FROM active_batsman_rankings AS in0
  
  ORDER BY total_scores DESC

)

SELECT *

FROM ranked_batsmen
