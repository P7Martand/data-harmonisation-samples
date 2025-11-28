{{
  config({    
    "materialized": "ephemeral",
    "database": "qa_team",
    "schema": "qa_orchestration"
  })
}}

WITH ind_team AS (

  SELECT * 
  
  FROM {{ source('qa_team_prakhar', 'ind_team') }}

),

eng_team AS (

  SELECT * 
  
  FROM {{ source('qa_team_prakhar', 'eng_team') }}

),

srilanka_team AS (

  SELECT * 
  
  FROM {{ source('qa_team_prakhar', 'srilanka_team') }}

),

nz_team AS (

  SELECT * 
  
  FROM {{ source('qa_team_prakhar', 'nz-team') }}

),

ind_team_1 AS (

  SELECT * 
  
  FROM {{ source('qa_team_prakhar', 'ind_team') }}

),

combined_player_data AS (

  {#Consolidates player and team performance data from multiple sources for a comprehensive overview.#}
  SELECT * 
  
  FROM eng_team AS in0
  
  UNION ALL
  
  SELECT * 
  
  FROM ind_team_1 AS in1
  
  UNION ALL
  
  SELECT * 
  
  FROM nz_team AS in2
  
  UNION ALL
  
  SELECT * 
  
  FROM ind_team AS in3
  
  UNION ALL
  
  SELECT * 
  
  FROM srilanka_team AS in4

)

SELECT *

FROM combined_player_data
