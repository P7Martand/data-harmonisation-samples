{{
  config({    
    "materialized": "incremental",
    "alias": "cdm_player_master",
    "database": "qa_team",
    "incremental_strategy": "merge",
    "schema": "prakhartargets",
    "unique_key": ["cdm_player_id"]
  })
}}

WITH nba_players AS (

  SELECT * 
  
  FROM {{ source('qa_team_prakhar', 'nba_players') }}

),

player_profile_extended AS (

  {#Creates detailed player profiles including personal details, physical attributes, and draft information for basketball players.#}
  SELECT 
    player_id AS cdm_player_id,
    player_id AS cdm_player_key,
    first_name AS first_name,
    last_name AS last_name,
    CONCAT(first_name, ' ', last_name) AS full_name,
    active_status AS player_status,
    position AS position_code,
    draft_year AS draft_info_year,
    country AS country_of_origin,
    height_cm AS physical_attributes_height_cm,
    weight_kg AS physical_attributes_weight_kg,
    CURRENT_TIMESTAMP() AS created_date,
    CURRENT_TIMESTAMP() AS updated_date,
    'qa_team_prakhar.nba_players' AS data_source
  
  FROM nba_players AS in0

),

Pipeline_1 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('pipeline_with_models', 'Pipeline_1') }}

),

player_profile_details AS (

  {#Provides detailed player profiles including personal, physical, and career information.#}
  SELECT 
    in0.cdm_player_id AS cdm_player_id,
    in0.cdm_player_key AS cdm_player_key,
    in0.first_name AS first_name,
    in0.last_name AS last_name,
    in0.full_name AS full_name,
    in0.player_status AS player_status,
    in0.position_code AS position_code,
    in0.draft_info_year AS draft_info_year,
    in0.country_of_origin AS country_of_origin,
    in0.physical_attributes_height_cm AS physical_attributes_height_cm,
    in0.physical_attributes_weight_kg AS physical_attributes_weight_kg,
    in0.created_date AS created_date,
    in0.updated_date AS updated_date,
    in0.data_source AS data_source
  
  FROM player_profile_extended AS in0
  INNER JOIN Pipeline_1 AS in1
     ON true

)

SELECT *

FROM player_profile_details

{% if is_incremental() %}
  WHERE 
    cdm_player_id != (
      SELECT MAX(cdm_player_key)
      
      FROM {{this}}
     )
{% endif %}