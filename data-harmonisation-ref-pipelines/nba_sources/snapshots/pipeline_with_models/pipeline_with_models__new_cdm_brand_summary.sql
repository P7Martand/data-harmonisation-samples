{% snapshot pipeline_with_models__new_cdm_brand_summary %}
{{
  config({    
    "alias": "new_cdm_brand_summary",
    "database": "qa_team",
    "strategy": "timestamp",
    "target_schema": "prakhartargets",
    "unique_key": "cdm_player_id",
    "updated_at": "created_timestamp"
  })
}}

WITH nba_brand_patnership AS (

  SELECT *
  
  FROM {{ source('qa_team_prakhar', 'nba_brand_patnership') }}

),

brand_partnership_engagements AS (

  {#Tracks brand partnership deals with players, including engagement status, value, and duration, to assess brand collaboration impact.#}
  SELECT 
    partnership_id AS cdm_brand_engagement_id,
    player_id AS cdm_player_id,
    upper(trim(brand_name)) AS brand_name_normalized,
    brand_name AS brand_category,
    partnership_start_date AS engagement_start_date,
    partnership_end_date AS engagement_end_date,
    status AS engagement_status,
    deal_value_usd AS deal_value_total_usd,
    annual_value_usd AS deal_value_annual_usd,
    deal_type AS engagement_type,
    current_timestamp() AS created_timestamp
  
  FROM nba_brand_patnership AS in0

)

SELECT *

FROM brand_partnership_engagements

{% endsnapshot %}
