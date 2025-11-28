{{
  config({    
    "materialized": "table",
    "alias": "cdm_player_financial_snapshot",
    "database": "qa_team",
    "schema": "prakhartargets"
  })
}}

WITH nba_brand_patnership AS (

  SELECT * 
  
  FROM {{ source('qa_team_prakhar', 'nba_brand_patnership') }}

),

partnership_yearly_values AS (

  {#Summarizes annual partnership values for each player by the year the partnership began.#}
  SELECT 
    YEAR(partnership_start_date) AS start_year,
    YEAR(partnership_start_date) AS season_year,
    player_id AS player_id,
    annual_value_usd AS annual_value_usd
  
  FROM nba_brand_patnership AS in0

),

endorsement_revenue_by_player_season AS (

  {#Summarizes total endorsement revenue and number of active deals per player for each season.#}
  SELECT 
    SUM(annual_value_usd) AS endorsement_annual_revenue_usd,
    COUNT(*) AS endorsement_total_active_deals,
    any_value(start_year) AS start_year,
    any_value(season_year) AS season_year,
    any_value(player_id) AS player_id,
    any_value(annual_value_usd) AS annual_value_usd
  
  FROM partnership_yearly_values AS in0
  
  GROUP BY 
    player_id, season_year

),

nba_contracts AS (

  SELECT * 
  
  FROM {{ source('qa_team_prakhar', 'nba_contracts') }}

),

nba_contract_financials AS (

  {#Summarizes NBA player contract financials, including annual and total salary, guaranteed amounts, and tax rates for each season and team.#}
  SELECT 
    player_id AS cdm_player_id,
    season_start_year AS season_year,
    team_id AS team_id,
    annual_salary_usd AS salary_annual_usd,
    total_value_usd AS salary_total_contract_usd,
    guaranteed_amount_usd + signing_bonus_usd + incentives_usd AS salary_guaranteed_usd,
    annual_salary_usd + signing_bonus_usd AS salary_signing_bonus_usd,
    annual_salary_usd + incentives_usd AS salary_incentives_usd,
    state_tax_rate AS effective_tax_rate,
    contract_id AS contract_id,
    current_timestamp() AS financial_snapshot_date
  
  FROM nba_contracts AS in0

),

player_contracts_with_endorsements AS (

  {#Combines player contract details with endorsement revenues for each season to provide a complete view of player earnings and deals.#}
  SELECT 
    in0.cdm_player_id AS cdm_player_id,
    in0.season_year AS season_year,
    in0.team_id AS team_id,
    in0.salary_annual_usd AS salary_annual_usd,
    in0.salary_total_contract_usd AS salary_total_contract_usd,
    in0.salary_guaranteed_usd AS salary_guaranteed_usd,
    in0.salary_signing_bonus_usd AS salary_signing_bonus_usd,
    in0.salary_incentives_usd AS salary_incentives_usd,
    in0.effective_tax_rate AS effective_tax_rate,
    in0.contract_id AS contract_id,
    in0.financial_snapshot_date AS financial_snapshot_date,
    in1.endorsement_annual_revenue_usd AS endorsement_annual_revenue_usd,
    in1.endorsement_total_active_deals AS endorsement_total_active_deals,
    in1.annual_value_usd AS annual_value_usd,
    in1.start_year AS start_year
  
  FROM nba_contract_financials AS in0
  INNER JOIN endorsement_revenue_by_player_season AS in1
     ON in0.cdm_player_id == in1.player_id and in0.season_year == in1.season_year

),

player_contract_salaries AS (

  {#Summarizes player contracts, including salaries, incentives, endorsements, and calculates total and after-tax earnings for each season and team.#}
  SELECT 
    contract_id AS cdm_financial_id,
    cdm_player_id AS cdm_player_id,
    season_year AS season_year,
    team_id AS team_id,
    salary_annual_usd AS salary_annual_usd,
    salary_total_contract_usd AS salary_total_contract_usd,
    salary_guaranteed_usd AS salary_guaranteed_usd,
    salary_signing_bonus_usd AS salary_signing_bonus_usd,
    salary_incentives_usd AS salary_incentives_usd,
    coalesce(endorsement_annual_revenue_usd, 0) AS endorsement_annual_revenue_usd,
    coalesce(endorsement_total_active_deals, 0) AS endorsement_total_active_deals,
    salary_annual_usd + salary_incentives_usd + endorsement_annual_revenue_usd AS total_compensation_usd,
    effective_tax_rate AS effective_tax_rate,
    (salary_annual_usd + salary_incentives_usd + endorsement_annual_revenue_usd)
    * (1 - effective_tax_rate) AS net_earnings_after_tax_usd,
    salary_annual_usd AS capped_room_impact_usd,
    financial_snapshot_date AS financial_snapshot_date
  
  FROM player_contracts_with_endorsements AS in0

)

SELECT *

FROM player_contract_salaries
