{{
  config({    
    "materialized": "incremental",
    "alias": "cdm_player_performance_aggregate",
    "database": "qa_team",
    "incremental_strategy": "append",
    "schema": "prakhartargets"
  })
}}

WITH nba_team_roaster AS (

  SELECT * 
  
  FROM {{ source('qa_team_prakhar', 'nba_team_roaster') }}

),

nba_player_performance AS (

  SELECT * 
  
  FROM {{ source('qa_team_prakhar', 'nba_player_performance') }}

),

season_years AS (

  {#Summarizes key performance statistics for basketball players by season.#}
  SELECT 
    season_year AS season_year,
    performance_id AS performance_id,
    player_id AS player_id,
    team_id AS team_id,
    games_played AS games_played,
    total_points AS total_points,
    total_assists AS total_assists,
    total_rebounds AS total_rebounds,
    field_goal_percentage AS field_goal_percentage,
    three_point_percentage AS three_point_percentage,
    free_throw_percentage AS free_throw_percentage,
    steals AS steals,
    blocks AS blocks,
    turnovers AS turnovers
  
  FROM nba_player_performance AS in0

),

season_year_cast_int AS (

  {#Standardizes season year for player performance records to support consistent sports analytics.#}
  SELECT 
    CAST(season_year AS BIGINT) AS season_year_int,
    performance_id AS performance_id,
    player_id AS player_id,
    team_id AS team_id,
    games_played AS games_played,
    total_points AS total_points,
    total_assists AS total_assists,
    total_rebounds AS total_rebounds,
    field_goal_percentage AS field_goal_percentage,
    three_point_percentage AS three_point_percentage,
    free_throw_percentage AS free_throw_percentage,
    steals AS steals,
    blocks AS blocks,
    turnovers AS turnovers
  
  FROM season_years AS in0

),

player_season_stats AS (

  {#Combines player performance and playing time for each season and team, enabling comprehensive analysis of player contributions.#}
  SELECT 
    in0.player_id AS player_id,
    in0.season_year_int AS season_year_int,
    in0.team_id AS team_id,
    in0.games_played AS games_played,
    in0.total_points AS total_points,
    in0.total_assists AS total_assists,
    in0.total_rebounds AS total_rebounds,
    in0.field_goal_percentage AS field_goal_percentage,
    in0.three_point_percentage AS three_point_percentage,
    in0.free_throw_percentage AS free_throw_percentage,
    in0.steals AS steals,
    in0.blocks AS blocks,
    in0.turnovers AS turnovers,
    in1.minutes_played AS minutes_played,
    in0.performance_id AS performance_id
  
  FROM season_year_cast_int AS in0
  INNER JOIN nba_team_roaster AS in1
     ON in0.player_id == in1.player_id
    and in0.season_year_int == in1.season_year
    and in0.team_id == in1.team_id

),

player_season_performance_stats AS (

  {#Summarizes each player's seasonal performance, including key statistics and efficiency ratings, to support player evaluation and team strategy decisions.#}
  SELECT 
    performance_id AS cdm_performance_id,
    player_id AS cdm_player_id,
    season_year_int AS season_year,
    team_id AS team_id,
    games_played AS stat_games_played,
    minutes_played AS stat_minutes_total,
    total_points AS stat_points_total,
    total_assists AS stat_assists_total,
    total_rebounds AS stat_rebounds_total,
    blocks AS stat_blocks_total,
    steals AS stat_steals_total,
    turnovers AS stat_turnovers_total,
    field_goal_percentage AS efficiency_field_goal_pct,
    three_point_percentage AS efficiency_three_point_pct,
    free_throw_percentage AS efficiency_free_throw_pct,
    field_goal_percentage * 0.5 AS efficiency_player_efficiency_rating,
    current_timestamp() AS stat_capture_date
  
  FROM player_season_stats AS in0

)

SELECT *

FROM player_season_performance_stats
