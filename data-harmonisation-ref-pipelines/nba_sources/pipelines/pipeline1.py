Schedule = Schedule(cron = "* 0 2 * * * *", timezone = "GMT", emails = ["email@gmail.com"], enabled = False)
SensorSchedule = SensorSchedule(enabled = False)

with DAG(Schedule = Schedule, SensorSchedule = SensorSchedule):
    pipeline1__player_financials_and_compensation = Task(
        task_id = "pipeline1__player_financials_and_compensation", 
        component = "Model", 
        modelName = "pipeline1__player_financials_and_compensation"
    )
    nba_brand_patnership = Task(
        task_id = "nba_brand_patnership", 
        component = "Dataset", 
        table = {"name" : "nba_brand_patnership", "sourceType" : "Table", "sourceName" : "qa_team_prakhar", "alias" : ""}
    )
    pipeline1__basketball_player_performance = Task(
        task_id = "pipeline1__basketball_player_performance", 
        component = "Model", 
        modelName = "pipeline1__basketball_player_performance"
    )
    pipeline1__new_cdm_brand_summary = Task(
        task_id = "pipeline1__new_cdm_brand_summary", 
        component = "Model", 
        snapshotName = "pipeline1__new_cdm_brand_summary", 
        modelName = ""
    )
    pipeline1__player_profiles = Task(
        task_id = "pipeline1__player_profiles", 
        component = "Model", 
        modelName = "pipeline1__player_profiles"
    )
    (
        nba_brand_patnership.out
        >> [pipeline1__player_financials_and_compensation.in_1, pipeline1__new_cdm_brand_summary.in_0]
    )
