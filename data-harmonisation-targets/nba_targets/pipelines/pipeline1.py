Schedule = Schedule(cron = "* 0 2 * * * *", timezone = "GMT", emails = ["email@gmail.com"], enabled = False)
SensorSchedule = SensorSchedule(enabled = False)

with DAG(Schedule = Schedule, SensorSchedule = SensorSchedule):
    cdm_player_brand_engagement = Task(
        task_id = "cdm_player_brand_engagement", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {
          "name": "cdm_player_brand_engagement", 
          "sourceType": "Table", 
          "sourceName": "qa_team_prakhartargets", 
          "alias": ""
        }
    )
    cdm_player_financial_snapshot = Task(
        task_id = "cdm_player_financial_snapshot", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {
          "name": "cdm_player_financial_snapshot", 
          "sourceType": "Table", 
          "sourceName": "qa_team_prakhartargets", 
          "alias": ""
        }
    )
    cdm_player_master = Task(
        task_id = "cdm_player_master", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {"name" : "cdm_player_master", "sourceType" : "Table", "sourceName" : "qa_team_prakhartargets", "alias" : ""}
    )
    cdm_player_performance_aggregate = Task(
        task_id = "cdm_player_performance_aggregate", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {
          "name": "cdm_player_performance_aggregate", 
          "sourceType": "Table", 
          "sourceName": "qa_team_prakhartargets", 
          "alias": ""
        }
    )
