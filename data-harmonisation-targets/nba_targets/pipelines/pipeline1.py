Schedules = [Schedule(
               Name = "Schedule 1", 
               emails = ["email@gmail.com"], 
               emailOnStart = False, 
               emailOnFailure = False, 
               emailOnSuccess = False, 
               enabled = False, 
               cron = "* 0 2 * * * *", 
               timezone = "GMT"
             )]

with DAG(schedules = Schedules):
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
    new_cdm_brand_summary = Task(
        task_id = "new_cdm_brand_summary", 
        component = "Dataset", 
        table = {"name" : "new_cdm_brand_summary", "sourceType" : "Table", "sourceName" : "qa_team_prakhartargets", "alias" : ""}
    )
    cdm_player_master = Task(
        task_id = "cdm_player_master", 
        component = "Dataset", 
        writeOptions = {"writeMode" : "overwrite"}, 
        table = {"name" : "cdm_player_master", "sourceType" : "Table", "sourceName" : "qa_team_prakhartargets", "alias" : ""}
    )
