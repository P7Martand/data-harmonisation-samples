Schedule = Schedule(cron = "* 0 2 * * * *", timezone = "GMT", emails = ["email@gmail.com"], enabled = False)
SensorSchedule = SensorSchedule(enabled = False)

with DAG(Schedule = Schedule, SensorSchedule = SensorSchedule):
    player_performance_summary_1 = Task(
        task_id = "player_performance_summary_1", 
        component = "Dataset", 
        table = {"name" : "best_batsman", "sourceType" : "Table", "sourceName" : "qa_team_prakharTargets", "alias" : ""}, 
        writeOptions = {"writeMode" : "overwrite"}
    )
    player_performance_summary = Task(
        task_id = "player_performance_summary", 
        component = "Dataset", 
        table = {"name" : "best_bowlers", "sourceType" : "Table", "sourceName" : "qa_team_prakharTargets", "alias" : ""}, 
        writeOptions = {"writeMode" : "overwrite"}
    )
