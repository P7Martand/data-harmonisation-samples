Schedule = Schedule(cron = "* 0 2 * * * *", timezone = "GMT", emails = ["email@gmail.com"], enabled = False)
SensorSchedule = SensorSchedule(enabled = False)

with DAG(Schedule = Schedule, SensorSchedule = SensorSchedule):
    ref1__best_batsman_1 = Task(
        task_id = "ref1__best_batsman_1", 
        component = "Model", 
        modelName = "ref1__best_batsman_1"
    )
    ref1__combined_player_data = Task(
        task_id = "ref1__combined_player_data", 
        component = "Model", 
        modelName = "ref1__combined_player_data"
    )
    ref1__best_bowlers = Task(task_id = "ref1__best_bowlers", component = "Model", modelName = "ref1__best_bowlers")
    ref1__combined_player_data.out_0 >> [ref1__best_batsman_1.in_0, ref1__best_bowlers.in_0]
