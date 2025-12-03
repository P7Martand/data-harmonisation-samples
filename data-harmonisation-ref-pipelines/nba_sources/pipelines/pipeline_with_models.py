Schedule = Schedule(cron = "* 0 2 * * * *", timezone = "GMT", emails = ["email@gmail.com"], enabled = False)
SensorSchedule = SensorSchedule(enabled = False)

with DAG(Schedule = Schedule, SensorSchedule = SensorSchedule):
    Pipeline_1 = Task(
        task_id = "Pipeline_1", 
        component = "Dataset", 
        label = "Pipeline_1", 
        table = {"name" : "{{ prophecy_tmp_source('pipeline_with_models', 'Pipeline_1') }}", "sourceType" : "UnreferencedSource"}
    )
    model1_1 = Task(task_id = "model1_1", component = "Model", modelName = "model1")
    pipeline_with_models__new_cdm_brand_summary = Task(
        task_id = "pipeline_with_models__new_cdm_brand_summary", 
        component = "Model", 
        snapshotName = "pipeline_with_models__new_cdm_brand_summary", 
        modelName = ""
    )
    pipeline_with_models__player_financials_and_compensation = Task(
        task_id = "pipeline_with_models__player_financials_and_compensation", 
        component = "Model", 
        modelName = "pipeline_with_models__player_financials_and_compensation"
    )
    pipeline_with_models__basketball_player_performance = Task(
        task_id = "pipeline_with_models__basketball_player_performance", 
        component = "Model", 
        modelName = "pipeline_with_models__basketball_player_performance"
    )
    Pipeline_1 = Task(
        task_id = "Pipeline_1", 
        component = "Pipeline", 
        maxTriggers = 10000, 
        triggerCondition = "Always", 
        enableMaxTriggers = False, 
        pipelineName = "pipeline1", 
        parameters = {}
    )
    pipeline_with_models__player_profiles = Task(
        task_id = "pipeline_with_models__player_profiles", 
        component = "Model", 
        modelName = "pipeline_with_models__player_profiles"
    )
    nba_brand_patnership = Task(
        task_id = "nba_brand_patnership", 
        component = "Dataset", 
        table = {"name" : "nba_brand_patnership", "sourceType" : "Table", "sourceName" : "qa_team_prakhar", "alias" : ""}
    )
    Pipeline_1.out0 >> Pipeline_1.input_port_0
    Pipeline_1.output_port_0 >> pipeline_with_models__player_profiles.in_2
    (
        nba_brand_patnership.out
        >> [pipeline_with_models__player_financials_and_compensation.in_1,
              pipeline_with_models__new_cdm_brand_summary.in_0]
    )
    model1_1.out >> pipeline_with_models__basketball_player_performance.in_1
