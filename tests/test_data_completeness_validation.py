from src.data_completeness_validation import Validation_SourceToStage, Validation_StageToTarget

def test_datacompleteness_StoS_validation(source_db, stage_db,report_helper):
    cv = Validation_SourceToStage()
    cv.run(source_db, stage_db,report_helper)

def test_datacompleteness_StoT_validation(stage_db, target_db,report_helper):
    cv = Validation_StageToTarget()
    cv.run(stage_db, target_db,report_helper)