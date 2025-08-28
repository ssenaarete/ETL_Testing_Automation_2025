from src.scd_validation_cross_env import SCD_Validation_SourceToStage, SCD_Validation_StageToTarget

def test_scd_StoS_validation(source_db, stage_db, report_helper):
    validator = SCD_Validation_SourceToStage()
    validator.run(source_db, stage_db, report_helper)

def test_scd_StoT_validation(stage_db, target_db, report_helper):
    validator = SCD_Validation_StageToTarget()
    validator.run(stage_db, target_db, report_helper)