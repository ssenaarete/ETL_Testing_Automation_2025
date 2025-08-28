from src.datatype_constraints_Cross_ENV_validation import DC_Validation_SourceToStage, DC_Validation_SourceToTarget

def test_dc_SourceToStage_Validation(source_db, stage_db, report_helper):
    validator = DC_Validation_SourceToStage()
    validator.run(source_db, stage_db, report_helper)

def test_dc_SourceToTarget_Validation(source_db, target_db, report_helper):
    validator = DC_Validation_SourceToTarget()
    validator.run(source_db, target_db, report_helper)