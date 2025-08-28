from src.transformation_validation import TransformationValidation

def test_transformation_sourceTotarget_validation(source_db, target_db, report_helper):
    validator = TransformationValidation()
    validator.run(source_db, target_db, report_helper)