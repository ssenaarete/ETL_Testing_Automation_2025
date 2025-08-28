from src.count_validation import CountValidation

def test_count_validation(source_db, stage_db, target_db,report_helper):
    cv = CountValidation()
    cv.run(source_db, stage_db, target_db,report_helper)