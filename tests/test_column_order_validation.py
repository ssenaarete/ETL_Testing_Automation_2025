from src.Check_column_order import ColumnNameValidation

def test_check_column_order_validation(source_db, target_db, report_helper):
    validator = ColumnNameValidation()
    validator.run(source_db, target_db, report_helper)