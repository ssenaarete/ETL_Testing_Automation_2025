from src.duplicate_validation import DuplicateValidation

def test_Duplicate_validation(config_loader, db_name):
    validator = DuplicateValidation(config_loader, db_name)
    validator.run()