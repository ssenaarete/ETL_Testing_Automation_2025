from src.date_field_validation import DateFieldValidation

def test_Date_field_validation(config_loader):
    validator = DateFieldValidation(config_loader)
    validator.run()