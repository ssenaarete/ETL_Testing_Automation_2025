from src.null_validation import NullValidation

def test_null_validation(config_loader):
    validator = NullValidation(config_loader)
    validator.run()