from src.test import CountValidation

def test_validation(config_loader):
    validator = CountValidation(config_loader)
    validator.run()