from src.other_validation import OtherValidation

def test_Other_validation(config_loader):
    validator = OtherValidation(config_loader)
    validator.run()