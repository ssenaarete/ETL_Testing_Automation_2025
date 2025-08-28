from src.garbage_value_validation import GarbageValueValidation

def test_Garbage_value_validation(config_loader):
    validator = GarbageValueValidation(config_loader)
    validator.run()