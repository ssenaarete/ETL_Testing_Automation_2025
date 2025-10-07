from src.data_precision_validation import DataPrecisionValidation

def test_data_precision_validation(config_loader):
    validator = DataPrecisionValidation(config_loader)
    validator.run()