from src.Datatype_constraint_validation import DataTypeValidation

def test_Datatype_constraint_validation(config_loader):
    validator = DataTypeValidation(config_loader)
    validator.run()