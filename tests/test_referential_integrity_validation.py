from src.Referential_Integrity_validation import ReferentialIntegrity_Validation

def test_ReferentialIntegrity_validation(config_loader):
    validator = ReferentialIntegrity_Validation(config_loader)
    validator.run()