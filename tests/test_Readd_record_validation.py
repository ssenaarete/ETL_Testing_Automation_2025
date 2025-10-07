from src.readd_record_validation import ReAddedRecords_Validation

def test_ReaddRecords_validation(config_loader):
    validator = ReAddedRecords_Validation(config_loader)
    validator.run()