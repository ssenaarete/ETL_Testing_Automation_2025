from src.deleted_vs_target_validation import DeletedVsTarget_Validation

def test_deletedVsTarget_validation(config_loader):
    validator = DeletedVsTarget_Validation(config_loader)
    validator.run()