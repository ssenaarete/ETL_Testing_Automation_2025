from src.deleted_vs_source_validation import DeletedVsSource_Validation

def test_DeletedVSSource_validation(config_loader):
    validator = DeletedVsSource_Validation(config_loader)
    validator.run()