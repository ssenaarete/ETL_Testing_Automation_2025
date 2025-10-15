from src.count_validation import CountValidation

def test_count_validation(config_loader):
    cv = CountValidation(config_loader)
    cv.run()