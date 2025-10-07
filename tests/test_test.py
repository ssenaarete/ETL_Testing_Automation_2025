from src.test import DC_Validation_Helper
from src.test import DC_Validation_SourceToStage
from src.test import DC_Validation_SourceToTarget


def test_validation(config_loader):
    validator = DC_Validation_Helper(config_loader)
    validator.run()