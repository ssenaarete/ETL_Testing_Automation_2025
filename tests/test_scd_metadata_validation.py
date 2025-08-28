import pytest
from src.scd_metadata_field_validation import SCDAuditValidation

@pytest.mark.not_for_source
def test_scd_metadata_field_validation(config_loader):
    validator = SCDAuditValidation(config_loader)
    validator.run()