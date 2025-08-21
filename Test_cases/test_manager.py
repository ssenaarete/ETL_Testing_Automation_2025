# main.py
import sys
import os
# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# test_manager.py
from Test_cases.duplicate_validation import DuplicateValidation
from Test_cases.null_validation import NullValidation
from utils.config_loader import ConfigLoader
# from Test_cases.SCD_validation_Cross_ENV import SCDValidationCrossEnv
# import other validations similarly...

config_loader = ConfigLoader()

TEST_CASES = {
    "Duplicate Validation": DuplicateValidation,
    "Null Validation": NullValidation,
    # "SCD Validation Cross ENV": SCDValidationCrossEnv,
    # Add more mappings...
}

def run_test(test_name):
    cls = TEST_CASES[test_name]
    obj = cls(config_loader)
    return obj.run()   # assuming each class has a run() method

