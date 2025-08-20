import sys
import os
# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from tkinter import simpledialog, Tk
from tabulate import tabulate
from null_validation import NullValidation
from duplicate_validation import DuplicateValidation
from count_validation import CountValidation
from garbage_value_validation import GarbageValueValidation
from utils.config_loader import ConfigLoader
from Data_completness_validation import Validation_SourceToStage, Validation_StageToTarget
from Transformation_validation import TransformationValidation
from Date_field_validation import DateFieldValidation
from DataType_Constraint_validdation import DataTypeValidation
from DataType_Constrains_Cross_ENV_validation import DC_Validation_SourceToStage, DC_Validation_SourceToTarget
from SCD_Metadata_Field_validation import SCDAuditValidation
# from test import SCDAuditValidation

print("🔍 Starting ETL Test Automation Suite...\n")

# Create config loader only once (dialog box appears here only)
config_loader = ConfigLoader()
# config_loader_count = ConfigLoader_count()

# Running Data type and Constraints Validation
print("1--Running Data type and Constraints Validation...")
DataTypeValidation(config_loader).run()

# Running Data type and Constraints cross env Validation
print("2--Running Data type and Constraints cross env Validation...")
DC_Validation_SourceToStage("config.ini").run()

# Running Data type and Constraints cross env Validation
print("3--Running Data type and Constraints cross env Validation...")
DC_Validation_SourceToTarget("config.ini").run()

# Running Data type and Constraints cross env Validation
print("4--Running Data type and Constraints cross env Validation...")
Validation_StageToTarget("config.ini").run()

# Running Null Validation
print("5--Running Null Validation...")
NullValidation(config_loader).run()

# Running Duplicate Validation
print("6--Running Duplicate Validation...")
DuplicateValidation(config_loader).run()

# Running Date Field Validation
print("7--Running Date Field Validation...")
DateFieldValidation(config_loader).run()

# Running Count Validation
print("8--Running Count Validation...") 
CountValidation("config.ini").run()

# Running Data completeness Validation - Source to Stage
print("9--Data completeness Validation - Source to Stage...") 
Validation_SourceToStage("config.ini").run()

# Running Data completeness Validation - Stage to Target
print("10--Data completeness Validation - Stage to Target...") 
Validation_StageToTarget("config.ini").run()

# Running Transformation Validation
print("11--Running Transformation Validation...")
TransformationValidation("config.ini").run()

# Running SCD Fields Metadata Validation
print("12--SCD Fields Metadata Validation...")
try:
    SCDAuditValidation(config_loader).run()
except Exception as e:
    print(f"Error during SCD Fields Metadata Validation: {e}")


print("\n✅ ETL Test Automation Suite completed successfully!")
print("📊 Check the generated reports for details.")