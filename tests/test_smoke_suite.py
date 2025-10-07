import os
import logging
import pytest
import pandas as pd
import configparser

from src.count_validation import CountValidation
from src.data_completeness_validation import Validation_SourceToStage, Validation_StageToTarget
from src.datatype_constraints_Cross_ENV_validation import DC_Validation_SourceToStage, DC_Validation_SourceToTarget
from src.Datatype_constraint_validation import DataTypeValidation
from src.date_field_validation import DateFieldValidation
from src.duplicate_validation import DuplicateValidation
from src.garbage_value_validation import GarbageValueValidation
from src.null_validation import NullValidation
from src.other_validation import OtherValidation
from src.scd_metadata_field_validation import SCDAuditValidation
from src.scd_validation_cross_env import SCD_Validation_SourceToStage, SCD_Validation_StageToTarget
from src.transformation_validation import TransformationValidation
from src.Referential_Integrity_validation import ReferentialIntegrity_Validation
from src.Check_column_order import ColumnNameValidation
from src.data_precision_validation import DataPrecisionValidation
from src.exclusion_etl_batch_columns_in_views import ExclusionETLBatchColumnsInViews
from src.Job_Run_validation import JobExecutionValidation
from src.ETL_log_table_validations import ETLLog_Validation
from src.ETLProcess_vs_Details_log_validation import Process_vs_Detail_log_Validation
from src.deleted_vs_source_validation import DeletedVsSource_Validation
from src.deleted_vs_target_validation import DeletedVsTarget_Validation
from src.readd_record_validation import ReAddedRecords_Validation

log = logging.getLogger(__name__)

# --- Load Excel once ---------------------------------------------------------

# 🔹 Load path from config.ini
config = configparser.ConfigParser()
config.read("config.ini")

try:
    EXCEL_PATH = config.get("PATHS", "excel_file_path")
except Exception as e:
    log.error(f"Excel path not found in config.ini: {e}")

SHEET_NAME = "Smoke_Suite_Test_cases"

def _load_run_flags():
    """Load the test run flags from Excel once. Returns dict {normalized_test_name: 'Y'/'N'}."""
    try:
        df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)
        # return dict(zip(df["Test Cases"].str.strip(), df["Run (Y/N)"].str.strip().str.upper()))
                # Normalize: lowercase, strip spaces, replace spaces with underscores
        df["Test Cases"] = (
            df["Test Cases"].astype(str).str.strip().str.lower().str.replace(" ", "_")
        )
        df["Run (Y/N)"] = df["Run (Y/N)"].astype(str).str.strip().str.upper()

        return dict(zip(df["Test Cases"], df["Run (Y/N)"]))
    
    except FileNotFoundError:
        log.error(f"Excel not found: {EXCEL_PATH}. All tests will be skipped.")
        return {}
    except ValueError:
        log.error(f"Sheet '{SHEET_NAME}' not found in {EXCEL_PATH}. All tests will be skipped.")
        return {}
    
# 🔹 Load run flags once
run_flags = _load_run_flags()
print("Loaded run_flags:", run_flags)

def should_run(test_name: str) -> bool:
    """True if test is marked 'Y' in Excel, else False (defaults to N/skip)."""
    return run_flags.get(str(test_name).strip().lower(), "N") == "Y"


# --- Tests -------------------------------------------------------------------
@pytest.mark.skipif(not should_run("count_validation"), reason="Marked N in Excel")
def test_count_validation(source_db, stage_db, target_db,report_helper):
    cv = CountValidation()
    cv.run(source_db, stage_db, target_db,report_helper)

@pytest.mark.skipif(not should_run("data_completeness_validation_SourceToStage"), reason="Marked N in Excel")
def test_datacompleteness_StoS_validation(source_db, stage_db,report_helper):
    cv = Validation_SourceToStage()
    cv.run(source_db, stage_db,report_helper)

@pytest.mark.skipif(not should_run("data_completeness_validation_StageToTarget"), reason="Marked N in Excel")   
def test_datacompleteness_StoT_validation(stage_db, target_db,report_helper):
    cv = Validation_StageToTarget()
    cv.run(stage_db, target_db,report_helper)

@pytest.mark.skipif(not should_run("datatype_constraints_Cross_ENV_validation_SourceToStage"), reason="Marked N in Excel")
def test_dc_SourceToStage_Validation(source_db, stage_db, report_helper):
    validator = DC_Validation_SourceToStage()
    validator.run(source_db, stage_db, report_helper)

@pytest.mark.skipif(not should_run("datatype_constraints_Cross_ENV_validation_SourceToTarget"), reason="Marked N in Excel")
def test_dc_SourceToTarget_Validation(source_db, target_db, report_helper):
    validator = DC_Validation_SourceToTarget()
    validator.run(source_db, target_db, report_helper)

@pytest.mark.skipif(not should_run("datatype_constraints_validation"), reason="Marked N in Excel")
def test_Datatype_constraint_validation(config_loader):
    validator = DataTypeValidation(config_loader)
    validator.run()

@pytest.mark.skipif(not should_run("Referential_Integrity_validation"), reason="Marked N in Excel")
@pytest.mark.not_for_source
def test_ReferentialIntegrity_validation(config_loader):
    validator = ReferentialIntegrity_Validation(config_loader)
    validator.run()

@pytest.mark.skipif(not should_run("date_field_validation"), reason="Marked N in Excel")
def test_Date_field_validation(config_loader):
    validator = DateFieldValidation(config_loader)
    validator.run()

@pytest.mark.skipif(not should_run("duplicate_validation"), reason="Marked N in Excel")
def test_Duplicate_validation(config_loader, db_name):
    validator = DuplicateValidation(config_loader, db_name)
    validator.run()

@pytest.mark.skipif(not should_run("garbage_value_validation"), reason="Marked N in Excel")
def test_Garbage_value_validation(config_loader):
    validator = GarbageValueValidation(config_loader)
    validator.run()

@pytest.mark.skipif(not should_run("null_validation"), reason="Marked N in Excel")
def test_null_validation(config_loader):
    validator = NullValidation(config_loader)
    validator.run()

@pytest.mark.skipif(not should_run("other_validation"), reason="Marked N in Excel")
def test_Other_validation(config_loader):
    validator = OtherValidation(config_loader)
    validator.run()

@pytest.mark.skipif(not should_run("scd_metadata_field_validation"), reason="Marked N in Excel")
@pytest.mark.not_for_source
def test_scd_metadata_field_validation(config_loader):
    validator = SCDAuditValidation(config_loader)
    validator.run()

@pytest.mark.skipif(not should_run("scd_validation_cross_env_SourceToStage"), reason="Marked N in Excel")
def test_scd_StoS_validation(source_db, stage_db, report_helper):
    validator = SCD_Validation_SourceToStage()
    validator.run(source_db, stage_db, report_helper)

@pytest.mark.skipif(not should_run("scd_validation_cross_env_StageToTarget"), reason="Marked N in Excel")
def test_scd_StoT_validation(stage_db, target_db, report_helper):
    validator = SCD_Validation_StageToTarget()
    validator.run(stage_db, target_db, report_helper)

@pytest.mark.skipif(not should_run("transformation_validation"), reason="Marked N in Excel")
def test_transformation_sourceTotarget_validation(source_db, target_db, report_helper):
    validator = TransformationValidation()
    validator.run(source_db, target_db, report_helper)

@pytest.mark.skipif(not should_run("Check_Column_order"), reason="Marked N in Excel")
def test_check_column_order_validation(source_db, target_db, report_helper):
    validator = ColumnNameValidation()
    validator.run(source_db, target_db, report_helper)

@pytest.mark.skipif(not should_run("Data_Precision_validation"), reason="Marked N in Excel")
def test_data_precision_validation(config_loader):
    validator = DataPrecisionValidation(config_loader)
    validator.run()

@pytest.mark.skipif(not should_run("ETL_Batch_Column_Exclusion_validation"), reason="Marked N in Excel")
@pytest.mark.not_for_source
def test_exclusion_of_etl_batch_columns_in_views(config_loader):
    validator = ExclusionETLBatchColumnsInViews(config_loader)
    validator.run()

@pytest.mark.skipif(not should_run("Job_Run_validation"), reason="Marked N in Excel")
@pytest.mark.not_for_source
def test_Job_Execution_validation(config_loader):
    validator = JobExecutionValidation(config_loader)
    validator.run()

@pytest.mark.skipif(not should_run("ETL_Log_Table_validation"), reason="Marked N in Excel")
@pytest.mark.not_for_source
def test_ETLLog_Table_validation(config_loader):
    validator = ETLLog_Validation(config_loader)
    validator.run()

@pytest.mark.skipif(not should_run("ETL_Process_VS_Details_validation"), reason="Marked N in Excel")
@pytest.mark.not_for_source
def test_ETLProcess_vs_Details_log_validation(config_loader):
    validator = Process_vs_Detail_log_Validation(config_loader)
    validator.run()

@pytest.mark.skipif(not should_run("DeletedVsTarget_validation"), reason="Marked N in Excel")
@pytest.mark.not_for_source
def test_deletedVsTarget_validation(config_loader):
    validator = DeletedVsTarget_Validation(config_loader)
    validator.run()

@pytest.mark.skipif(not should_run("DeletedVsSource_validation"), reason="Marked N in Excel")
def test_DeletedVSSource_validation(config_loader):
    validator = DeletedVsSource_Validation(config_loader)
    validator.run()

@pytest.mark.skipif(not should_run("Readd_Record_validation"), reason="Marked N in Excel")
def test_ReaddRecords_validation(config_loader):
    validator = ReAddedRecords_Validation(config_loader)
    validator.run()