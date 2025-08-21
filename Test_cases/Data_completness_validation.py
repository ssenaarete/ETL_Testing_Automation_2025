import sys
import os

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logging
import configparser
import pandas as pd
from tkinter import simpledialog, Tk
from utils.db_helper import DBHelper
from utils.report_helper import ReportHelper
from utils.generate_pdf_report import PDFReportGenerator

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

#Source to Stage Data Completeness Validation
# This script validates data completeness between source and stage databases by checking for missing rows in both directions
class Validation_SourceToStage:
    def __init__(self, config_path="config.ini"):
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        print("Sections found:", self.config.sections())
        self.excel_path = self.config.get("PATHS", "excel_file_path")

        # ✅ Connect to Source DB
        self.source_db = DBHelper.from_config_section(config_path, "SOURCEDB")
        self.source_db.connect()

        # ✅ Connect to Stage DB
        self.stage_db = DBHelper.from_config_section(config_path, "STAGEDB")
        self.stage_db.connect()

        self.report_helper = ReportHelper(config_path)

    def get_common_columns(self, source_table, stage_table):
        """Get common column names between source and stage tables."""
        src_cols = self.source_db.execute_query(
            f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{source_table}'
            """
        )
        # src_cols = [row[0] for row in src_cols]  # Flatten to list
        print(f"Source columns for {source_table}: {src_cols}")
        src_cols = [col[0] for col in src_cols]      
        
        stg_cols = self.stage_db.execute_query(
            f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{stage_table}'
            """
        )
        print(f"Stage columns for {stage_table}: {stg_cols}")
        stg_cols = [c[0] for c in stg_cols]

        # Intersection
        common = list(set(src_cols).intersection(set(stg_cols)))

        # ✅ Quote columns to handle spaces/reserved keywords
        common_quoted = [f"[{col}]" for col in common]
        
        return ", ".join(common_quoted)

    def run(self):
        df = pd.read_excel(self.excel_path, sheet_name="Table_Mapping")

        results = []

        for _, row in df.iterrows():
            source_table = row["source_table"]
            stage_table = row["stage_table"]

            common_columns = self.get_common_columns(source_table, stage_table)
            print(f"Common columns for {source_table} ↔ {stage_table}: {common_columns}")

            if not common_columns:
                logging.warning(f"No common columns found for {source_table} ↔ {stage_table}")
                continue

            completeness_query = f"""
                SELECT COUNT(*) AS Missing_Count
                FROM (
                    SELECT {common_columns} FROM {self.config.get("SOURCEDB", "database")}.dbo.{source_table}
                    EXCEPT
                    SELECT {common_columns} FROM {self.config.get("STAGEDB", "database")}.dbo.{stage_table}
                ) AS diff
            """
                
            logging.info(f"Running completeness check: {source_table} → {stage_table}")
            raw_result = self.source_db.execute_query(completeness_query)

            missing_count = raw_result[0][0] if raw_result else 0
            is_check_passed = missing_count == 0

            results.append({
                "Source_DB": self.source_db.database,
                "Stage_DB": self.stage_db.database,
                "Source_Table": source_table,
                "Stage_Table": stage_table,
                "Common_Columns": common_columns,
                "Data_Missing_Count": missing_count,
                "IsCheckPassed": is_check_passed
            })

        # ✅ Save & print report
        self.report_helper.save_report(results, test_type="Data_Completeness_Check")
        self.report_helper.print_validation_report_Source_to_Stage(results)

        # # ✅ Generate PDF report
        # report = PDFReportGenerator(config_path="config.ini", font_path="DejaVuSans.ttf")
        # pdf_path = report.generate(results, check_type="Data Completeness Check")
        # print(f"PDF report saved at: {pdf_path}")

        # Close DB connections
        self.source_db.close()
        self.stage_db.close()


if __name__ == "__main__":
    validator = Validation_SourceToStage()
    validator.run()


#Stage to Target Data Completeness Validation
# This script validates data completeness between stage and target databases by checking for missing rows in both directions
class Validation_StageToTarget:
    def __init__(self, config_path="config.ini"):
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        print("Sections found:", self.config.sections())
        self.excel_path = self.config.get("PATHS", "excel_file_path")

        # ✅ Connect to Source DB
        self.stage_db = DBHelper.from_config_section(config_path, "STAGEDB")
        self.stage_db.connect()

        # ✅ Connect to Stage DB
        self.target_db = DBHelper.from_config_section(config_path, "TARGETDB")
        self.target_db.connect()

        self.report_helper = ReportHelper(config_path)

    def get_common_columns(self, stage_table, target_table):
        """Get common column names between source and stage tables."""
        stg_cols = self.stage_db.execute_query(
            f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{stage_table}'
            """
        )
        # src_cols = [row[0] for row in src_cols]  # Flatten to list
        print(f"Stage columns for {stage_table}: {stg_cols}")
        stg_cols = [col[0] for col in stg_cols]      
        
        trg_cols = self.target_db.execute_query(
            f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{target_table}'
            """
        )
        print(f"Target columns for {target_table}: {trg_cols}")
        trg_cols = [c[0] for c in trg_cols]

        # Intersection
        common = list(set(stg_cols).intersection(set(trg_cols)))

        # ✅ Quote columns to handle spaces/reserved keywords
        common_quoted = [f"[{col}]" for col in common]
        
        return ", ".join(common_quoted)

    def run(self):
        df = pd.read_excel(self.excel_path, sheet_name="Table_Mapping")

        results = []

        for _, row in df.iterrows():
            stage_table = row["stage_table"]
            target_table = row["target_table"]

            common_columns = self.get_common_columns(stage_table, target_table)
            print(f"Common columns for {stage_table} ↔ {target_table}: {common_columns}")

            if not common_columns:
                logging.warning(f"No common columns found for {stage_table} ↔ {target_table}")
                continue

            completeness_query = f"""
                SELECT COUNT(*) AS Missing_Count
                FROM (
                    SELECT {common_columns} FROM {self.config.get("STAGEDB", "database")}.dbo.{stage_table}
                    EXCEPT
                    SELECT {common_columns} FROM {self.config.get("TARGETDB", "database")}.dbo.{target_table}
                ) AS diff
            """
                

            logging.info(f"Running completeness check: {stage_table} → {target_table}")
            raw_result = self.stage_db.execute_query(completeness_query)

            missing_count = raw_result[0][0] if raw_result else 0
            is_check_passed = missing_count == 0

            results.append({
                "Stage_DB": self.stage_db.database,
                "Stage_Table": stage_table,
                "Target_DB": self.target_db.database,
                "Target_Table": target_table,
                "Common_Columns": common_columns,
                "Data_Missing_Count": missing_count,
                "IsCheckPassed": is_check_passed
            })

        # ✅ Save & print report
        self.report_helper.save_report(results, test_type="Data_Completeness_Check")
        self.report_helper.print_validation_report_Stage_to_Target(results)

        # # ✅ Generate PDF report
        # report = PDFReportGenerator(config_path="config.ini", font_path="DejaVuSans.ttf")
        # pdf_path = report.generate(results, check_type="Data Completeness Check")
        # print(f"PDF report saved at: {pdf_path}")

        # Close DB connections
        self.stage_db.close()
        self.target_db.close()


if __name__ == "__main__":
    validator = Validation_StageToTarget()
    validator.run()
