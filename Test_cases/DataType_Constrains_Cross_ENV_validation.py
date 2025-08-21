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


class DC_Validation_SourceToStage:
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

    def run(self):
        df = pd.read_excel(self.excel_path, sheet_name="Table_Mapping")
        results = []

        for _, row in df.iterrows():
            source_table = row["source_table"]
            stage_table = row["stage_table"]

            # Fetch metadata from Source & Stage
            src_meta = self.source_db.execute_query(f"""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{source_table}'
            """)

            stg_meta = self.stage_db.execute_query(f"""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{stage_table}'
            """)

            src_meta = {r[0]: {"DATA_TYPE": r[1], "IS_NULLABLE": r[2]} for r in src_meta}
            stg_meta = {r[0]: {"DATA_TYPE": r[1], "IS_NULLABLE": r[2]} for r in stg_meta}

            # Compare common columns
            common_cols = set(src_meta.keys()).intersection(set(stg_meta.keys()))

            for col in common_cols:
                src_type = src_meta[col]["DATA_TYPE"].upper()
                stg_type = stg_meta[col]["DATA_TYPE"].upper()
                src_const = "NULL" if src_meta[col]["IS_NULLABLE"] == "YES" else "NOT NULL"
                stg_const = "NULL" if stg_meta[col]["IS_NULLABLE"] == "YES" else "NOT NULL"

                issue_1 = "Matched" if src_type == stg_type else f"Datatype mismatch: {src_type} vs {stg_type}"
                issue_2 = "Matched" if src_const == stg_const else f"Constraint mismatch: {src_const} vs {stg_const}"

                status = "✅ PASS" if issue_1 == "Matched" and issue_2 == "Matched" else \
                         f"❌ FAIL ({'; '.join([i for i in [issue_1, issue_2] if i != 'Matched'])})"

                results.append({
                    "Database": f"{self.source_db.database} vs {self.stage_db.database}",
                    "Table_Excel": f"{source_table} vs {stage_table}",
                    "Column_Excel": col,
                    "DataType_Source": src_type,
                    "Constraint_Source": src_const,
                    "DataType_Stage": stg_type,
                    "Constraint_Stage": stg_const,
                    "Status": status
                })

        # ✅ Save & print report
        self.report_helper.save_report(results, test_type="Source_to_Stage_Check")
        self.report_helper.print_validation_report_DataType_Constraints_SourceToStage(results, check_type="Source vs Stage")

        # ✅ Generate PDF report
        report = PDFReportGenerator(config_path="config.ini", font_path="DejaVuSans.ttf")
        pdf_path = report.generate(results, check_type="Source vs Stage Datatype & Constraint Check")
        print(f"PDF report saved at: {pdf_path}")

        # Close DB connections
        self.source_db.close()
        self.stage_db.close()


class DC_Validation_SourceToTarget:
    def __init__(self, config_path="config.ini"):
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        print("Sections found:", self.config.sections())
        self.excel_path = self.config.get("PATHS", "excel_file_path")

        # ✅ Connect to Source DB
        self.source_db = DBHelper.from_config_section(config_path, "SOURCEDB")
        self.source_db.connect()

        # ✅ Connect to Target DB
        self.target_db = DBHelper.from_config_section(config_path, "TARGETDB")
        self.target_db.connect()

        self.report_helper = ReportHelper(config_path)

    def run(self):
        df = pd.read_excel(self.excel_path, sheet_name="Table_Mapping")
        results = []

        for _, row in df.iterrows():
            source_table = row["source_table"]
            target_table = row["target_table"]

            # Fetch metadata from Source & Target
            src_meta = self.source_db.execute_query(f"""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{source_table}'
            """)

            tgt_meta = self.target_db.execute_query(f"""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{target_table}'
            """)

            src_meta = {r[0]: {"DATA_TYPE": r[1], "IS_NULLABLE": r[2]} for r in src_meta}
            tgt_meta = {r[0]: {"DATA_TYPE": r[1], "IS_NULLABLE": r[2]} for r in tgt_meta}

            # Compare common columns
            common_cols = set(src_meta.keys()).intersection(set(tgt_meta.keys()))

            for col in common_cols:
                src_type = src_meta[col]["DATA_TYPE"].upper()
                tgt_type = tgt_meta[col]["DATA_TYPE"].upper()
                src_const = "NULL" if src_meta[col]["IS_NULLABLE"] == "YES" else "NOT NULL"
                tgt_const = "NULL" if tgt_meta[col]["IS_NULLABLE"] == "YES" else "NOT NULL"

                issue_1 = "Matched" if src_type == tgt_type else f"Datatype mismatch: {src_type} vs {tgt_type}"
                issue_2 = "Matched" if src_const == tgt_const else f"Constraint mismatch: {src_const} vs {tgt_const}"

                status = "✅ PASS" if issue_1 == "Matched" and issue_2 == "Matched" else \
                         f"❌ FAIL ({'; '.join([i for i in [issue_1, issue_2] if i != 'Matched'])})"

                results.append({
                    "Database": f"{self.source_db.database} vs {self.target_db.database}",
                    "Table_Excel": f"{source_table} vs {target_table}",
                    "Column_Excel": col,
                    "DataType_Source": src_type,
                    "Constraint_Source": src_const,
                    "DataType_Target": tgt_type,
                    "Constraint_Target": tgt_const,
                    "Status": status
                })

        # ✅ Save & print report
        self.report_helper.save_report(results, test_type="Source_to_Target_Check")
        self.report_helper.print_validation_report_DataType_Constraints_SourceToTarget(results, check_type="Source vs Target")

        # # ✅ Generate PDF report
        # report = PDFReportGenerator(config_path="config.ini", font_path="DejaVuSans.ttf")
        # pdf_path = report.generate(results, check_type="Source vs Target Datatype & Constraint Check")
        # print(f"PDF report saved at: {pdf_path}")

        # Close DB connections
        self.source_db.close()
        self.target_db.close()


if __name__ == "__main__":
    # Run Source vs Stage Validation
    validator_stage = DC_Validation_SourceToStage()
    validator_stage.run()

    # Run Source vs Target Validation
    validator_target = DC_Validation_SourceToTarget()
    validator_target.run()
