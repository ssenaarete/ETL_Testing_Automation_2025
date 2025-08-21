import sys
import os

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logging
import configparser
import pandas as pd
from tkinter import simpledialog, Tk
from utils.db_helper import DBHelper
from utils.excel_helper import ExcelHelper
from utils.report_helper import ReportHelper
from utils.generate_pdf_report import PDFReportGenerator

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class DuplicateValidation:
    def __init__(self, config_loader):
        self.config_loader = config_loader
        self.db = config_loader.db
        self.df = config_loader.df
        self.report_helper = config_loader.report_helper

    def run(self):
        df = self.df.copy()

        # Filter only Business Key = Y
        df = df[df["Business Key"].str.upper() == "Y"]

        # Group by table and merge all Y columns into a list
        grouped = df.groupby("table_name")["column_name"].apply(list).reset_index()
        
        results = []

        for _, row in grouped.iterrows():
            table = row["table_name"]
            columns = row["column_name"]  
            composite_key = ", ".join(columns)
            print("DEBUG composite_key:", composite_key, type(composite_key))

            # Get duplicate query from excel (if available)
            duplicate_query_excel = df.loc[df["table_name"] == table, "Duplicate_Check_SQL_query"].dropna().unique()

            if len(duplicate_query_excel) > 0 and duplicate_query_excel[0].strip():
                duplicate_query = duplicate_query_excel[0].strip()
            else:
                # Default dynamic duplicate check query → returns duplicate groups
                duplicate_query = f"""
                    SELECT {", ".join(columns)}
                    FROM {table}
                    WHERE Is_Current = 1 OR Is_Current IN ('TRUE','True','true')
                    GROUP BY {", ".join(columns)}
                    HAVING COUNT(*) > 1
                """
                print("DEBUG duplicate_query:", duplicate_query)

            logging.info(f"Running query for {table} with key [{composite_key}]")
            raw_result = self.db.execute_query(duplicate_query)
            logging.debug(f"Raw DB result for {table}.{composite_key}: {raw_result!r}")

            # ✅ FIX: count duplicate groups correctly
            duplicate_count = len(raw_result) if raw_result else 0
            is_check_passed = (duplicate_count == 0)

            results.append({
                "Database": self.db.database,
                "Table_name": table,
                "Column_names": composite_key,   # string, not list
                "DUPLICATE_Count": duplicate_count,
                "IsCheckPassed": is_check_passed
            })

            logging.info(f"{table}.{composite_key} → Duplicate count: {duplicate_count} → {'PASS' if is_check_passed else 'FAIL'}")
            print(f"{table}.{composite_key} → Duplicate Count:", duplicate_count)

        # Save + print reports
        self.report_helper.save_report(results, test_type="Duplicate_Check")
        self.report_helper.print_validation_report_Duplicate(results, check_type="Duplicate")

        # # Generate PDF report
        # report = PDFReportGenerator(config_path="config.ini", font_path="DejaVuSans.ttf")
        # pdf_path = report.generate(results, check_type="Duplicate Check")
        # print(f"PDF report saved at: {pdf_path}")

# if __name__ == "__main__":
#     nv = DuplicateValidation()
#     nv.run()