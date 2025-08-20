import sys
import os

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logging
import configparser
from utils.db_helper import DBHelper
import pandas as pd
from utils.report_helper import ReportHelper
from utils.generate_pdf_report import PDFReportGenerator
from utils.generate_pdf_report import PDFReportGenerator, CountCheckPDFGenerator

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class CountValidation:
    def __init__(self, config_path="config.ini"):
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.excel_path = self.config.get("PATHS", "excel_file_path")

        self.db_source = DBHelper.from_config_section(config_path, "SOURCEDB")
        self.db_stage = DBHelper.from_config_section(config_path, "STAGEDB")
        self.db_target = DBHelper.from_config_section(config_path, "TARGETDB")

        self.db_source.connect()
        self.db_stage.connect()
        self.db_target.connect()

        self.report_helper = ReportHelper(config_path)


    def run(self):
        # Read only the Table_Mapping sheet
        df = pd.read_excel(self.excel_path, sheet_name="Table_Mapping", engine="openpyxl")

        results = []
        for _, row in df.iterrows():
            source_table = row["source_table"]
            stage_table = row["stage_table"]
            target_table = row["target_table"]

            source_query = f"SELECT COUNT(*) FROM {source_table}"
            stage_query = f"SELECT COUNT(*) FROM {stage_table}"
            target_query = f"SELECT COUNT(*) FROM {target_table}"

            logging.info(f"Getting counts for Source: {source_table}, Stage: {stage_table}, Target: {target_table}")

            source_count = self.db_source.execute_query(source_query)
            stage_count = self.db_stage.execute_query(stage_query)
            target_count = self.db_target.execute_query(target_query)

            # If DBHelper returns a list like [(123,)], unpack it
            if isinstance(source_count, list) and len(source_count) > 0:
                source_count = source_count[0][0]
            if isinstance(stage_count, list) and len(stage_count) > 0:
                stage_count = stage_count[0][0]
            if isinstance(target_count, list) and len(target_count) > 0:
                target_count = target_count[0][0]

            status = "PASS" if (source_count == stage_count == target_count) else "FAIL"

            results.append({
                "Source_Table": source_table,
                "Source_Count": source_count,
                "Stage_Table": stage_table,
                "Stage_Count": stage_count,
                "Target_Table": target_table,
                "Target_Count": target_count,
                "status": status
            })

            print(f"Source: {source_table}({source_count}), Stage: {stage_table}({stage_count}), Target: {target_table}({target_count})")

        # Save results to Excel and PDF
        self.report_helper.save_report(results, test_type="Count_Check")
        self.report_helper.print_validation_report_count(results)
        
        pdf_gen = CountCheckPDFGenerator(output_path="Reports")
        pdf_path = pdf_gen.generate_count(results)
        logging.info(f"PDF report saved at: {pdf_path}")

        self.db_source.close()
        self.db_stage.close()
        self.db_target.close()

if __name__ == "__main__":
    cv = CountValidation("config.ini")
    cv.run()