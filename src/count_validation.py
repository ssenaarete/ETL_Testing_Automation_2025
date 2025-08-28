import logging
import pandas as pd
import configparser

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class CountValidation:
    def __init__(self, config_path="config.ini"):
        # Load excel file path from config.ini
        config = configparser.ConfigParser()
        config.read(config_path)
        self.excel_file = config.get("PATHS", "excel_file_path")

    def run(self, source_db, stage_db, target_db, report_helper):
        # Always load "Table_Mapping" sheet from the Excel file
        df = pd.read_excel(self.excel_file, sheet_name="Table_Mapping", engine="openpyxl")

        logging.info(f"Excel Columns Found: {df.columns.tolist()}")
        print("DEBUG: Columns in Excel →", df.columns.tolist())

        results = []

        def normalize(val):
            if isinstance(val, list) and val and isinstance(val[0], (tuple, list)):
                return val[0][0]
            elif isinstance(val, (list, tuple)) and val:
                return val[0]
            return val

        for _, row in df.iterrows():
            source_table, stage_table, target_table = (
                row["source_table"], 
                row["stage_table"], 
                row["target_table"]
            )

            queries = {
                "Source": f"SELECT COUNT(*) FROM {source_table}",
                "Stage": f"SELECT COUNT(*) FROM {stage_table}",
                "Target": f"SELECT COUNT(*) FROM {target_table}"
            }

            logging.info(f"Validating counts: {source_table} ↔ {stage_table} ↔ {target_table}")

            counts = {
                "Source": normalize(source_db.execute_query(queries["Source"])),
                "Stage": normalize(stage_db.execute_query(queries["Stage"])),
                "Target": normalize(target_db.execute_query(queries["Target"])),
            }

            status = "PASS" if (counts["Source"] == counts["Stage"] == counts["Target"]) else "FAIL"

            results.append({
                "Source_Table": source_table, "Source_Count": counts["Source"],
                "Stage_Table": stage_table, "Stage_Count": counts["Stage"],
                "Target_Table": target_table, "Target_Count": counts["Target"],
                "Status": status
            })

            logging.info(
                f"{source_table}({counts['Source']}) ↔ "
                f"{stage_table}({counts['Stage']}) ↔ "
                f"{target_table}({counts['Target']}) → {status}"
            )

        # Save and assert at once
        report_helper.save_report(results, test_type="Count_Check")
        # report_helper.print_validation_report_count(results)

        failed = [r for r in results if r["Status"] == "FAIL"]
        assert not failed, f"Row count mismatches found: {failed}"
