import sys
import os

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class GarbageValueValidation:
    def __init__(self, config_loader):
        self.config_loader = config_loader
        self.db = config_loader.db
        self.df = config_loader.df
        self.report_helper = config_loader.report_helper

    def run(self):
        # df = ExcelHelper.read_test_cases(self.excel_path)
        df = self.df.copy()
        
        results = []
        
        for _, row in df.iterrows():
            table = row["table_name"]
            column = row["column_name"]

            # Custom SQL from Excel or default regex query to find garbage values
            garbage_check_sql = row.get("Garbage_Check_SQL_query", "").strip()

            if not garbage_check_sql:
                # This SQL uses SQL Server syntax with NOT LIKE and a pattern for allowed chars (alphanumeric)
                # It counts rows where the column contains any character NOT a-z, A-Z, or 0-9
                garbage_check_sql = f"""
                    SELECT COUNT(*) FROM {table}
                    WHERE {column} LIKE '%[^a-zA-Z0-9@. -]%'
                """

            logging.info(f"Running Garbage Value Check query for {table}.{column}")
            garbage_count = self.db.execute_query(garbage_check_sql)

            if isinstance(garbage_count, list) and len(garbage_count) > 0:
                garbage_count = garbage_count[0][0]

            status = "PASS" if (garbage_count == 0) else "FAIL"

            results.append({
                "Database": self.db.database,
                "Table": table,
                "Column": column,
                "GARBAGE_VALUE_Count": garbage_count,
                "Status": status
            })

            print(f"{table}.{column} → Garbage Value Count:", garbage_count)

        # Save results to Excel and PDF
        self.report_helper.save_report(results, test_type="Garbage_Value_Check")
        self.report_helper.print_validation_report_GarbageVlueValidation(results, check_type="Garbage_Value_Check")
        self.db.close()

# if __name__ == "__main__":
#     # Create config loader (this will ask for DB and load Excel)
#     config_loader = self.config_loader("config.ini")
#     gv = GarbageValueValidation(config_loader)
#     gv.run()
