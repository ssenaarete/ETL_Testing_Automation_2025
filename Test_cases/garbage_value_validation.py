import logging
import configparser
from utils.db_helper import DBHelper
from utils.excel_helper import ExcelHelper
from utils.report_helper import ReportHelper

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class GarbageValueValidation:
    def __init__(self, config_path="config.ini"):
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.excel_path = self.config.get("PATHS", "excel_file_path")

        self.db = DBHelper.from_config_section(config_path, "SOURCEDB")
        self.db.connect()

        self.report_helper = ReportHelper(config_path)

    def run(self):
        df = ExcelHelper.read_test_cases(self.excel_path)

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
                    WHERE {column} LIKE '%[^a-zA-Z0-9]%'
                """

            logging.info(f"Running Garbage Value Check query for {table}.{column}")
            garbage_count = self.db.execute_query(garbage_check_sql)

            results.append({
                "Table": table,
                "Column": column,
                "GARBAGE_VALUE_Count": garbage_count
            })

            print(f"{table}.{column} → Garbage Value Count:", garbage_count)

        self.report_helper.save_report(results, test_type="Garbage_Value_Check")
        self.db.close()

if __name__ == "__main__":
    gv = GarbageValueValidation()
    gv.run()
