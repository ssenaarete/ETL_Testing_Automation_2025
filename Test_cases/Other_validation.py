import sys
import os

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class OtherValidation:
    def __init__(self, config_loader):
        self.config_loader = config_loader
        self.db = config_loader.db
        self.df = config_loader.df
        self.report_helper = config_loader.report_helper

    def run(self):

        # Use the sheet loaded in __init__ based on selected DB
        df = self.df.copy()


        def get_scalar(result):
            """Extract scalar from DBHelper.execute_query result"""
            return result[0][0] if result and len(result[0]) > 0 else None

       
        results = []
        # for _, row in grouped.iterrows():
        for _, row in df.iterrows():    
            table = row["table_name"]
            column = row["column_name"]

            # Get null query from excel (specific to both table & column)
            other_query_excel = df.loc[
                    (df["table_name"] == table) & (df["column_name"] == column),
                    "Other_SQL_query"
                ].dropna().unique()
            

            if other_query_excel.size > 0:
                other_query_excel = other_query_excel[0]   # take the first query if multiple
            else:
                other_query_excel = f"No query found for table: {table}, column: {column}"

            logging.info(f"Running query for {table}.{column}")
            raw_result = self.db.execute_query(other_query_excel)
            
            null_count = get_scalar(raw_result) or 0
            
            logging.debug(f"Raw DB result for {table}.{column}: {other_query_excel!r}")
            
            # # ✅ Correctly extract COUNT(*)
            # try:
            #     if raw_result and isinstance(raw_result, list):
            #         first_row = raw_result[0]
            #         null_count = int(first_row[0]) if isinstance(first_row, (list, tuple)) else int(first_row)
            #     elif isinstance(raw_result, (int, float)):
            #         null_count = int(raw_result)
            #     else:
            #         null_count = 0
            # except Exception as e:
            #     logging.error(f"Failed to parse COUNT(*) result for {table}.{column}: {raw_result!r} ({e})")
            #     null_count = 0
                      
            is_check_passed = (other_query_excel == 0)
            
            results.append({
                "Database": self.db.database,
                "Table_name": table,
                "Column_names": column,
                "Result": other_query_excel,
                "IsCheckPassed": is_check_passed
            })
            logging.info(f"{table}.{column} → Null count: {null_count} → {'PASS' if is_check_passed else 'FAIL'}")
            print(f"{table}.{column} → Null Count:", null_count)

        self.report_helper.save_report(results,test_type="Null")
        self.report_helper.print_validation_report_Null(results, check_type="Null")
        # self.db.close()

        # Generate PDF report
        # report = PDFReportGenerator(config_path="config.ini", font_path="DejaVuSans.ttf")
        # pdf_path = report.generate(results, check_type="Null")
        # print(f"PDF report saved at: {pdf_path}")
