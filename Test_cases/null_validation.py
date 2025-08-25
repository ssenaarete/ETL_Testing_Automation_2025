import sys
import os

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class NullValidation:
    def __init__(self, config_loader):
        self.config_loader = config_loader
        self.db = config_loader.db
        self.df = config_loader.df
        self.report_helper = config_loader.report_helper

    def run(self):

        # Use the sheet loaded in __init__ based on selected DB
        df = self.df.copy()

        # Filter only Business Key = Y
        df = df[df["Business Key"].str.upper() == "Y"]


        def get_scalar(result):
            """Extract scalar from DBHelper.execute_query result"""
            return result[0][0] if result and len(result[0]) > 0 else None

       
        results = []
        # for _, row in grouped.iterrows():
        for _, row in df.iterrows():    
            table = row["table_name"]
            column = row["column_name"]

            # Get null query from excel (specific to both table & column)
            null_query_excel = df.loc[
                (df["table_name"] == table) & (df["column_name"] == column),
                "Null_Check_SQL_query"
            ].dropna().unique()

            if len(null_query_excel) > 0 and str(null_query_excel[0]).strip():
                null_query = str(null_query_excel[0]).strip()
            else:
                # Default dynamic null check query
                null_query = f"SELECT COUNT(*) as nullcount FROM {table} WHERE {column} IS NULL"

            logging.info(f"Running query for {table}.{column}")
            raw_result = self.db.execute_query(null_query)
            
            null_count = get_scalar(raw_result) or 0
            
            logging.debug(f"Raw DB result for {table}.{column}: {null_query!r}")
            
            # ✅ Correctly extract COUNT(*)
            try:
                if raw_result and isinstance(raw_result, list):
                    first_row = raw_result[0]
                    null_count = int(first_row[0]) if isinstance(first_row, (list, tuple)) else int(first_row)
                elif isinstance(raw_result, (int, float)):
                    null_count = int(raw_result)
                else:
                    null_count = 0
            except Exception as e:
                logging.error(f"Failed to parse COUNT(*) result for {table}.{column}: {raw_result!r} ({e})")
                null_count = 0
                      
            is_check_passed = (null_count == 0)
            
            results.append({
                "Database": self.db.database,
                "Table_name": table,
                "Column_names": column,
                "Null_Count": null_count,
                "IsCheckPassed": is_check_passed
            })
            logging.info(f"{table}.{column} → Null count: {null_count} → {'PASS' if is_check_passed else 'FAIL'}")
            print(f"{table}.{column} → Null Count:", null_count)

        self.report_helper.save_report(results,test_type="Null_Check")
        self.report_helper.print_validation_report_Null(results, check_type="Null_Check")
        # self.db.close()

        # Generate PDF report
        # report = PDFReportGenerator(config_path="config.ini", font_path="DejaVuSans.ttf")
        # pdf_path = report.generate(results, check_type="Null")
        # print(f"PDF report saved at: {pdf_path}")

# if __name__ == "__main__":
#     nv = NullValidation()
#     nv.run()
