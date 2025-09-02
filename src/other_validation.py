import logging
import pandas as pd

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
        failed_checks = []
        # for _, row in grouped.iterrows():
        for _, row in df.iterrows():    
            table = row["table_name"]
            column = row["column_name"]

            # Get SQL query from excel (specific to both table & column)
            other_query_excel = df.loc[
                    (df["table_name"] == table) & (df["column_name"] == column),
                    "Other_SQL_query"
                ].dropna().unique()
            
            issue_count = 0
            executed_query = None
            

            if other_query_excel.size > 0:
                executed_query = other_query_excel[0]   # take the first query if multiple
                logging.info(f"Running query for {table}.{column}")
                raw_result = self.db.execute_query(executed_query)
                issue_count = get_scalar(raw_result) or 0

            else:
                # other_query_excel = f"No query found for table: {table}, column: {column}"
                logging.info(f"✅ No custom SQL query found for {table}.{column}, skipping check.")
                # issue_count = 0
       
            logging.debug(f"Raw DB result for {table}.{column}: {other_query_excel!r}")
                      
            # is_check_passed = (other_query_excel == 0)
            is_check_passed = (issue_count == 0)
            
            results.append({
                "Database": self.db.database,
                "Table_name": table,
                "Column_names": column,
                "Result": issue_count,
                "IsCheckPassed": is_check_passed
            })

            if not is_check_passed:
                failed_checks.append(f"❌ Other Check failed for {table}.{column} → Issue count: {issue_count}")

            logging.info(f"{table}.{column} → Issue count: {issue_count} → {'PASS' if is_check_passed else 'FAIL'}")
            print(f"{table}.{column} → Issue Count:", issue_count)

        
        self.report_helper.save_report(results,test_type="Other_Check")

        # ✅ Fail test only at the end (after report generation)
        assert not failed_checks, "\n".join(failed_checks)