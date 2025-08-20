import sys
import os

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logging
import pandas as pd
from openpyxl import load_workbook

from utils.db_helper import DBHelper
from utils.report_helper import ReportHelper
from utils.generate_pdf_report import PDFReportGenerator

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class SCDAuditValidation:
    def __init__(self, config_loader):
        self.config_loader = config_loader
        self.db = config_loader.db
        self.df = config_loader.df   # Excel metadata (table_name, column_name, Business Key, etc.)
        self.report_helper = config_loader.report_helper

    def get_business_keys(self, table_name):
        if self.df is None or self.df.empty:
            return []

        keys = (
            self.df[(self.df["table_name"].str.lower() == table_name.lower()) &
                    (self.df["Business Key"].str.upper() == "Y")]
            ["column_name"]
            .tolist()
        )
        return keys

    def build_business_key_expr(self, business_keys):
        if not business_keys:
            return None
        if len(business_keys) == 1:
            return business_keys[0]
        else:
            return " + '_' + ".join([f"CAST({col} AS NVARCHAR(100))" for col in business_keys])

    def run_for_table(self, table_name):
        business_keys = self.get_business_keys(table_name)

        if not business_keys:
            logging.warning(f"No Business Keys found in Excel for table: {table_name}")
            return []

        bk_expr = self.build_business_key_expr(business_keys)
        # logging.info(f"Business Key expression for {table_name}: {bk_expr}")

        queries = {
            "Version_Begin_Date Check": f"""
                SELECT * FROM {table_name}
                WHERE Version_Begin_Date IS NULL
                   OR Version_Begin_Date > Load_Timestamp;
            """,
            "Single Current Record per Business Key": f"""
                SELECT {bk_expr} AS BusinessKey, COUNT(*) AS CurrentRecordCount
                FROM {table_name}
                WHERE CAST(Is_Current AS NVARCHAR) IN ('1', 'TRUE', 'True', 'true')
                GROUP BY {bk_expr}
                HAVING COUNT(*) > 1;
            """,
            "Version_End_Date & Is_Current Consistency": f"""
                SELECT * FROM {table_name}
                WHERE (
                        (CAST(Is_Current AS NVARCHAR) IN ('1','TRUE','True','true') AND Version_End_Date IS NULL)
                    OR (CAST(Is_Current AS NVARCHAR) IN ('0','FALSE','False','false') AND Version_End_Date IS NULL)
                );
            """,
            # "Load_Timestamp Freshness (last 1 day)": f"""
            #     SELECT * FROM {table_name}
            #     WHERE Load_Timestamp < DATEADD(DAY, -1, GETDATE());
            # """,
            "Historical Version Dates Check": f"""
                SELECT * FROM {table_name}
                WHERE Is_Current = 0
                  AND Version_Begin_Date >= Version_End_Date;
            """,
            "Overlapping Versions Check": f"""
                WITH VersionedData AS (
                    SELECT 
                        {bk_expr} AS BusinessKey,
                        Version_Begin_Date,
                        ISNULL(Version_End_Date, '9999-12-31') AS Version_End_Date
                    FROM {table_name}
                )
                SELECT a.BusinessKey, 
                       a.Version_Begin_Date AS Begin_A, a.Version_End_Date AS End_A,
                       b.Version_Begin_Date AS Begin_B, b.Version_End_Date AS End_B
                FROM VersionedData a
                JOIN VersionedData b 
                  ON a.BusinessKey = b.BusinessKey
                 AND a.Version_Begin_Date < b.Version_End_Date
                 AND b.Version_Begin_Date < a.Version_End_Date
                 AND a.Version_Begin_Date <> b.Version_Begin_Date;
            """
        }

        results = []
        for check_name, query in queries.items():
            logging.info(f"Running check: {check_name} on {table_name}")

            # ✅ Use raw connection cursor to get column names
            cursor = self.db.conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            cursor.close()
            raw_result = [dict(zip(columns, row)) for row in rows]  # convert into dicts

            # raw_result = self.db.execute_query(query)
            row_count = len(raw_result) #if raw_result else 0
            is_check_passed = (row_count == 0)

            results.append({
                "Database": self.db.database,
                "Table_name": table_name,
                "Check_name": check_name,
                "Issue_Count": row_count,
                "IsCheckPassed": "PASS" if is_check_passed else "FAIL",
                "Details": raw_result if not is_check_passed else None  # ✅ add raw rows
            })

            logging.info(f"{check_name} → Issues: {row_count} → {'PASS' if is_check_passed else 'FAIL'}")

        return results

    def run(self):
        if self.df is None or self.df.empty:
            logging.error("Excel metadata is missing or empty.")
            return

        results = []
        unique_tables = self.df["table_name"].dropna().unique()

        for table in unique_tables:
            logging.info(f"🔍 Starting SCD checks for table: {table}")
            table_results = self.run_for_table(table)
            results.extend(table_results)

        if results:
            report_file = self.report_helper.save_report(results, test_type="SCD_Metadata_Validation_Report")

            summary_df = pd.DataFrame(results)
            with pd.ExcelWriter(report_file, engine="openpyxl", mode="w") as writer:
                # Summary sheet
                summary_df.drop(columns=["Details"], errors="ignore").to_excel(writer, sheet_name="Summary", index=False)

                # Write details per failed check with proper columns
                for r in results:
                    if r.get("Issue_Count", 0) > 0 and r.get("Details") is not None:
                        details_df = pd.DataFrame(r["Details"])   # ✅ now includes DB column names
                        if not details_df.empty:
                            sheet_name = f"{r['Table_name']}_{r['Check_name']}"[:31]
                            details_df.to_excel(writer, sheet_name=sheet_name, index=False)

            # Still print PDF/console report
            self.report_helper.print_validation_report_SCD_Metadata_Validation(
                results, check_type="SCD_Metadata_Validation_Report"
            )
