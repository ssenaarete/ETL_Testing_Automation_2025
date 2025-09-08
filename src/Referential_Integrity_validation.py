import logging
import pandas as pd
import configparser

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class ReferentialIntegrity_Validation:
    def __init__(self, config_loader):
        self.config_loader = config_loader
        self.db = config_loader.db     # ✅ Only check in Target DB
        self.report_helper = config_loader.report_helper

        # Get excel_file_path from config.ini via config_loader
        try:
            excel_file_path = config_loader.config.get("PATHS", "excel_file_path")
            self.excel_df = pd.read_excel(
                excel_file_path,
                sheet_name="Referential Integrity Check",
                engine="openpyxl"
            )
        except Exception as e:
            logging.error(f"❌ Could not load 'Referential Integrity Check' sheet: {e}")
            self.excel_df = pd.DataFrame()  # fallback

    def run(self, schema="dbo"):
        if self.excel_df is None or self.excel_df.empty:
            logging.error("❌ Referential Integrity Check sheet is missing or empty in Excel.")
            return

        required_cols = {"parent_table", "parent_column", "child_table", "child_column"}
        if not required_cols.issubset(set(self.excel_df.columns.str.lower())):
            logging.error(f"❌ Missing required columns in Referential Integrity Check sheet: {required_cols}")
            return

        results = []

        for _, row in self.excel_df.iterrows():
            parent_table = str(row["parent_table"]).strip()
            parent_column = str(row["parent_column"]).strip()
            child_table = str(row["child_table"]).strip()
            child_column = str(row["child_column"]).strip()

            if not parent_table or not parent_column or not child_table or not child_column:
                logging.warning("⚠ Skipping row with missing metadata")
                continue

            query = f"""
            SELECT a.{child_column}
            FROM {schema}.{child_table} a
            LEFT JOIN {schema}.{parent_table} b
              ON a.{child_column} = b.{parent_column}
            WHERE b.{parent_column} IS NULL
              AND a.{child_column} IS NOT NULL;
            """

            cursor = self.db.conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            cursor.close()

            invalid_rows = [dict(zip(columns, r)) for r in rows]
            invalid_count = len(invalid_rows)

            results.append({
                "Database": self.db.database,
                "Parent_Table": parent_table,
                "Parent_Column": parent_column,
                "Child_Table": child_table,
                "Child_Column": child_column,
                "Invalid_Count": invalid_count,
                "IsCheckPassed": "PASS" if invalid_count == 0 else "FAIL",
                "Details": invalid_rows if invalid_count > 0 else None
            })

            logging.info(f"RI Check: {child_table}.{child_column} → {parent_table}.{parent_column} | Invalid = {invalid_count}")

        # Save Report
        report_file = self.report_helper.save_report(results, test_type="Referential_Integrity_Report")

        # Excel Output
        summary_df = pd.DataFrame(results)
        with pd.ExcelWriter(report_file, engine="openpyxl", mode="w") as writer:
            summary_df.drop(columns=["Details"], errors="ignore").to_excel(writer, sheet_name="Summary", index=False)

            for r in results:
                if r.get("Invalid_Count", 0) > 0 and r.get("Details") is not None:
                    details_df = pd.DataFrame(r["Details"])
                    if not details_df.empty:
                        sheet_name = f"{r['Child_Table']}_FKCheck"[:31]
                        details_df.to_excel(writer, sheet_name=sheet_name, index=False)

        assert all(r["IsCheckPassed"] == "PASS" for r in results), "❌ Referential Integrity checks failed. See report."
