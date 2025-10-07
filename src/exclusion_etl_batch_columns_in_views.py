import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class ExclusionETLBatchColumnsInViews:
    """
    Validator to ensure target views/tables do NOT contain ETL batch columns.
    """

    FORBIDDEN_COLUMNS = ["Is_current", "version_begin_date", "version_end_date", "load_timestamp"]

    def __init__(self, config_loader):
        self.config_loader = config_loader
        self.db = config_loader.db  # ✅ Only check in Target DB
        self.report_helper = config_loader.report_helper

        # Get Excel file path from config.ini via config_loader
        try:
            excel_file_path = config_loader.config.get("PATHS", "excel_file_path")
            self.excel_df = pd.read_excel(
                excel_file_path,
                sheet_name="Table_Mapping",
                engine="openpyxl"
            )
            # normalize column names to lowercase
            self.excel_df.columns = [c.lower() for c in self.excel_df.columns]
        except Exception as e:
            logging.error(f"❌ Could not load 'Table_Mapping' sheet: {e}")
            self.excel_df = pd.DataFrame()  # fallback

    def run(self):
        if self.excel_df is None or self.excel_df.empty:
            logging.error("❌ Table mapping sheet is missing or empty in Excel.")
            return

        required_cols = {"target_view"}
        if not required_cols.issubset(set(self.excel_df.columns)):
            logging.error(f"❌ Missing required columns in Table mapping sheet: {required_cols}")
            return

        results = [] 

        for _, row in self.excel_df.iterrows():
            view_table = str(row["target_view"]).strip()

            if not view_table or view_table.lower() == "nan":
                logging.warning("⚠ Skipping row with missing target_view")
                continue

            try:
                query = f"""
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{view_table}'
                    ORDER BY ORDINAL_POSITION
                """
                cursor = self.db.conn.cursor()
                cursor.execute(query)
                rows = cursor.fetchall()
                col_names = [r[0] for r in rows] if rows else []
                cursor.close()

                # --- Check forbidden columns ---
                lower_cols = {c.lower(): c for c in col_names}
                present_forbidden = []
                for forbidden in self.FORBIDDEN_COLUMNS:
                    if forbidden.lower() in lower_cols:
                        present_forbidden.append({
                            "forbidden_column": forbidden,
                            "actual_column_name": lower_cols[forbidden.lower()]
                        })

                is_pass = len(present_forbidden) == 0

                results.append({
                    "Database": self.db.database,
                    "Target_View": view_table,
                    "Checked_Columns": ", ".join(col_names) if col_names else None,
                    "Forbidden_Present": ", ".join(
                        [f["actual_column_name"] for f in present_forbidden]
                            ) if present_forbidden else "No",
                    "IsCheckPassed": "PASS" if is_pass else "FAIL",
                    "Details": present_forbidden if not is_pass else None
                })

                logging.info(
                    f"Check: {view_table} | Forbidden Columns Found = {len(present_forbidden)}"
                )

            except Exception as ex:
                logging.error(f"❌ Error fetching columns for {view_table}: {ex}")
                results.append({
                    "Database": self.db.database,
                    "Target_View": view_table,
                    "Checked_Columns": None,
                    "Forbidden_Present": "No",
                    "IsCheckPassed": "ERROR",
                    "Error": str(ex)
                })
                continue

        # Save Report
        report_file = self.report_helper.save_report(results, test_type="Exclusion_ETL_Batch_Columns_In_Views")

        # Excel Output
        summary_df = pd.DataFrame(results)
        with pd.ExcelWriter(report_file, engine="openpyxl", mode="w") as writer:
            summary_df.drop(columns=["Details"], errors="ignore").to_excel(writer, sheet_name="Summary", index=False)

            for r in results:
                if r.get("IsCheckPassed") == "FAIL" and r.get("Details") is not None:
                    details_df = pd.DataFrame(r["Details"])
                    if not details_df.empty:
                        sheet_name = f"{r['Target_View']}_ForbiddenCols"[:31]
                        details_df.to_excel(writer, sheet_name=sheet_name, index=False)

        assert all(r["IsCheckPassed"] == "PASS" for r in results), (
            "❌ Forbidden columns detected in Target Views. See report.")
