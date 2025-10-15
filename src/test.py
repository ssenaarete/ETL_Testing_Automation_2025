import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class CountValidation:
    def __init__(self, config_loader):
        """
        Initialize CountValidation with shared config_loader.
        This ensures consistent access to DB connections, config, and report helper.
        """
        self.config_loader = config_loader
        self.db = config_loader.db
        self.report_helper = config_loader.report_helper

        # Load Excel sheet from config
        try:
            excel_file_path = config_loader.config.get("PATHS", "excel_file_path")
            self.excel_df = pd.read_excel(
                excel_file_path,
                sheet_name="Table_Mapping",   # ✅ Count check sheet
                engine="openpyxl"
            )
            logging.info(f"✅ Loaded Excel file successfully: {excel_file_path}")
        except Exception as e:
            logging.error(f"❌ Could not load 'Table_Mapping' sheet: {e}")
            self.excel_df = pd.DataFrame()  # fallback to empty df

    def run(self):
        """
        Performs count validation across Source → Stage → Target tables.
        If stage_table is blank, bypass stage validation.
        Also logs mismatched records in a separate sheet.
        """
        if self.excel_df.empty:
            logging.warning("⚠️ Skipping Count Validation as Excel sheet is empty or missing.")
            return

        df = self.excel_df
        logging.info(f"Excel Columns Found: {df.columns.tolist()}")
        print("DEBUG: Columns in Excel →", df.columns.tolist())

        results = []
        mismatch_records = []   # ✅ new list to store mismatched table info

        def normalize(val):
            """Helper to extract scalar value from nested tuples/lists."""
            if isinstance(val, list) and val and isinstance(val[0], (tuple, list)):
                return val[0][0]
            elif isinstance(val, (list, tuple)) and val:
                return val[0]
            return val

        for _, row in df.iterrows():
            source_table = row.get("source_table")
            stage_table = row.get("stage_table")
            target_table = row.get("target_table")

            # ✅ Check if Stage table is blank or NaN
            stage_exists = pd.notna(stage_table) and str(stage_table).strip() != ""

            if stage_exists:
                logging.info(f"🔍 Validating counts for: {source_table} ↔ {stage_table} ↔ {target_table}")
            else:
                logging.info(f"🔍 Validating counts for: {source_table} ↔ {target_table} (Stage skipped)")

            try:
                # Build count queries
                queries = {
                    "Source": f"SELECT COUNT(*) FROM SOURCE_DB.DBO.{source_table}",
                    "Target": f"SELECT COUNT(*) FROM TARGET_DB.DBO.{target_table}"
                }

                if stage_exists:
                    queries["Stage"] = f"SELECT COUNT(*) FROM STAGE_DB.DBO.{stage_table}"

                # Execute and normalize
                counts = {
                    "Source": normalize(self.db.execute_query(queries["Source"])),
                    "Target": normalize(self.db.execute_query(queries["Target"]))
                }

                if stage_exists:
                    counts["Stage"] = normalize(self.db.execute_query(queries["Stage"]))
                else:
                    counts["Stage"] = "N/A"

                # ✅ Status logic changes when stage is missing
                if stage_exists:
                    status = (
                        "PASS" if (counts["Source"] == counts["Stage"] == counts["Target"])
                        else "FAIL"
                    )
                else:
                    status = (
                        "PASS" if counts["Source"] == counts["Target"]
                        else "FAIL"
                    )

                results.append({
                    "Source_Table": source_table,
                    "Source_Count": counts["Source"],
                    "Stage_Table": stage_table if stage_exists else "N/A",
                    "Stage_Count": counts["Stage"],
                    "Target_Table": target_table,
                    "Target_Count": counts["Target"],
                    "Status": status
                })

                # ✅ Capture mismatches for extra sheet
                if status == "FAIL":
                    mismatch_records.append({
                        "Source_Table": source_table,
                        "Source_Count": counts["Source"],
                        "Stage_Table": stage_table if stage_exists else "N/A",
                        "Stage_Count": counts["Stage"],
                        "Target_Table": target_table,
                        "Target_Count": counts["Target"],
                    })

                # Logging summary
                log_msg = (
                    f"{source_table}({counts['Source']}) "
                    + (f"↔ {stage_table}({counts['Stage']}) " if stage_exists else "")
                    + f"↔ {target_table}({counts['Target']}) → {status}"
                )

                if status == "PASS":
                    logging.info(f"✅ {log_msg}")
                else:
                    logging.warning(f"❌ {log_msg}")

            except Exception as e:
                logging.error(f"⚠️ Error validating {source_table}, {stage_table}, {target_table}: {e}")
                results.append({
                    "Source_Table": source_table, "Source_Count": None,
                    "Stage_Table": stage_table, "Stage_Count": None,
                    "Target_Table": target_table, "Target_Count": None,
                    "Status": "ERROR"
                })

        # ✅ Save the main report
        report_file = self.report_helper.save_report(results, test_type="Count_Check")

        # # ✅ Save mismatch sheet if any
        # if mismatch_records:
        #     mismatch_df = pd.DataFrame(mismatch_records)
        #     with pd.ExcelWriter(report_file, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        #         mismatch_df.to_excel(writer, sheet_name="Count_Mismatches", index=False)
        #     logging.info(f"📄 Mismatch details written to 'Count_Mismatches' sheet in {report_file}")

        # ✅ Fail only at the end
        failed = [r for r in results if r["Status"] != "PASS"]
        assert not failed, f"❌ Row count mismatches or errors found. See 'Count_Mismatches' sheet in {report_file}"