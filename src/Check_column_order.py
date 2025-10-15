import logging
import pandas as pd
import configparser

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class ColumnNameValidation:
    def __init__(self, config_path="config.ini"):
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.excel_path = self.config.get("PATHS", "excel_file_path")

    def run(self, source_db, target_db, report_helper, schema="dbo"):
        df = pd.read_excel(self.excel_path, sheet_name="Table_Mapping")

        results = []
        failed_checks = []

        for _, row in df.iterrows():
            source_table = str(row["source_table"]).strip()
            target_table = str(row["target_table"]).strip()

            if not source_table or not target_table:
                logging.warning("⚠ Skipping row with missing source/target table mapping")
                continue

            # Fetch columns from Source
            src_cols = source_db.execute_query(f"""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{source_table}'
                ORDER BY ORDINAL_POSITION
            """)

            # Fetch columns from Target
            tgt_cols = target_db.execute_query(f"""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{target_table}'
                ORDER BY ORDINAL_POSITION
            """)

            src_cols = [r[0] for r in src_cols] if src_cols else []
            tgt_cols = [r[0] for r in tgt_cols] if tgt_cols else []

            # Compare (case-insensitive)
            missing_in_target = [c for c in src_cols if c.lower() not in [t.lower() for t in tgt_cols]]
            missing_in_source = [c for c in tgt_cols if c.lower() not in [s.lower() for s in src_cols]]

            invalid_count = len(missing_in_target) + len(missing_in_source)

            details = pd.DataFrame({
                "Missing_in_Source_against_Target": [", ".join(missing_in_source)] if missing_in_source else [""],
                "Missing_in_Target_against_Source": [", ".join(missing_in_target)] if missing_in_target else [""],
                "Source_Columns": [", ".join(src_cols)],
                "Target_Columns": [", ".join(tgt_cols)]
            })

            status = "✅ PASS" if invalid_count == 0 else "❌ FAIL"

            results.append({
                "Source_Table": source_table,
                "Target_Table": target_table,
                "Invalid_Count": invalid_count,
                "Status": status,
                "Details": details if invalid_count > 0 else None
            })

            if invalid_count > 0:
                failed_checks.append(f"Mismatch in {source_table} vs {target_table}")

            logging.info(
                f"Column Name Check {source_table} ↔ {target_table} | "
                f"Invalid={invalid_count} | Status={status}"
            )

        # ✅ Save report
        report_file = report_helper.save_report(results, test_type="Column_Name_Check")

        # ✅ Excel Output (summary + details per mapping)
        summary_df = pd.DataFrame(results).drop(columns=["Details"], errors="ignore")
        with pd.ExcelWriter(report_file, engine="openpyxl", mode="w") as writer:
            summary_df.to_excel(writer, sheet_name="Summary", index=False)

            for r in results:
                if r.get("Invalid_Count", 0) > 0 and r.get("Details") is not None:
                    details_df = r["Details"]
                    if not details_df.empty:
                        sheet_name = f"{r['Source_Table']}_ColCheck"[:31]
                        details_df.to_excel(writer, sheet_name=sheet_name, index=False)

        # ✅ Fail test at the end
        assert not failed_checks, "\n".join(failed_checks)
