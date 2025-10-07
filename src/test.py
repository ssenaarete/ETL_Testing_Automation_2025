import logging
import pandas as pd
import configparser

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class DC_Validation_Helper:
    @staticmethod
    def compare_metadata(src_meta, tgt_meta, source_table, target_table, source_db, target_db, results, failed_checks):
        """
        Compare columns between two environments (Source vs Stage/Target).
        Checks for:
        1. Missing columns
        2. Extra columns
        3. Data type & constraint mismatches
        """

        src_cols = set(src_meta.keys())
        tgt_cols = set(tgt_meta.keys())

        # --- Missing Columns in Target ---
        missing_in_tgt = src_cols - tgt_cols
        for col in missing_in_tgt:
            results.append({
                "Database": f"{source_db.database} vs {target_db.database}",
                "Table_Excel": f"{source_table} vs {target_table}",
                "Column_Excel": col,
                "DataType_Source": src_meta[col]["DATA_TYPE"].upper(),
                "Constraint_Source": "NULL" if src_meta[col]["IS_NULLABLE"] == "YES" else "NOT NULL",
                "DataType_Target": "❌ MISSING",
                "Constraint_Target": "❌ MISSING",
                "Status": "❌ FAIL (Column missing in Target)"
            })
            failed_checks.append(f"Column {col} in {source_table} missing in {target_table}")

        # --- Extra Columns in Target ---
        extra_in_tgt = tgt_cols - src_cols
        for col in extra_in_tgt:
            results.append({
                "Database": f"{source_db.database} vs {target_db.database}",
                "Table_Excel": f"{source_table} vs {target_table}",
                "Column_Excel": col,
                "DataType_Source": "❌ MISSING",
                "Constraint_Source": "❌ MISSING",
                "DataType_Target": tgt_meta[col]["DATA_TYPE"].upper(),
                "Constraint_Target": "NULL" if tgt_meta[col]["IS_NULLABLE"] == "YES" else "NOT NULL",
                "Status": "❌ FAIL (Extra column in Target)"
            })
            failed_checks.append(f"Extra column {col} in {target_table} not present in {source_table}")

        # --- Compare Common Columns ---
        common_cols = src_cols.intersection(tgt_cols)
        for col in common_cols:
            src_type = src_meta[col]["DATA_TYPE"].upper()
            tgt_type = tgt_meta[col]["DATA_TYPE"].upper()
            src_const = "NULL" if src_meta[col]["IS_NULLABLE"] == "YES" else "NOT NULL"
            tgt_const = "NULL" if tgt_meta[col]["IS_NULLABLE"] == "YES" else "NOT NULL"

            issue_1 = "Matched" if src_type == tgt_type else f"Datatype mismatch: {src_type} vs {tgt_type}"
            issue_2 = "Matched" if src_const == tgt_const else f"Constraint mismatch: {src_const} vs {tgt_const}"

            status = "✅ PASS" if issue_1 == "Matched" and issue_2 == "Matched" else \
                     f"❌ FAIL ({'; '.join([i for i in [issue_1, issue_2] if i != 'Matched'])})"

            results.append({
                "Database": f"{source_db.database} vs {target_db.database}",
                "Table_Excel": f"{source_table} vs {target_table}",
                "Column_Excel": col,
                "DataType_Source": src_type,
                "Constraint_Source": src_const,
                "DataType_Target": tgt_type,
                "Constraint_Target": tgt_const,
                "Status": status
            })

            if status != "✅ PASS":
                failed_checks.append(f"Issue in {source_table}.{col}: {status}")


class DC_Validation_SourceToStage:
    def __init__(self, config_path="config.ini"):
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.excel_path = self.config.get("PATHS", "excel_file_path")

    def run(self, source_db, stage_db, report_helper):
        df = pd.read_excel(self.excel_path, sheet_name="Table_Mapping")
        results = []
        failed_checks = []

        for _, row in df.iterrows():
            source_table = row["source_table"]
            stage_table = row["stage_table"]

            # Fetch metadata
            src_meta = source_db.execute_query(f"""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{source_table}'
            """)
            stg_meta = stage_db.execute_query(f"""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{stage_table}'
            """)

            src_meta = {r[0]: {"DATA_TYPE": r[1], "IS_NULLABLE": r[2]} for r in src_meta}
            stg_meta = {r[0]: {"DATA_TYPE": r[1], "IS_NULLABLE": r[2]} for r in stg_meta}

            # Compare metadata
            DC_Validation_Helper.compare_metadata(src_meta, stg_meta,
                                                  source_table, stage_table,
                                                  source_db, stage_db,
                                                  results, failed_checks)

        # Save & assert
        report_helper.save_report(results, test_type="DC_Source_to_Stage_Check")
        assert not failed_checks, "\n".join(failed_checks)


class DC_Validation_SourceToTarget:
    def __init__(self, config_path="config.ini"):
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.excel_path = self.config.get("PATHS", "excel_file_path")

    def run(self, source_db, target_db, report_helper):
        df = pd.read_excel(self.excel_path, sheet_name="Table_Mapping")
        results = []
        failed_checks = []

        for _, row in df.iterrows():
            source_table = row["source_table"]
            target_table = row["target_table"]

            # Fetch metadata
            src_meta = source_db.execute_query(f"""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{source_table}'
            """)
            tgt_meta = target_db.execute_query(f"""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{target_table}'
            """)

            src_meta = {r[0]: {"DATA_TYPE": r[1], "IS_NULLABLE": r[2]} for r in src_meta}
            tgt_meta = {r[0]: {"DATA_TYPE": r[1], "IS_NULLABLE": r[2]} for r in tgt_meta}

            # Compare metadata
            DC_Validation_Helper.compare_metadata(src_meta, tgt_meta,
                                                  source_table, target_table,
                                                  source_db, target_db,
                                                  results, failed_checks)

        # Save & assert
        report_helper.save_report(results, test_type="DC_Source_to_Target_Check")
        assert not failed_checks, "\n".join(failed_checks)
