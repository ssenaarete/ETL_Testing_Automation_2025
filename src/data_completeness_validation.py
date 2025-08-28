import logging
import pandas as pd
import configparser

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# Source to Stage Data Completeness Validation
class Validation_SourceToStage:
    def __init__(self, config_path="config.ini"):    
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.excel_path = self.config.get("PATHS", "excel_file_path")

    def get_common_columns(self, source_db, stage_db, source_table, stage_table):
        """Get common column names between source and stage tables."""
        src_cols = source_db.execute_query(    
            f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{source_table}'
            """
        )
        logging.info(f"Source columns for {source_table}: {src_cols}")
        src_cols = [col[0] for col in src_cols]      

        stg_cols = stage_db.execute_query(    
            f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{stage_table}'
            """
        )
        logging.info(f"Stage columns for {stage_table}: {stg_cols}")
        stg_cols = [c[0] for c in stg_cols]

        # Intersection
        common = list(set(src_cols).intersection(set(stg_cols)))
        common_quoted = [f"[{col}]" for col in common]
        
        return ", ".join(common_quoted)

    def run(self, source_db, stage_db, report_helper):
        df = pd.read_excel(self.excel_path, sheet_name="Table_Mapping")
        
        results = []
        failed_checks = []  # track failures

        for _, row in df.iterrows():
            source_table = row["source_table"]
            stage_table = row["stage_table"]

            common_columns = self.get_common_columns(source_db, stage_db, source_table, stage_table)
            logging.info(f"Common columns for {source_table} ↔ {stage_table}: {common_columns}")

            # ✅ Assertion: must have common columns
            assert common_columns, f"❌ No common columns found for {source_table} ↔ {stage_table}"

            completeness_query = f"""
                SELECT COUNT(*) AS Missing_Count
                FROM (
                    SELECT {common_columns} 
                    FROM {self.config.get("SOURCEDB", "database")}.dbo.{source_table}
                    EXCEPT
                    SELECT {common_columns} 
                    FROM {self.config.get("STAGEDB", "database")}.dbo.{stage_table}
                ) AS diff
            """
                
            logging.info(f"Running completeness check: {source_table} → {stage_table}")
            raw_result = source_db.execute_query(completeness_query)

            missing_count = raw_result[0][0] if raw_result else 0

            # ✅ Assertion: missing count should be 0
            # assert missing_count == 0, (
            #     f"❌ Data completeness check failed for {source_table} ↔ {stage_table}. "
            #     f"Missing rows = {missing_count}"
            # )

            is_check_passed = missing_count == 0

            results.append({
                "Source_DB": self.config.get("SOURCEDB", "database"),
                "Source_Table": source_table,
                "Stage_DB": self.config.get("STAGEDB", "database"),
                "Stage_Table": stage_table,
                "Common_Columns": common_columns,
                "Data_Missing_Count": missing_count,
                "IsCheckPassed": is_check_passed
            })

            if not is_check_passed:
                failed_checks.append(f"❌ Data completeness check failed for {source_table} ↔ {stage_table}. Missing rows = {missing_count}")

            # assert is_check_passed, f"❌ Data completeness check failed for {source_table} ↔ {stage_table}"

        # ✅ Save & print report
        report_helper.save_report(results, test_type="Data_Completeness_Source_to_Stage")
        # report_helper.print_validation_report_Source_to_Stage(results)

        # ✅ Fail test at the end (after report generation)
        assert not failed_checks, "\n".join(failed_checks)



#Stage to Target Data Completeness Validation
# This script validates data completeness between stage and target databases by checking for missing rows in both directions
class Validation_StageToTarget:
    def __init__(self, config_path="config.ini"):
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.excel_path = self.config.get("PATHS", "excel_file_path")

    def get_common_columns(self,stage_db, target_db, stage_table, target_table):
        """Get common column names between source and stage tables."""
        stg_cols = stage_db.execute_query(
            f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{stage_table}'
            """
        )
        logging.info(f"Stage columns for {stage_table}: {stg_cols}")
        stg_cols = [col[0] for col in stg_cols]      
        
        trg_cols = target_db.execute_query(
            f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{target_table}'
            """
        )
        logging.info(f"Stage columns for {target_table}: {trg_cols}")
        trg_cols = [c[0] for c in trg_cols]

        # Intersection
        common = list(set(stg_cols).intersection(set(trg_cols)))

        # ✅ Quote columns to handle spaces/reserved keywords
        common_quoted = [f"[{col}]" for col in common]
        
        return ", ".join(common_quoted)

    def run(self, stage_db, target_db, report_helper):
        df = pd.read_excel(self.excel_path, sheet_name="Table_Mapping")

        results = []
        failed_checks = []  # track failures

        for _, row in df.iterrows():
            stage_table = row["stage_table"]
            target_table = row["target_table"]

            common_columns = self.get_common_columns(stage_db, target_db,stage_table, target_table)
            # print(f"Common columns for {stage_table} ↔ {target_table}: {common_columns}")
            logging.info(f"Common columns for {stage_table} ↔ {target_table}: {common_columns}")

            # if not common_columns:
            #     logging.warning(f"No common columns found for {stage_table} ↔ {target_table}")
            #     continue

            assert common_columns, f"❌ No common columns found for {stage_table} ↔ {target_table}"

            completeness_query = f"""
                SELECT COUNT(*) AS Missing_Count
                FROM (
                    SELECT {common_columns} FROM {self.config.get("STAGEDB", "database")}.dbo.{stage_table}
                    EXCEPT
                    SELECT {common_columns} FROM {self.config.get("TARGETDB", "database")}.dbo.{target_table}
                ) AS diff
            """
                

            logging.info(f"Running completeness check: {stage_table} → {target_table}")
            raw_result = stage_db.execute_query(completeness_query)

            missing_count = raw_result[0][0] if raw_result else 0

            # assert missing_count == 0, (
            #     f"❌ Data completeness check failed for {stage_table} ↔ {target_table}. "
            #     f"Missing rows = {missing_count}"
            # )

            is_check_passed = missing_count == 0

            results.append({
                "Stage_DB": self.config.get("STAGEDB", "database"),
                "Stage_Table": stage_table,
                "Target_DB": self.config.get("TARGETDB", "database"),
                "Target_Table": target_table,
                "Common_Columns": common_columns,
                "Data_Missing_Count": missing_count,
                "IsCheckPassed": is_check_passed
            })

            if not is_check_passed:
                failed_checks.append(f"❌ Data completeness check failed for {stage_table} ↔ {target_table}. Missing rows = {missing_count}")

            # assert is_check_passed, f"❌ Data completeness check failed for {stage_table} ↔ {target_table}"

        # ✅ Save & print report
        report_helper.save_report(results, test_type="Data_Completeness_Stage_to_Target")
        # self.report_helper.print_validation_report_Stage_to_Target(results)

        # ✅ Fail test at the end (after report generation)
        assert not failed_checks, "\n".join(failed_checks)