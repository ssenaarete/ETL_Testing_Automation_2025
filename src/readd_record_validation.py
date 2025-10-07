import logging
import pandas as pd
import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

class ReAddedRecords_Validation:
    def __init__(self, config_loader):
        self.config_loader = config_loader
        self.db = config_loader.db
        self.report_helper = config_loader.report_helper

        try:
            excel_file_path = config_loader.config.get("PATHS", "excel_file_path")
            self.mapping_df = pd.read_excel(
                excel_file_path, sheet_name="Table_Mapping", engine="openpyxl"
            )
            self.target_db_df = pd.read_excel(
                excel_file_path, sheet_name="TARGETDB", engine="openpyxl"
            )
        except Exception as e:
            log.error(f"❌ Could not load mapping sheets: {e}")
            self.mapping_df = pd.DataFrame()
            self.target_db_df = pd.DataFrame()

    def get_composite_keys(self, table_name: str) -> list:
        try:
            keys = (
                self.target_db_df[
                    (self.target_db_df["table_name"] == table_name)
                    & (self.target_db_df["Constraints"].str.contains("COMPOSITE KEY", case=False, na=False))
                ]["column_name"]
                .tolist()
            )
            if not keys:
                log.warning(f"⚠️ No composite keys found for {table_name}")
            else:
                log.info(f"✅ Composite keys for {table_name}: {keys}")
            return keys
        except Exception as e:
            log.error(f"❌ Could not fetch composite keys for {table_name}: {e}")
            return []

    def run(self):
        results = []
        details_dict = {}

        today = datetime.date.today().strftime("%Y-%m-%d")

        for _, row in self.mapping_df.iterrows():
            source_table = str(row["source_table"]).strip()
            target_table = str(row["target_table"]).strip()
            deleted_table = row.get("deleted_table", "")

            if pd.isna(deleted_table) or str(deleted_table).strip() == "":
                continue
            deleted_table = str(deleted_table).strip()

            keys = self.get_composite_keys(target_table)
            if not keys:
                continue

            try:
                # Build join condition
                join_condition = " AND ".join([f"T.{k}=D.{k}" for k in keys])
                src_condition = " AND ".join([f"S.{k}=D.{k}" for k in keys])

                # Re-added Validation
                readded_query = f"""
                    SELECT D.*, S.*, T.Is_Current, T.Version_Begin_Date, T.Version_End_Date
                    FROM {deleted_table} D
                    INNER JOIN SOURCE_db.dbo.{source_table} S
                      ON {src_condition}
                    INNER JOIN {target_table} T
                      ON {join_condition}
                    WHERE (CAST(T.Is_Current AS VARCHAR) = '1' OR CAST(T.Is_Current AS VARCHAR) = 'TRUE'  )
                       AND (CAST(T.Version_Begin_Date AS DATE) <> CAST(GETDATE() AS DATE))
                       AND (T.Version_End_Date NOT IN ('3000-12-31') AND T.Version_End_Date IS NOT NULL)
                """

                mismatches = pd.read_sql(readded_query, self.db.conn)

                if mismatches.empty:
                    status = "PASS"
                    error = None
                else:
                    status = "FAIL"
                    error = "Re-added record does not satisfy business rules"
                    details_dict[f"{target_table}_Readded_Mismatches"] = mismatches

                results.append({
                    "Source_Table": source_table,
                    "Target_Table": target_table,
                    "Deleted_Table": deleted_table,
                    "Composite_Keys": ",".join(keys),
                    "Validation_Status": status,
                    "Error": error
                })

            except Exception as ex:
                log.error(f"❌ Re-added record validation failed: {ex}")
                results.append({
                    "Source_Table": source_table,
                    "Target_Table": target_table,
                    "Deleted_Table": deleted_table,
                    "Composite_Keys": ",".join(keys),
                    "Validation_Status": "ERROR",
                    "Error": str(ex)
                })

        # Save report
        report_file = self.report_helper.save_report(results, test_type="ReAdded_Records_Validation")

        # Save mismatches in separate sheets
        if details_dict:
            with pd.ExcelWriter(report_file, mode="a", if_sheet_exists="replace", engine="openpyxl") as writer:
                for tbl, df in details_dict.items():
                    df.to_excel(writer, sheet_name=f"{tbl[:25]}", index=False)

        return results
