import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

class DeletedVsTarget_Validation:
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
            logging.error(f"❌ Could not load mapping sheets: {e}")
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
                logging.warning(f"⚠️ No composite keys found for {table_name}")
            else:
                logging.info(f"✅ Composite keys for {table_name}: {keys}")

            return keys

        except Exception as e:
            logging.error(f"❌ Could not fetch composite keys for {table_name}: {e}")
            return []


    def run(self):
        results = []
        details_dict = {}  # store mismatched rows by table

        for _, row in self.mapping_df.iterrows():
            # source_table = str(row["source_table"]).strip()
            target_table = str(row["target_table"]).strip()

            deleted_table = row.get("deleted_table", "")
            if pd.isna(deleted_table) or str(deleted_table).strip() == "":
                continue  # skip if no deleted table
            deleted_table = str(deleted_table).strip()


            keys = self.get_composite_keys(target_table)
            if not keys:
                logging.warning(f"⚠️ No composite keys found for {target_table}")
                continue

            try:
                # Build join condition dynamically
                join_condition = " AND ".join([f"T.{k}=D.{k}" for k in keys])
                # src_condition = " AND ".join([f"S.{k}=D.{k}" for k in keys])

                # 1️⃣ Deleted vs Target Validation
                deleted_vs_target = f"""
                    SELECT D.*, T.Is_Current, T.Version_End_Date
                    FROM {deleted_table} D
                    LEFT JOIN {target_table} T
                      ON {join_condition}
                    WHERE (CAST(T.Is_Current AS VARCHAR) <> '0' AND CAST(T.Is_Current AS VARCHAR) <> 'FALSE'  ) 
                       AND T.Version_End_Date <> D.CreatedDTM
                """

                target_mismatch = pd.read_sql(deleted_vs_target, self.db.conn)

                # # 2️⃣ Deleted vs Source Validation
                # deleted_vs_source = f"""
                #     SELECT D.*
                #     FROM {deleted_table} D
                #     INNER JOIN {source_table} S
                #       ON {src_condition}
                # """
                # source_mismatch = pd.read_sql(deleted_vs_source, self.db.conn)

                # status = "PASS"
                error = None
                if target_mismatch.empty:
                    status = "PASS"
                elif not target_mismatch.empty:
                    status = "FAIL"
                    error = "Target mismatch: Deleted record still active"
                    details_dict[f"{target_table}_Mismatches"] = target_mismatch
                # elif not source_mismatch.empty:
                #     status = "FAIL"
                #     error = "Source mismatch: Deleted record exists in source"

                results.append({
                    # "Source_Table": source_table,
                    "Target_Table": target_table,
                    "Deleted_Table": deleted_table,
                    "Composite_Keys": ",".join(keys),
                    "Validation_Status": status,
                    # "Execution_Time": round(time.time() - start_time, 2),
                    "Error": error
                })

            except Exception as ex:
                logging.error(f"❌ Deleted record validation failed: {ex}")
                results.append({
                    # "Source_Table": source_table,
                    "Target_Table": target_table,
                    "Deleted_Table": deleted_table,
                    "Composite_Keys": ",".join(keys),
                    "Validation_Status": "ERROR",
                    # "Execution_Time": round(time.time() - start_time, 2),
                    "Error": str(ex)
                })

        # Save report
        self.report_helper.save_report(results, test_type="Deleted_Vs_Target_Validation")

        assert results, "❌ Validation returned no results — check Excel sheet or DB connection."