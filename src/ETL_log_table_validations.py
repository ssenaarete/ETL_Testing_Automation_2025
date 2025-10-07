import logging
import pandas as pd
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class ETLLog_Validation:
    def __init__(self, config_loader):
        self.config_loader = config_loader
        self.db = config_loader.db
        self.report_helper = config_loader.report_helper

        # Get excel_file_path from config.ini via config_loader
        try:
            excel_file_path = config_loader.config.get("PATHS", "excel_file_path")
            self.excel_df = pd.read_excel(
                excel_file_path,
                sheet_name="Audit_tables",
                engine="openpyxl"
            )
        except Exception as e:
            logging.error(f"❌ Could not load 'Audit_tables' sheet: {e}")
            self.excel_df = pd.DataFrame()  # fallback

    def run(self):
        results = []
        process_log_dfs = {}  # dict of DataFrames for process log outputs
        error_log_dfs = {}    # dict of DataFrames for error log outputs
        start_time = time.time()

        for _, row in self.excel_df.iterrows():
            ETL_Process_Log = str(row["Audit_table_1"]).strip()
            ETL_Error_Log = str(row["Audit_table_3"]).strip()
            
            # Split Common_Column
            Common_Cols = [col.strip() for col in str(row["Common_Column"]).split(",") if col.strip()]

            if len(Common_Cols) < 2:
                logging.error(f"❌ Common_Column must have at least 2 columns (ProcessLogId, ComponentName). Got: {Common_Cols}")
                continue

            processlog_id_col = Common_Cols[0]  # ProcessLogId
            group_by_col = Common_Cols[1]      # ComponentName

            try:
                # 1️⃣ Get latest entry from ETL_Process_Log grouped by component
                process_log_query = f"""
                    SELECT T.*
                    FROM {ETL_Process_Log} T
                    WHERE {processlog_id_col} IN (
                        SELECT MAX({processlog_id_col})
                        FROM {ETL_Process_Log}
                        GROUP BY {group_by_col}
                    )
                """

                process_df = pd.read_sql(process_log_query, self.db.conn)
                process_log_dfs[ETL_Process_Log] = process_df.copy()

                if process_df.empty:
                    results.append({
                        "ProcessLogID": None,
                        "Component_Name": None,
                        "ETL_Log_Status": "NOT_FOUND",
                        "ETL_Error_Present": None,
                        "Error_Details": None,
                        "Execution_Time": round(time.time() - start_time, 2),
                        "Error": None
                    })
                    continue

                # Process each row separately
                for _, prow in process_df.iterrows():
                    row_dict = prow.to_dict()
                    process_id = row_dict.get(processlog_id_col)
                    component_name = row_dict.get(group_by_col)
                    log_status = str(row_dict.get("Status", "")).upper()

                    error_present, error_details = None, None

                    # 2️⃣ Case 1: SUCCESS → error log must be empty
                    if log_status == "SUCCESS":
                        error_log_query = f"""
                            SELECT *
                            FROM {ETL_Error_Log}
                            WHERE {processlog_id_col} = ?
                            AND {group_by_col} = ?
                        """
                        error_df = pd.read_sql(error_log_query, self.db.conn, params=[process_id, component_name])
                        error_log_dfs[ETL_Error_Log] = error_df.copy()

                        error_present = "YES" if not error_df.empty else "NO"

                    # 3️⃣ Case 2: FAILED → error log must contain at least one entry
                    elif log_status == "FAILED":
                        error_log_query = f"""
                            SELECT *
                            FROM {ETL_Error_Log}
                            WHERE {processlog_id_col} = ?
                            AND {group_by_col} = ?
                            ORDER BY {processlog_id_col} DESC
                        """
                        error_df = pd.read_sql(error_log_query, self.db.conn, params=[process_id, component_name])
                        error_log_dfs[ETL_Error_Log] = error_df.copy()

                        if not error_df.empty:
                            error_present = "YES"
                            first_row = error_df.iloc[0].to_dict()
                            error_details = f"ErrorID={first_row.get('ErrorID')}, Message={first_row.get('ErrorMessage')}"
                        else:
                            error_present = "NO"

                    results.append({
                        "ProcessLogID": process_id,
                        "Component_Name": component_name,
                        "ETL_Log_Status": log_status,
                        "ETL_Error_Present": error_present,
                        "Error_Details": error_details,
                        "Execution_Time": round(time.time() - start_time, 2),
                        "Error": None
                    })

            except Exception as ex:
                logging.error(f"❌ ETL log validation failed: {ex}")
                results.append({
                    "ProcessLogID": None,
                    "Component_Name": None,
                    "ETL_Log_Status": None,
                    "ETL_Error_Present": None,
                    "Error_Details": None,
                    "Execution_Time": round(time.time() - start_time, 2),
                    "Error": str(ex)
                })

        # Save results + full log tables
        report_file = self.report_helper.save_report(results, test_type="ETL_Log_Validation")

        # Save extra outputs manually
        with pd.ExcelWriter(report_file, mode="a", if_sheet_exists="replace", engine="openpyxl") as writer:
            for tbl, df in process_log_dfs.items():
                df.to_excel(writer, sheet_name=f"Process_{tbl[:20]}", index=False)
            for tbl, df in error_log_dfs.items():
                df.to_excel(writer, sheet_name=f"Error_{tbl[:20]}", index=False)

        # ✅ Assertion logic
        for r in results:
            if r["ETL_Log_Status"] == "SUCCESS" and r["ETL_Error_Present"] == "YES":
                raise AssertionError("❌ Process succeeded but errors found in ETL_Error_Log")
            if r["ETL_Log_Status"] == "FAILED" and r["ETL_Error_Present"] != "YES":
                raise AssertionError("❌ Process failed but no error logged in ETL_Error_Log")
