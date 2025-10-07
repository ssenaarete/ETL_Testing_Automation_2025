import logging
import pandas as pd
import time

class Process_vs_Detail_log_Validation:
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
        process_log_dfs = {}
        detail_log_dfs = {}
        start_time = time.time()

        for _, row in self.excel_df.iterrows():
            ETL_Process_Log = str(row["Audit_table_1"]).strip()
            ETL_Detail_Process_Log = str(row["Audit_table_2"]).strip()
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
                        "Detail_Log_Present": None,
                        "Detail_Log_Info": None,
                        "Execution_Time": round(time.time() - start_time, 2),
                        "Error": None
                    })
                    continue

                for _, prow in process_df.iterrows():
                    row_dict = prow.to_dict()
                    process_id = row_dict.get(processlog_id_col)
                    component_name = row_dict.get(group_by_col)
                    log_status = str(row_dict.get("Status", "")).upper()

                    detail_present, detail_info = None, None

                    # 2️⃣ SUCCESS → detail log should exist (NO missing details)
                    if log_status == "SUCCESS":
                        detail_log_query = f"""
                            SELECT *
                            FROM {ETL_Detail_Process_Log}
                            WHERE {processlog_id_col} = ?
                              AND {group_by_col} = ?
                        """
                        detail_df = pd.read_sql(detail_log_query, self.db.conn, params=[process_id, component_name])
                        detail_log_dfs[ETL_Detail_Process_Log] = detail_df.copy()

                        detail_present = "YES" if not detail_df.empty else "NO"

                    # 3️⃣ FAILED → detail log must also have error details
                    elif log_status == "FAILED":
                        detail_log_query = f"""
                            SELECT *
                            FROM {ETL_Detail_Process_Log}
                            WHERE {processlog_id_col} = ?
                              AND {group_by_col} = ?
                            ORDER BY {processlog_id_col} DESC
                        """
                        detail_df = pd.read_sql(detail_log_query, self.db.conn, params=[process_id, component_name])
                        detail_log_dfs[ETL_Detail_Process_Log] = detail_df.copy()

                        if not detail_df.empty:
                            detail_present = "YES"
                            first_row = detail_df.iloc[0].to_dict()
                            detail_info = f"DetailID={first_row.get('DetailID')}, Message={first_row.get('DetailMessage')}"
                        else:
                            detail_present = "NO"

                    results.append({
                        "ProcessLogID": process_id,
                        "Component_Name": component_name,
                        "ETL_Log_Status": log_status,
                        "Detail_Log_Present": detail_present,
                        "Detail_Log_Info": detail_info,
                        "Execution_Time": round(time.time() - start_time, 2),
                        "Error": None
                    })

            except Exception as ex:
                logging.error(f"❌ ETL Process vs Detail log validation failed: {ex}")
                results.append({
                    "ProcessLogID": None,
                    "Component_Name": None,
                    "ETL_Log_Status": None,
                    "Detail_Log_Present": None,
                    "Detail_Log_Info": None,
                    "Execution_Time": round(time.time() - start_time, 2),
                    "Error": str(ex)
                })

        # Save results + full log tables
        report_file = self.report_helper.save_report(results, test_type="ETL_ProcessVsDetails_Validation")

        # Save extra outputs manually
        with pd.ExcelWriter(report_file, mode="a", if_sheet_exists="replace", engine="openpyxl") as writer:
            for tbl, df in process_log_dfs.items():
                df.to_excel(writer, sheet_name=f"Process_{tbl[:20]}", index=False)
            for tbl, df in detail_log_dfs.items():
                df.to_excel(writer, sheet_name=f"Detail_{tbl[:20]}", index=False)

        # ✅ Assertion logic
        for r in results:
            if r["ETL_Log_Status"] == "SUCCESS" and r["Detail_Log_Present"] != "YES":
                raise AssertionError("❌ Process succeeded but no details found in ETL_Detail_Process_Log")
            if r["ETL_Log_Status"] == "FAILED" and r["Detail_Log_Present"] != "YES":
                raise AssertionError("❌ Process failed but no detail log entries found in ETL_Detail_Process_Log")
