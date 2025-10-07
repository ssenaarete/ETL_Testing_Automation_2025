import logging
import time
import pandas as pd

class JobExecutionValidation:
    def __init__(self, config_loader):
        self.config_loader = config_loader
        self.db = config_loader.db
        self.report_helper = config_loader.report_helper

        try:
            excel_file = config_loader.config.get("PATHS", "excel_file_path")
            self.excel_df = pd.read_excel(excel_file, sheet_name="TARGETDW", engine="openpyxl")
        except Exception as e:
            logging.error(f"❌ Could not load Jobs sheet: {e}")
            self.excel_df = pd.DataFrame()

    def run(self):
        if self.excel_df.empty:
            logging.error("❌ Jobs sheet is missing or empty in Excel.")
            return

        results = []
        # screenshots = []

        for _, row in self.excel_df.iterrows():
            job_name = str(row.get("Job_Name", "")).strip()
            job_command = str(row.get("Job_Command", "")).strip()
            run_flag = str(row.get("Run (Y/N)", "N")).strip().upper()

            if run_flag != "Y":
                logging.info(f"⏭ Skipping job: {job_name}")
                results.append({
                    "Job_Name": job_name,
                    "Command": job_command,
                    "Run_Flag": run_flag,
                    "Status": "SKIPPED",
                    "Execution_Time": None,
                    "Error": None,
                })
                continue

            start_time = time.time()
            try:
                logging.info(f"▶ Running job: {job_name} | Command: {job_command}")
                self.db.execute_non_query(job_command)  # run job
                exec_time = round(time.time() - start_time, 2)

                results.append({
                    "Job_Name": job_name,
                    "Command": job_command,
                    "Run_Flag": run_flag,
                    "Status": "PASS",
                    "Execution_Time": exec_time,
                    "Error": None,
                })

            except Exception as ex:
                exec_time = round(time.time() - start_time, 2)
                logging.error(f"❌ Job failed: {job_name} | Error: {ex}")

                results.append({
                    "Job_Name": job_name,
                    "Command": job_command,
                    "Run_Flag": run_flag,
                    "Status": "FAIL",
                    "Execution_Time": exec_time,
                    "Error": str(ex),
                })

        # Save report
        report_file = self.report_helper.save_report(results, test_type="Job_Execution_Validation")

        try:
            post_query = f"""
                    WITH LatestRun AS (
                    SELECT 
                        j.name AS JobName,
                        h.run_date,
                        h.run_time,
                        ROW_NUMBER() OVER (PARTITION BY j.job_id ORDER BY h.run_date DESC, h.run_time DESC) AS rn,
                        CASE h.run_status
                            WHEN 0 THEN 'Failed'
                            WHEN 1 THEN 'Succeeded'
                            WHEN 2 THEN 'Retry'
                            WHEN 3 THEN 'Canceled'
                            WHEN 4 THEN 'In Progress'
                        END AS RunStatus,
                        h.message
                    FROM msdb.dbo.sysjobs j
                    JOIN msdb.dbo.sysjobhistory h 
                        ON j.job_id = h.job_id
                    WHERE h.step_id = 0
                    AND j.name = '{job_name}'
                )
                SELECT JobName, run_date, run_time, RunStatus,message
                FROM LatestRun
                WHERE rn = 1
                ORDER BY JobName;
            """

            print("🔍 Capturing job execution log from msdb... - ", post_query)

            cursor = self.db.conn.cursor()
            cursor.execute(post_query)
            result = cursor.fetchall()
            print("DEBUG - fetchall result:", result)
            for i, row in enumerate(result, start=1):
                print(f"Row {i}: {row} | len={len(row)} | type={type(row)}")
            columns = [desc[0] for desc in cursor.description]  # ['JobName','run_date','run_time','RunStatus','message']

            print("DEBUG - Raw result from DB:", result)
            print("DEBUG - Columns from DB:", columns)

            # Ensure rows are tuples
            cleaned_result = [tuple(row) for row in result]

            df_post = pd.DataFrame(cleaned_result, columns=columns)

            with pd.ExcelWriter(report_file, engine="openpyxl", mode="a", if_sheet_exists="overlay") as writer:
                df_post.to_excel(writer, sheet_name="Job_Execution_Log", index=False)

            logging.info("📄 Job execution log appended to Excel report.")

        except Exception as ex:
                logging.error(f"❌ Could not capture job execution log: {ex}")


        # Fail test if any FAIL status found
        assert all(r["Status"] in ("PASS", "SKIPPED") for r in results), (
            "❌ One or more jobs failed. Check Job Execution report."
        )
