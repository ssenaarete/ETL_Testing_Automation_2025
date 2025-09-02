import logging
import pandas as pd
import configparser

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class TransformationValidation:
    def __init__(self, config_path="config.ini"):
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.excel_path = self.config.get("PATHS", "excel_file_path")

        # # DB connections
        # self.db_source = DBHelper.from_config_section(config_path, "SOURCEDB")
        # self.db_target = DBHelper.from_config_section(config_path, "TARGETDB")

        # self.db_source.connect()
        # self.db_target.connect()

        # self.report_helper = ReportHelper(config_path)

        # ✅ Ask user for transformation name
        # self.transformation_name = input("Enter the transformation name (e.g., Age Transformation): ")

    def run(self, source_db, target_db, report_helper):
        # Read queries from SOURCEDB sheet
        df = pd.read_excel(self.excel_path, sheet_name="SOURCEDB", engine="openpyxl")

        # Filter only Is_Transformation = Y
        df = df[df["Is_Transformation"].str.upper() == "Y"]

        results = []
        mismatch_records = []   # ✅ store mismatches separately

        for _, row in df.iterrows():
            columns = row["column_name"] 
            source_query = row.get("Source_Query")
            print(f"Executing Source Query: {source_query}")
            target_query = row.get("Target_Query")
            print(f"Executing Target Query: {target_query}")

            # Execute queries
            # source_data = self.db_source.execute_query(source_query)
            # target_data = self.db_target.execute_query(target_query)

            source_data = source_db.execute_query(source_query)
            target_data = target_db.execute_query(target_query)

            # Convert to dict for row-by-row comparison
            source_dict = {row[0]: row[1] for row in source_data if row[0] is not None}
            target_dict = {row[0]: row[1] for row in target_data if row[0] is not None}

            mismatches = []
            for key, src_val in source_dict.items():
                tgt_val = target_dict.get(key)
                if tgt_val is None:
                    mismatches.append({"Key": key, "Source_Value": src_val, "Target_Value": "MISSING"})
                    mismatch_records.append({
                        "Transformation Name": f"{columns}_Transformation",
                        "Column_Name": columns,
                        "Key": key,
                        "Source_Value": src_val,
                        "Target_Value": "MISSING"
                    })
                elif src_val != tgt_val:
                    mismatches.append({"Key": key, "Source_Value": src_val, "Target_Value": tgt_val})
                    mismatch_records.append({
                        "Transformation Name": f"{columns}_Transformation",
                        "Column_Name": columns,
                        "Key": key,
                        "Source_Value": src_val,
                        "Target_Value": tgt_val
                    })
            status = "PASS" if not mismatches else "FAIL"

            results.append({
                # "Transformation Name": self.transformation_name,  # ✅ added transformation name
                "Transformation Name": f"{columns}_Transformation",  # ✅ added transformation name
                "Column_Name": columns,   # <-- keep column name in report
                "Mismatches": "Mismatche" if mismatches else "Matched",
                "Status": status
            })

            if status == "PASS":
                logging.info(f"✅ Transformation check passed for {columns}. No mismatches found.")

        # Save report
        report_file = report_helper.save_report(results, test_type="Transformation_Logic_check")
        print(f"DEBUG: report_file returned = {report_file}")
        # report_helper.print_validation_report_Transformation_logic(results)

        if mismatch_records:
            mismatch_df = pd.DataFrame(mismatch_records)
            with pd.ExcelWriter(report_file, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                mismatch_df.to_excel(writer, sheet_name="Mismatches", index=False)

            logging.info(f"Mismatches exported to new worksheet in: {report_file}")

        # ✅ Fail test only at the end (after report generation)
        assert all(r["Status"] == "PASS" for r in results), "❌ Some transformation checks failed. See report for details."