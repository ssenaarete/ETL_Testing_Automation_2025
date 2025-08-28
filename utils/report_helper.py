import pytest
import os
import configparser
from datetime import datetime
import pandas as pd
import logging
from tabulate import tabulate
import textwrap


class ReportHelper:
    def __init__(self, config_path="config.ini"):
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.output_folder = self.config.get("PATHS ", "report_output_path", fallback="Reports")
        os.makedirs(self.output_folder, exist_ok=True)


    def save_report(self, data, test_type="Null_Check"):
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_test_type = test_type.replace(" ", "_")  # clean spaces if any
            output_file = os.path.join(
                self.output_folder, f"{safe_test_type}_{timestamp}.xlsx"
            )
            print(f"Saving report to: {output_file}")

            df = pd.DataFrame(data)
            df.to_excel(output_file, index=False, engine="openpyxl", sheet_name=safe_test_type)

            print("Report saved successfully.")
            logging.info(f"✅ Report saved at {output_file}")
        except Exception as e:
            print(f"Failed to save report: {e}")
            logging.error(f"❌ Error saving report: {e}")
            raise
        return output_file  # ✅ MUST return path


    def print_validation_report_Duplicate(self, results, check_type):

        print(f"\n📊 {check_type} Summary:\n")

        table_data = []
        for row in results:
            print(f"IsCheckPassed value for row: {row['IsCheckPassed']}")
            status = "✅ No" if row["IsCheckPassed"] else "❌ Yes"
            table_data.append([
                row.get("Database", ""),
                row.get("Table_name", ""),
                row.get("Column_names", ""),
                status,
            ])

        headers = ["Database_Name", "Table_Name", "Column_Names", f"Is {check_type.replace(' ', '')} Present"]

        print(tabulate(table_data, headers=headers, tablefmt="grid"))


    def print_validation_report_Null(self, results, check_type):

        print(f"\n📊 {check_type} Summary:\n")

        table_data = []
        for row in results:
            print(f"IsCheckPassed value for row: {row['IsCheckPassed']}")
            status = "✅ No" if row["IsCheckPassed"] else "❌ Yes"
            table_data.append([
                row.get("Database", ""),
                row.get("Table_name", ""),
                row.get("Column_names", ""),
                status,
            ])

        headers = ["Database_Name", "Table_Name", "Column_Names", f"Is {check_type.replace(' ', '')} Present"]

        print(tabulate(table_data, headers=headers, tablefmt="grid"))    


    def print_validation_report_count(self, results):
        """Pretty print count check results in terminal."""
        table_data = []
        for row in results:
            is_passed = (row["Source_Count"] == row["Stage_Count"] == row["Target_Count"])
            table_data.append([
                row["Source_Table"], row["Source_Count"],
                row["Stage_Table"], row["Stage_Count"],
                row["Target_Table"], row["Target_Count"],
                "✅ PASS" if is_passed else "❌ FAIL"
            ])

        headers = ["Source Table", "Source Count",
                   "Stage Table", "Stage Count",
                   "Target Table", "Target Count", "Status"]

        print("\n" + "=" * 80)
        print("COUNT VALIDATION REPORT")
        print("=" * 80)
        print(tabulate(table_data, headers=headers, tablefmt="grid"))
    
    
    def print_validation_report_Source_to_Stage(self, results):
        """Pretty print data completeness check results in terminal."""
        table_data = []
        for row in results:
            # Wrap long column lists
            wrapped_columns = "\n".join(textwrap.wrap(row.get("Common_Columns", ""), width=40))
            table_data.append([
                row.get("Source_DB", ""),
                row.get("Source_Table",""),
                row.get("Stage_DB", ""),
                row.get("Stage_Table",""),
                wrapped_columns,
                row.get("Data_Missing_Count",0),
                "✅ PASS" if row.get("Data_Missing_Count", 0) == 0 else "❌ FAIL"
            ])

        headers = [
            "SOURCE_DB",
            "Source Table",
            "STAGE_DB",
            "Stage Table",
            "Common Columns",
            "Data_Missing_Count",
            "Status"
        ]

        print("\n" + "=" * 80)
        print("DATA COMPLETENESS VALIDATION REPORT")
        print("=" * 80)
        print(tabulate(table_data, headers=headers, tablefmt="grid"))


    def print_validation_report_Stage_to_Target(self, results):
        """Pretty print data completeness check results in terminal."""
        table_data = []
        for row in results:
            # Wrap long column lists
            wrapped_columns = "\n".join(textwrap.wrap(row.get("Common_Columns", ""), width=45))
            table_data.append([
                row.get("Stage_DB", ""),
                row.get("Stage_Table",""),
                row.get("Target_DB", ""),
                row.get("Target_Table",""),
                wrapped_columns,
                row.get("Data_Missing_Count",0),
                "✅ PASS" if row.get("Data_Missing_Count", 0) == 0 else "❌ FAIL"
            ])

        headers = [
            "STAGE_DB",
            "Stage Table",
            "TARGET_DB",
            "Target Table",
            "Common Columns",
            "Data_Missing_Count",
            "Status"
        ]

        print("\n" + "=" * 80)
        print("DATA COMPLETENESS VALIDATION REPORT")
        print("=" * 80)
        print(tabulate(table_data, headers=headers, tablefmt="grid"))


    def print_validation_report_Transformation_logic(self, results):
            """Pretty print count check results in terminal."""
            table_data = []
            for row in results:
                # is_passed = (row["Source_result"] == row["Target_result"])
                table_data.append([
                    row ["Transformation Name"],
                    row["Column_Name"], 
                    # row["Source_result"], 
                    # row["Target_result"],
                    row["Mismatches"],
                    # "✅ PASS" if is_passed else "❌ FAIL"
                    "✅ PASS" if row["Status"] == "PASS" else "❌ FAIL"
                ])

            # headers = ["Column Name", "Source Data", "Target Data", "Status"]

            headers = ["Transformation Name","Column Name", "Mismatch", "Status"]

            print("\n" + "=" * 80)
            print("TRANSFORMATION LOGIC VALIDATION REPORT")
            print("=" * 80)
            print(tabulate(table_data, headers=headers, tablefmt="grid"))
    

    def print_validation_report_Date_Field_Validation(self, results, check_type):
        """Pretty print date field validation results in terminal."""
        table_data = []
        for row in results:
            table_data.append([
                # row.get("Database", self.db.database if hasattr(self, "db") else ""),
                row.get("Database", ""),  # fallback to DB name if present
                row.get("Table", ""),
                row.get("Column", ""),
                "✅ PASS" if row.get("IsCheckPassed", False) else f"❌ FAIL ({row.get('Invalid_Count', 0)} invalid)"
            ])

        headers = ["Database_Name", "Table_Name", "Column_Name", f"{check_type} Status"]

        print("\n" + "=" * 80)
        print("DATE FIELD VALIDATION REPORT")
        print("=" * 80)
        print(tabulate(table_data, headers=headers, tablefmt="grid"))


    def print_validation_report_DataType_Constraints_Validation(self, results, check_type):
        """Pretty print datatype & constraint validation results in terminal."""
        table_data = []
        for row in results:
            table_data.append([
                row.get("Database", ""),
                row.get("Table_Excel", ""),
                row.get("Column_Excel", ""),
                row.get("DataType_Excel", ""),
                row.get("DataType_DB", ""),
                row.get("DataType_Status", ""),
                row.get("Constraint_Excel", ""),
                row.get("Constraint_DB", ""),
                row.get("Constraint_Status", ""),
                # status
            ])
        
        headers = ["Database_Name", "Table_Name_Excel", "Column_Name_Excel","Data_Type_Excel","DataType_DB","DataType_Status",
        "Constraint_Excel","Constraint_DB","Constraint_Status"]

         # ✅ Ensure wide columns (so text like mismatches is fully visible)
        tabulate.PRESERVE_WHITESPACE = True
        print("\n" + "=" * 80)
        print("DATATYPE & CONSTRAINT VALIDATION REPORT")
        print("=" * 80)
        print(tabulate(table_data, headers=headers, tablefmt="grid"))


    def print_validation_report_DataType_Constraints_SourceToStage(self, results, check_type):
        """Pretty print datatype & constraint validation results in terminal."""
        table_data = []
        for row in results:
            table_data.append([
                row.get("Database", ""),
                row.get("Table_Excel", ""),
                row.get("Column_Excel", ""),
                row.get("DataType_Source", ""),
                row.get("DataType_Stage", ""),
                row.get("Constraint_Source", ""),
                row.get("Constraint_Stage", ""),
                row.get("Status","")
            ])
        
        headers = ["Database_Name", "Table_Name_Excel", "Column_Name_Excel","DataType_Source","DataType_Stage","Constraint_Source",
        "Constraint_Stage"]

         # ✅ Ensure wide columns (so text like mismatches is fully visible)
        tabulate.PRESERVE_WHITESPACE = True
        print("\n" + "=" * 80)
        print("DATATYPE & CONSTRAINT SOURCE VS STAGE VALIDATION REPORT")
        print("=" * 80)
        print(tabulate(table_data, headers=headers, tablefmt="grid"))


    def print_validation_report_DataType_Constraints_SourceToTarget(self, results, check_type):
        """Pretty print datatype & constraint validation results in terminal."""
        table_data = []
        for row in results:
            table_data.append([
                row.get("Database", ""),
                row.get("Table_Excel", ""),
                row.get("Column_Excel", ""),
                row.get("DataType_Source", ""),
                row.get("Constraint_Source", ""),
                row.get("DataType_Target", ""),
                row.get("Constraint_Target", ""),
                # row.get("Constraint_DB", ""),
                # row.get("Constraint_Status", ""),
                row.get("Status","")
            ])
        
        headers = ["Database_Name", "Table_Name_Excel", "Column_Name_Excel","DataType_Source","Constraint_Source","DataType_Target",
        "Constraint_Target"]

         # ✅ Ensure wide columns (so text like mismatches is fully visible)
        tabulate.PRESERVE_WHITESPACE = True
        print("\n" + "=" * 80)
        print("DATATYPE & CONSTRAINT SOURCE VS TARGET VALIDATION REPORT")
        print("=" * 80)
        print(tabulate(table_data, headers=headers, tablefmt="grid"))

    
    def print_validation_report_SCD_Metadata_Validation(self, results, check_type):
        """Pretty print datatype & constraint validation results in terminal."""
        table_data = []
        for row in results:
            table_data.append([
                row.get("Database", ""),
                row.get("Table_name", ""),
                row.get("Check_name", ""),
                row.get("Issue_Count"),
                row.get("IsCheckPassed")
            ])
        
        headers = ["Database_Name", "Table_Name_Excel","Check_Name","Issue_Count", "IsCheckPassed"]

         # ✅ Ensure wide columns (so text like mismatches is fully visible)
        tabulate.PRESERVE_WHITESPACE = True
        print("\n" + "=" * 80)
        print("SCD METADATA VALIDATION REPORT")
        print("=" * 80)
        print(tabulate(table_data, headers=headers, tablefmt="grid"))


    def print_validation_report_GarbageVlueValidation(self, results, check_type):
        """Pretty print Garbage value validation results in terminal."""
        table_data = []
        for row in results:
            table_data.append([
                row.get("Database", ""),
                row.get("Table", ""),
                row.get("Column", ""),
                row.get("GARBAGE_VALUE_Count"),
                row.get("Status")
            ])
        
        headers = ["Database_Name", "Table_Name_Excel","Column","GARBAGE_VALUE_Count", "Status"]

         # ✅ Ensure wide columns (so text like mismatches is fully visible)
        tabulate.PRESERVE_WHITESPACE = True
        print("\n" + "=" * 80)
        print("GARBAGE VALUE VALIDATION REPORT")
        print("=" * 80)
        print(tabulate(table_data, headers=headers, tablefmt="grid"))