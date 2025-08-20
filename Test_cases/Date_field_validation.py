import os
import sys

# Add project root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from datetime import datetime
import logging
from utils.db_helper import DBHelper
from utils.excel_helper import ExcelHelper
from utils.report_helper import ReportHelper
from utils.generate_pdf_report import PDFReportGenerator

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class DateFieldValidation:
    def __init__(self, config_loader):
        self.config_loader = config_loader
        self.db = config_loader.db
        self.df = config_loader.df
        self.report_helper = config_loader.report_helper

    def get_date_columns(self, table_name, schema="dbo"):
        """Fetch all date/datetime/datetime2 columns for a given table."""
        query = f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}'
          AND TABLE_NAME = '{table_name}'
          AND DATA_TYPE IN (
              'date', 'datetime', 'datetime2', 'smalldatetime', 
              'datetimeoffset', 'time'
          )
        ORDER BY COLUMN_NAME;
        """
        rows = self.db.execute_query(query)
        return [r[0] for r in rows] if rows else []

    def validate_date(self, date_value):
        # """Validate date in strict YYYY-MM-DD format."""
        # try:
        #     datetime.strptime(str(date_value), "%Y-%m-%d")
        #     return True
        # except Exception:
        #     return False
        

        """Validate date/time values against multiple strict formats."""
        if date_value is None:
            return True  # Ignore nulls, as they are valid

        value = str(date_value).strip()
        formats = [
            "%Y-%m-%d",                # Date
            "%Y-%m-%d %H:%M:%S",       # Datetime
            "%Y-%m-%d %H:%M:%S.%f",    # Datetime with microseconds
            "%H:%M:%S",                # Time
            "%H:%M:%S.%f",             # Time with fractional seconds
            "%Y-%m-%d %H:%M:%S%z",     # Datetime with timezone offset
            "%Y-%m-%dT%H:%M:%S",       # ISO format
            "%Y-%m-%dT%H:%M:%S.%f",    # ISO with microseconds
        ]

        for fmt in formats:
            try:
                datetime.strptime(value, fmt)
                return True
            except ValueError:
                continue

        return False

    def run(self, tables=None, schema="dbo"):
        results = []

        # ✅ If user didn't pass tables, fallback to Excel sheet (self.df)
        if tables is None and "table_name" in self.df.columns:
            tables = self.df["table_name"].dropna().tolist()

        for table in tables:
            date_columns = self.get_date_columns(table, schema)
            if not date_columns:
                logging.info(f"ℹ No date columns found in {schema}.{table}")
                continue

            for col in date_columns:
                query = f"SELECT {col} FROM {schema}.{table}"
                values = [v[0] for v in self.db.execute_query(query) if v[0] is not None]
                invalid_dates = [v for v in values if not self.validate_date(v)]

                results.append({
                    "Database": self.db.database,
                    "Table": f"{schema}.{table}",
                    "Column": col,
                    "Invalid_Count": len(invalid_dates),
                    "IsCheckPassed": (len(invalid_dates) == 0)
                })

                logging.info(f"{schema}.{table}.{col} → Invalid Count: {len(invalid_dates)}")

        # Save + print reports
        self.report_helper.save_report(results, test_type="Date_Field_Check")

        if hasattr(self.report_helper, "print_validation_report_Date_Field_Validation"):
            self.report_helper.print_validation_report_Date_Field_Validation(results, check_type="Date format")
        else:
            self.report_helper.print_validation_report(results, check_type="Date format")

        # Generate PDF report
        report = PDFReportGenerator(config_path="config.ini", font_path="DejaVuSans.ttf")
        pdf_path = report.generate(results, check_type="Date format")
        print(f"PDF report saved at: {pdf_path}")
