import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)


class DataPrecisionValidation:
    def __init__(self, config_loader):
        self.config_loader = config_loader
        self.db = config_loader.db
        self.df = config_loader.df
        self.report_helper = config_loader.report_helper

    def get_db_metadata(self, table_name):
        query = f"""
        SELECT 
            o.name AS Table_Name,
            c.name AS Column_Name,
            t.name AS Data_Type,
            c.max_length,
            c.precision,
            c.scale
        FROM sys.columns c
        JOIN sys.types t ON c.user_type_id = t.user_type_id
        JOIN sys.objects o ON c.object_id = o.object_id
        JOIN sys.schemas s ON o.schema_id = s.schema_id
        WHERE o.object_id = OBJECT_ID('{table_name}')
        ORDER BY c.column_id;
        """
        raw = self.db.execute_query(query)
        metadata = []
        for row in raw:
            metadata.append({
                "TABLE_NAME": row[0],
                "COLUMN_NAME": row[1],
                "DATA_TYPE": row[2],
                "MAX_LENGTH": row[3],
                "PRECISION": row[4],
                "SCALE": row[5]
            })
        return metadata

    def run(self):
        df = self.df.copy()
        results = []

        for _, row in df.iterrows():
            table = row["table_name"]
            column = row["column_name"]
            expected_dtype = str(row["Data_Type"]).strip().upper()

            db_meta = self.get_db_metadata(table)
            db_cols = [col["COLUMN_NAME"] for col in db_meta]

            if column not in db_cols:
                results.append({
                    "Table_Excel": table,
                    "Column_Excel": column,
                    "DataType_Excel": expected_dtype,
                    "DataType_DB": "N/A",
                    "Status": "Mismatch (Column Missing in DB)"
                })
                continue

            db_col = next(c for c in db_meta if c["COLUMN_NAME"] == column)
            db_dtype = db_col["DATA_TYPE"].upper()
            db_precision = db_col.get("PRECISION")
            db_maxlen = db_col.get("MAX_LENGTH")

            precision_issue = None
            if db_dtype in ("DECIMAL", "NUMERIC") and db_precision and db_precision > 32:
                precision_issue = f"❌ Mismatch - Numeric precision too high: {db_precision} > 32"

            if db_dtype in ("CHAR", "NCHAR", "VARCHAR", "NVARCHAR") and db_maxlen and db_maxlen > 32:
                precision_issue = f"❌ Mismatch - String length too high: {db_maxlen} > 32"

            if precision_issue:
                results.append({
                    "Database": self.db.database,
                    "Table_Excel": table,
                    "Column_Excel": column,
                    "DataType_Excel": expected_dtype,
                    "DataType_DB": db_dtype,
                    "Status": precision_issue
                })

        # Save + print
        self.report_helper.save_report(results, test_type="DataType_Precision_Validation")

        # ✅ Assertions
        assert results, "❌ Validation returned no results — check Excel sheet or DB connection."

        missing_columns = [r for r in results if "Missing" in r.get("Status", "")]
        assert not missing_columns, f"❌ Columns missing in DB: {missing_columns}"

        precision_issues = [r for r in results if "too high" in r.get("Status", "")]
        assert not precision_issues, f"❌ Precision length exceeds 32: {precision_issues}"

        return results
