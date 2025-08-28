import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class DataTypeValidation:
    def __init__(self, config_loader):
        """
        config_loader should already provide:
        - self.db  → connected DBHelper instance
        - self.df  → dataframe from Excel (already filtered by correct sheet)
        - self.report_helper → ReportHelper instance
        """
        self.config_loader = config_loader
        self.db = config_loader.db
        self.df = config_loader.df
        self.report_helper = config_loader.report_helper

    def get_db_metadata(self, table_name):
        """
        Fetch column metadata (datatype + constraints) from DB
        """
        query = f"""
        SELECT 
            o.name AS Table_Name,
            c.name AS Column_Name,
            t.name AS Data_Type,
            ISNULL(tc.CONSTRAINT_TYPE, 
                CASE WHEN c.is_nullable = 0 THEN 'NOT NULL' ELSE 'NULL' END
            ) AS Constraint_Def
        FROM sys.columns c
        JOIN sys.types t 
            ON c.user_type_id = t.user_type_id
        JOIN sys.objects o
            ON c.object_id = o.object_id
        JOIN sys.schemas s
            ON o.schema_id = s.schema_id
        LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            ON kcu.TABLE_NAME = o.name
           AND kcu.TABLE_SCHEMA = s.name
           AND kcu.COLUMN_NAME = c.name
        LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
           AND tc.TABLE_NAME = kcu.TABLE_NAME
           AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
        WHERE o.object_id = OBJECT_ID('{table_name}')
        ORDER BY c.column_id;
        """
        raw = self.db.execute_query(query)

        metadata = []
        for row in raw:
            if isinstance(row, tuple):
                metadata.append({
                    "TABLE_NAME": row[0],
                    "COLUMN_NAME": row[1],
                    "DATA_TYPE": row[2],
                    "CONSTRAINTS": row[3]
                })
            elif isinstance(row, dict):
                metadata.append(row)
        return metadata

    def run(self):
        df = self.df.copy()
        
        results = []

        for _, row in df.iterrows():
            table = row["table_name"]
            column = row["column_name"]
            expected_dtype = str(row["Data_Type"]).strip().upper()
            # expected_constraint = str(row["Constraints"]).strip().upper()
            # ✅ Handle NaN or blank constraint values as NULL
            expected_constraint = row["Constraints"]
            if pd.isna(expected_constraint) or str(expected_constraint).strip() == "":
                expected_constraint = "NULL"
            else:
                expected_constraint = str(expected_constraint).strip().upper()

            db_meta = self.get_db_metadata(table)
            db_cols = [col["COLUMN_NAME"] for col in db_meta]

            if column not in db_cols:
                results.append({
                    "Table_Excel": table,
                    "Column_Excel": column,
                    "DataType_Excel": expected_dtype,
                    "Constraint_Excel": expected_constraint,
                    "DataType_DB": "N/A",
                    "Constraint_DB": "N/A",
                    "Status": "Mismatch (Column Missing in DB)"
                })
                continue

            # get db column details
            db_col = next(c for c in db_meta if c["COLUMN_NAME"] == column)
            db_dtype = db_col["DATA_TYPE"].upper()
            db_constraint = db_col["CONSTRAINTS"].upper() if db_col["CONSTRAINTS"] else "NULL"

            # compare
            dtype_match = (db_dtype == expected_dtype)
            constraint_match = (db_constraint == expected_constraint)

            status_1 = "✅ Matched" if (dtype_match) else "❌ Mismatch"
            status_2 = "✅ Matched" if (constraint_match) else "❌ Mismatch"

            results.append({
                "Database": self.db.database,
                "Table_Excel": table,
                "Column_Excel": column,
                "DataType_Excel": expected_dtype,
                "DataType_DB": db_dtype,
                "DataType_Status": status_1,
                "Constraint_Excel": expected_constraint,               
                "Constraint_DB": db_constraint,
                "Constraint_Status": status_2
            })

        # Save + print
        self.report_helper.save_report(results, test_type="DataType_Constraints_Validation")
        # self.report_helper.print_validation_report_DataType_Constraints_Validation(results, check_type="DataType_Constraints_Validation")

        # ✅ Assertions
        assert results, "Validation returned no results — check Excel sheet or DB connection."

        missing_columns = [r for r in results if "Missing" in r.get("Status", "")]
        assert not missing_columns, f"Columns missing in DB: {missing_columns}"

        mismatched_dtype = [r for r in results if r.get("DataType_Status") == "❌ Mismatch"]
        assert not mismatched_dtype, f"Datatype mismatches found: {mismatched_dtype}"

        mismatched_constraints = [r for r in results if r.get("Constraint_Status") == "❌ Mismatch"]
        assert not mismatched_constraints, f"Constraint mismatches found: {mismatched_constraints}"
        
        return results
