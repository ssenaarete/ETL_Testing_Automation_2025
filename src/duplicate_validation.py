import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class DuplicateValidation:
    def __init__(self, config_loader, db_name):
        self.config_loader = config_loader
        self.db = config_loader.db
        self.df = config_loader.df
        self.report_helper = config_loader.report_helper

        self.db_name = db_name.upper()

    def run(self):
        df = self.df.copy()

        # Filter only Business Key = Y
        df = df[df["Business Key"].str.upper() == "Y"]

        # Group by table and merge all Y columns into a list
        grouped = df.groupby("table_name")["column_name"].apply(list).reset_index()
        
        results = []
        failed_checks = []

        for _, row in grouped.iterrows():
            table = row["table_name"]
            columns = row["column_name"]  
            composite_key = ", ".join(columns)
            print("DEBUG composite_key:", composite_key, type(composite_key))

            # # Get duplicate query from excel (if available)
            # duplicate_query_excel = df.loc[df["table_name"] == table, "Duplicate_Check_SQL_query"].dropna().unique()

            # if len(duplicate_query_excel) > 0 and duplicate_query_excel[0].strip():
            #     duplicate_query = duplicate_query_excel[0].strip()

            # else:
            #     # ✅ Apply different duplicate logic depending on DB type
            if self.db_name.upper() == "SOURCEDB":
                # No Is_Current filter for source
                duplicate_query = f"""
                    SELECT {", ".join(columns)}
                    FROM {table}
                    GROUP BY {", ".join(columns)}
                    HAVING COUNT(*) > 1
                """    
                            
            else:
                # Default dynamic duplicate check query → returns duplicate groups
                duplicate_query = f"""
                    SELECT {", ".join(columns)}
                    FROM {table}
                    WHERE CAST(Is_Current AS NVARCHAR) IN ('1', 'TRUE', 'True', 'true')
                    GROUP BY {", ".join(columns)}
                    HAVING COUNT(*) > 1
                """
                # print("DEBUG duplicate_query:", duplicate_query)

            logging.info(f"Running query for {table} with key [{composite_key}]")
            raw_result = self.db.execute_query(duplicate_query)
            logging.debug(f"Raw DB result for {table}.{composite_key}: {raw_result!r}")

            # ✅ FIX: count duplicate groups correctly
            duplicate_count = len(raw_result) if raw_result else 0
            is_check_passed = (duplicate_count == 0)

            results.append({
                "Database": self.db.database,
                "Table_name": table,
                "Column_names": composite_key,   # string, not list
                "DUPLICATE_Count": duplicate_count,
                "IsCheckPassed": is_check_passed
            })

            if not is_check_passed:
                failed_checks.append(f"{table}.{composite_key} → Duplicate count: {duplicate_count}")
            
            logging.info(f"{table}.{composite_key} → Duplicate count: {duplicate_count} → {'PASS' if is_check_passed else 'FAIL'}")
            print(f"{table}.{composite_key} → Duplicate Count:", duplicate_count)

        # Save + print reports
        self.report_helper.save_report(results, test_type="Duplicate_Check")
        # self.report_helper.print_validation_report_Duplicate(results, check_type="Duplicate")

        # ✅ Assert: No failed checks
        assert not failed_checks, f"Duplicate validation failed for: {failed_checks}"