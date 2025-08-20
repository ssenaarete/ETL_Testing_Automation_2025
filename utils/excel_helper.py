import pandas as pd
import logging

class ExcelHelper:
    @staticmethod
    def read_test_cases(file_path):
        try:
            df = pd.read_excel(file_path)
            logging.info(f"Excel file read successfully from {file_path}")
            return df
        except Exception as e:
            logging.error(f"Error reading Excel file: {e}")
            raise
