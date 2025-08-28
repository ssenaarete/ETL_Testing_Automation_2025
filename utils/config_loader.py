import pytest
import configparser
import pandas as pd
from tkinter import simpledialog, Tk
from utils.db_helper import DBHelper
from utils.report_helper import ReportHelper


class ConfigLoader:

# #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#     _instance = None   # store the singleton
#     def __new__(cls, config_path="config.ini"):
#         if cls._instance is None:
#             cls._instance = super(ConfigLoader, cls).__new__(cls)
#         return cls._instance
# #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++    

    # def __init__(self, config_path="config.ini"):
    def __init__(self, config_path="config.ini", section_name=None):
        
# #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#         if hasattr(self, "_initialized") and self._initialized:
#             return  # ✅ already initialized once
# #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#         
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        print("Sections found:", self.config.sections())

        self.excel_path = self.config.get("PATHS", "excel_file_path")

# #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++        
        root = None  # ✅ define upfront so we can safely destroy later
        try:
            if section_name is None:
    # #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++        
            # Ask user for DB name only once
                root = Tk()
                root.withdraw()

                section_input = simpledialog.askstring(
                    "Database",
                    "Enter the Database name to use:"
                )
    # #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++  
            else:
                section_input = section_name   # ✅ ensure always defined
    # #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++              

            # Match case-insensitive section name
            sections_lower = {s.lower(): s for s in self.config.sections()}
            self.section_name = sections_lower.get(section_input.lower())
            if not self.section_name:
    # #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++  
                # if root:
    # #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++                  
                    # root.destroy()
                raise ValueError(f"Section '{section_input}' not found in {config_path}")

            # Initialize DBHelper
            self.db = DBHelper.from_config_section(config_path, self.section_name)
            print(f"Connected using section: {self.section_name}")

            # Load matching Excel sheet
            try:
                self.df = pd.read_excel(self.excel_path, sheet_name=self.section_name)
                print(f"Loaded data from Excel sheet: {self.section_name}")
            except ValueError:
                # root.destroy()
                raise ValueError(f"Sheet '{self.section_name}' not found in Excel file: {self.excel_path}")

            # root.destroy()
            self.db.connect()

            # Create report helper
            self.report_helper = ReportHelper(config_path)

        finally:
            # ✅ destroy only if created
            if root is not None:
                root.destroy()