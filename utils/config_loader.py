import configparser
import pandas as pd
from tkinter import simpledialog, Tk
from utils.db_helper import DBHelper
from utils.report_helper import ReportHelper

class ConfigLoader:
    def __init__(self, config_path="config.ini"):
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        print("Sections found:", self.config.sections())

        self.excel_path = self.config.get("PATHS", "excel_file_path")

        # Ask user for DB name only once
        root = Tk()
        root.withdraw()

        section_input = simpledialog.askstring(
            "Database_Name",
            "Enter the Database name to use:"
        )

        # Match case-insensitive section name
        sections_lower = {s.lower(): s for s in self.config.sections()}
        self.section_name = sections_lower.get(section_input.lower())
        if not self.section_name:
            root.destroy()
            raise ValueError(f"Section '{section_input}' not found in {config_path}")

        # Initialize DBHelper
        self.db = DBHelper.from_config_section(config_path, self.section_name)
        print(f"Connected using section: {self.section_name}")

        # Load matching Excel sheet
        try:
            self.df = pd.read_excel(self.excel_path, sheet_name=self.section_name)
            print(f"Loaded data from Excel sheet: {self.section_name}")
        except ValueError:
            root.destroy()
            raise ValueError(f"Sheet '{self.section_name}' not found in Excel file: {self.excel_path}")

        root.destroy()
        self.db.connect()

        # Create report helper
        self.report_helper = ReportHelper(config_path)
