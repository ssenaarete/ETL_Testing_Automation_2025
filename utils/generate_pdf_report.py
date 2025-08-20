import os
import configparser
from datetime import datetime
from fpdf import FPDF

class PDFReportGenerator:
    def __init__(self, config_path="config.ini", font_path="DejaVuSans.ttf"):
        # Load config
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        
        # Get output folder from config
        self.output_folder = self.config.get("PATHS", "report_output_path", fallback="Reports")
        os.makedirs(self.output_folder, exist_ok=True)  # Ensure folder exists
        
        # Initialize PDF
        self.pdf = FPDF()
        self.font_path = font_path

        # Add Unicode font
        if not os.path.exists(self.font_path):
            raise FileNotFoundError(f"Font file not found: {self.font_path}")
        
        self.pdf.add_font("DejaVu", "", self.font_path, uni=True)
        self.pdf.set_font("DejaVu", size=12)

    def generate(self, results, check_type="Validation Report"):
        self.pdf.add_page()

        # Title
        self.pdf.set_font("DejaVu", size=16)
        self.pdf.cell(0, 10, f"{check_type} Report", ln=True, align="C")
        self.pdf.ln(10)

        # Table Header
        metric_column_name = check_type
        #headers = ["Database", "Table Name", "Column Names", "Count", "Status"]
        headers = ["Database", "Table Name", "Column Names", metric_column_name, "Status"]
        col_widths = [40, 40, 40, 40, 25]

        self.pdf.set_font("DejaVu", size=12)
        for i, header in enumerate(headers):
            self.pdf.cell(col_widths[i], 10, header, border=1, align="C")
        self.pdf.ln()

        # Table Rows
        for row in results:
            self.pdf.cell(col_widths[0], 10, str(row.get("Database", "")), border=1)
            self.pdf.cell(col_widths[1], 10, str(row.get("Table_name", "")), border=1)
            # self.pdf.cell(col_widths[2], 10, str(row.get("Column_names", "")), border=1)

            # Wrap text for Column Names
            col_names = str(row.get("Column_names", ""))
            x_before = self.pdf.get_x()
            y_before = self.pdf.get_y()
            self.pdf.multi_cell(col_widths[2], 10, col_names, border=1)
            # Move cursor back to the right of Column Names cell
            self.pdf.set_xy(x_before + col_widths[2], y_before)

            #self.pdf.cell(col_widths[3], 10, str(row.get("DUPLICATE_Count", "")), border=1, align="C")
            #self.pdf.cell(col_widths[3], 10, str(row.get("Check", "")), border=1, align="C")
            if "Duplicate" in check_type:
                #metric_col_name = "Duplicate Count"
                metric_key = "DUPLICATE_Count"
            elif "Null" in check_type:
                #metric_col_name = "Null Count"
                metric_key = "Null_Count"
            elif "Count" in check_type:
                #metric_col_name = "Count Difference"
                metric_key = "Count_Check"
            else:
                #metric_col_name = "Metric"
                metric_key = "Metric"
            self.pdf.cell(col_widths[3], 10, str(row.get(metric_key, "")), border=1, align="C")

            # Unicode icons ✔ / ✘
            status_icon = "✔" if row.get("IsCheckPassed", False) else "✘"
            self.pdf.cell(col_widths[4], 10, status_icon, border=1, align="C")
            self.pdf.ln()

        # Generate timestamped filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(
        self.output_folder, f"{check_type.replace(' ', '_')}_Report_{timestamp}.pdf"
        )

        # Save PDF
        self.pdf.output(output_file)
        return output_file


# Additional class for count check PDF generation
# This can be used if you want to separate count check reports from other types
class CountCheckPDFGenerator:
    def __init__(self, output_path="Reports",font_path_="DejaVuSans.ttf"):
        self.output_path = output_path
        os.makedirs(output_path, exist_ok=True)
        self.pdf = FPDF()
        self.pdf.set_auto_page_break(auto=True, margin=15)
        self.font_path = font_path_

        # Add Unicode font
        if not os.path.exists(self.font_path):
            raise FileNotFoundError(f"Font file not found: {self.font_path}")
        
        self.pdf.add_font("DejaVu", "", self.font_path, uni=True)
        self.pdf.add_font("DejaVu", "B", self.font_path, uni=True)

    def generate_count(self, results, file_name_prefix="Count_Check_Report"):
        
        # Add timestamp to file name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"{file_name_prefix}_{timestamp}.pdf"
        
        self.pdf.add_page()
        self.pdf.set_font("Dejavu", "B", 16)
        self.pdf.cell(0, 10, "Count Check Report", ln=True, align="C")

        self.pdf.ln(10)
        self.pdf.set_font("Dejavu", "B", 10)

        # Table headers
        col_widths = [25, 25, 25, 25, 25, 25,25]
        headers = ["Source Table", "Source Count", "Stage Table", "Stage Count", "Target Table", "Target Count", "Status"]

        for i, header in enumerate(headers):
            self.pdf.cell(col_widths[i], 10, header, border=1, align="C")
        self.pdf.ln()

        # for width, header in zip(col_widths,headers):
        #     self.pdf.cell(width, 10, header, border=1, align="C")
        # self.pdf.ln()

        self.pdf.set_font("Dejavu", "", 8)

        # Table rows
        for row in results:
            self.pdf.cell(col_widths[0], 10, str(row.get("Source_Table", "")), border=1, align="C")
            self.pdf.cell(col_widths[1], 10, str(row.get("Source_Count", "")), border=1, align="C")
            self.pdf.cell(col_widths[2], 10, str(row.get("Stage_Table", "")), border=1, align="C")
            self.pdf.cell(col_widths[3], 10, str(row.get("Stage_Count", "")), border=1, align="C")
            self.pdf.cell(col_widths[4], 10, str(row.get("Target_Table", "")), border=1, align="C")
            self.pdf.cell(col_widths[5], 10, str(row.get("Target_Count", "")), border=1, align="C")
            #self.pdf.ln()

            # Unicode icons ✔ / ✘
            status_icon = "✘" if row.get("IsCheckPassed", False) else "✔"
            self.pdf.cell(col_widths[4], 10, status_icon, border=1, align="C")
            self.pdf.ln()

        # Save file
        pdf_path = os.path.join(self.output_path, file_name)
        self.pdf.output(pdf_path)
        return pdf_path
