import os
import shutil
import allure

class ExcelReportHelper:
    def __init__(self, allure_report_dir="Reports/allure-report"):
        self.allure_report_dir = allure_report_dir
        self.excel_dir = os.path.join(allure_report_dir, "excel")
        os.makedirs(self.excel_dir, exist_ok=True)

    def attach_excel(self, excel_path, link_name="Excel Report"):
        """
        Copy Excel to allure-report/excel/ and attach a clickable link.
        """
        if not os.path.exists(excel_path):
            raise FileNotFoundError(f"Excel file not found: {excel_path}")

        # Copy Excel file to allure-report/excel
        dest_path = os.path.join(self.excel_dir, os.path.basename(excel_path))
        shutil.copy(excel_path, dest_path)

        # Create relative path for link
        relative_path = f"excel/{os.path.basename(excel_path)}"
        link_html = f'<a href="{relative_path}" target="_blank">{link_name}</a>'

        # Attach clickable link to Allure
        allure.attach(
            link_html,
            name=link_name,
            attachment_type=allure.attachment_type.HTML
        )