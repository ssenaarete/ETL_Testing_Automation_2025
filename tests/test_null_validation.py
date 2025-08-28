from src.null_validation import NullValidation

def test_null_validation(config_loader):
    validator = NullValidation(config_loader)
    validator.run()


#---------------------------------------------------------------------------------------------

# import allure

# @allure.suite("Smoke Suite")
# @allure.title("Null Validation - Source to Stage")
# def test_null_validation(report_helper):
#     # Run your validation
#     # Assume Excel is generated at reports/excel/Null_Mismatch.xlsx
#     excel_path = "reports/excel/Null_Mismatch.xlsx"

#     with open(excel_path, "rb") as f:
#         allure.attach(
#             f.read(),
#             name="Mismatch Excel",
#             attachment_type=allure.attachment_type.XML  # or TEXT if CSV
#         )

#     assert True  # replace with real validation result