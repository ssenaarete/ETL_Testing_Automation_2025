from src.ETL_log_table_validations import ETLLog_Validation

def test_ETLLog_Table_validation(config_loader):
    validator = ETLLog_Validation(config_loader)
    validator.run()