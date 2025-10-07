from src.ETLProcess_vs_Details_log_validation import Process_vs_Detail_log_Validation

def test_ETLProcess_vs_Details_log_validation(config_loader):
    validator = Process_vs_Detail_log_Validation(config_loader)
    validator.run()