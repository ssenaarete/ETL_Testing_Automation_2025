from src.Job_Run_validation import JobExecutionValidation

def test_Job_Execution_validation(config_loader):
    validator = JobExecutionValidation(config_loader)
    validator.run()