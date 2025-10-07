from src.exclusion_etl_batch_columns_in_views import ExclusionETLBatchColumnsInViews

def test_exclusion_of_etl_batch_columns_in_views(config_loader):
    validator = ExclusionETLBatchColumnsInViews(config_loader)
    validator.run()