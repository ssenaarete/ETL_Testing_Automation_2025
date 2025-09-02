import pytest
import configparser
import allure
from datetime import datetime
from utils.db_helper import DBHelper
from utils.config_loader import ConfigLoader
from utils.report_helper import ReportHelper
from utils.attach_excel_report_helper import ExcelReportHelper


def pytest_addoption(parser):
    parser.addoption(
        "--db", action="store", default=None,
        help="Database section name from config.ini (e.g., SOURCEDB, STAGEDB, TARGETDB)"
    )
#---------------------------------------------------------------------------------------------
def pytest_collection_modifyitems(config, items):
    db = config.getoption("--db")
    if db and db.upper() == "SOURCEDB":
        skip_marker = pytest.mark.skip(reason="Not applicable for Source DB")
        for item in items:
            if "not_for_source" in item.keywords:
                item.add_marker(skip_marker)

#---------------------------------------------------------------------------------------------

@pytest.fixture()
def db_name(request):
    return request.config.getoption("--db").upper()

#---------------------------------------------------------------------------------------------

@pytest.fixture(scope="session")
def config_loader(request):
    """Fixture to load config once per test session"""
    db_name = request.config.getoption("--db")   # e.g. SOURCEDB
    return ConfigLoader("config.ini", section_name=db_name)


#---------------------------------------------------------------------------------------------

@pytest.fixture()
def source_db():
    db = DBHelper.from_config_section("config.ini", "SOURCEDB")
    db.connect()
    yield db
    db.close()

#---------------------------------------------------------------------------------------------

@pytest.fixture()
def stage_db():
    db = DBHelper.from_config_section("config.ini", "STAGEDB")
    db.connect()
    yield db
    db.close()

#---------------------------------------------------------------------------------------------

@pytest.fixture()
def target_db():
    db = DBHelper.from_config_section("config.ini", "TARGETDB")
    db.connect()
    yield db
    db.close()

#---------------------------------------------------------------------------------------------

# @pytest.fixture()
# def config_loader():
#     """Provide ConfigLoader instance (dialog or silent mode)"""
#     return ConfigLoader("config.ini")  # dialog box appears


@pytest.fixture()
def excel_data():
    """Load Excel file from config.ini and return dataframe"""
    config = configparser.ConfigParser()
    config.read("config.ini")
    # file_path = config.get("PATHS", "excel_file_path")
    # return pd.read_excel(file_path)
    return config.get("PATHS", "excel_file_path")

#---------------------------------------------------------------------------------------------

@pytest.fixture()
def report_helper():
    """Provide ReportHelper instance"""
    return ReportHelper(config_path="config.ini")

#---------------------------------------------------------------------------------------------

# @pytest.fixture(scope="session", autouse=True)
# def allure_metadata():
#     allure.dynamic.label("framework", "ETL Test Automation Framework")
#     allure.dynamic.label("executed_by", "Surojit Sen")
#     allure.dynamic.label("executed_on", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
#     yield

#---------------------------------------------------------------------------------------------


@pytest.fixture()
def excel_helper():
    return ExcelReportHelper()

def _get_excel_path():
    config = configparser.ConfigParser()
    config.read("config.ini")
    try:
        return config.get("PATHS", "excel_file_path")
    except Exception:
        return None

@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    rep = outcome.get_result()

    # 🔹 Attach Excel for both pass & fail, but only once (after "call" phase)
    if rep.when == "call":
        excel_path = _get_excel_path()
        if excel_path:
            excel_helper = item.funcargs.get("excel_helper", None)
            if excel_helper:
                try:
                    # Build readable link name per test
                    test_name = item.originalname or item.name
                    pretty_name = test_name.replace("_", " ").title()
                    link_name = f"{pretty_name} Excel"

                    excel_helper.attach_excel(excel_path, link_name=link_name)
                except Exception as e:
                    print(f"⚠️ Could not attach Excel for {item.name}: {e}")