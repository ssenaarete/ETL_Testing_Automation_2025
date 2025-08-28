import pytest
import configparser
from utils.db_helper import DBHelper
from utils.config_loader import ConfigLoader
from utils.report_helper import ReportHelper


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
