# import pytest
# import logging
# import sys

# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# if __name__ == "__main__":
#     print("🔍 Starting ETL Test Automation Suite...\n")

#     # Run pytest on all tests
#     exit_code = pytest.main([
#         "-v",          # verbose mode
#         "--tb=short",  # shorter tracebacks
#         "Test_cases"   # folder where test_count_validation.py lives
#     ])

#     print("\n✅ ETL Test Automation Suite completed!")
#     print("📊 Check the generated reports for details.")

#     sys.exit(exit_code)



import pytest
import os
import sys

# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def main():
    print("🔍 Starting ETL Test Automation Suite...\n")

    # Locate the tests folder relative to this file
    project_root = os.path.dirname(os.path.abspath(__file__))
    tests_dir = os.path.join(project_root, "test_count_validation.py")

    # if not os.path.exists(tests_dir):
    #     print(f"❌ Tests folder not found: {tests_dir}")
    #     return
    
    # Run pytest programmatically
    # -v = verbose, --tb=short = shorter tracebacks
    exit_code = pytest.main([tests_dir, "-v", "--tb=short"])

    if exit_code == 0:
        print("\n✅ ETL Test Automation Suite completed successfully!")
    else:
        print("\n❌ ETL Test Automation Suite completed with failures.")

    print("📊 Check the generated reports for details.")

if __name__ == "__main__":
    main()
