param(
    [string]$db = "SOURCEDB"   # default DB if not provided
)

# ==============================
# Run ETL Test Suite with Allure
# ==============================

# Paths
$resultsDir = "reports/allure-results"
$reportDir  = "reports/allure-report"

Write-Host "Preparing Allure Results Directory..."

# Create results dir if not exists
if (-Not (Test-Path $resultsDir)) {
    New-Item -ItemType Directory -Path $resultsDir | Out-Null
}

# Copy history from previous report if available
if (Test-Path "$reportDir/history") {
    Write-Host "Copying history from previous report..."
    Copy-Item "$reportDir/history" "$resultsDir/history" -Recurse -Force
}

# Run pytest with allure results
Write-Host "Running Pytest against DB: $db"
pytest -v tests/test_smoke_suite.py --db $db --alluredir=$resultsDir

# Generate Allure report
Write-Host "Generating Allure Report..."
allure generate $resultsDir -o $reportDir --clean

# Open Allure report in browser
Write-Host "Serving Allure Report..."
allure serve $resultsDir
