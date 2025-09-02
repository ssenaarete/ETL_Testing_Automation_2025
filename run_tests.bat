@echo off
SETLOCAL

REM === Set database argument (default SOURCEDB if not given) ===
SET DB=%1
IF "%DB%"=="" SET DB=SOURCEDB

REM === Run pytest with Allure ===
echo Running pytest with DB=%DB%...
call pytest -v tests/ --db %DB% --alluredir="Reports\allure-results"
echo Pytest exited with code %ERRORLEVEL%
REM Force continue even if pytest fails
cmd /c exit 0

REM === Copy Allure history for trends ===
if exist "Reports\allure-report\history" (
    echo Copying history from previous report...
    xcopy /E /I /Y "Reports\allure-report\history" "Reports\allure-results\history" >nul
)

REM === Generate Allure report ===
echo Generating Allure report...
allure generate "Reports\allure-results" -o "Reports\allure-report" --clean

REM === Open Allure report in browser ===
echo Opening Allure report in browser...
start "" "Reports\allure-report\index.html"

:end
echo Done!
ENDLOCAL
