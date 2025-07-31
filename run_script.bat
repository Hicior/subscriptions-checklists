@echo off
echo Enhanced Subscriptions & Invoices Data Fetcher
echo =============================================

REM Check if virtual environment exists
if not exist "venv" (
    echo Creating virtual environment...
    python -m venv venv
)

REM Activate virtual environment
call venv\Scripts\activate

REM Install requirements
echo Installing requirements...
pip install -r requirements.txt

REM Prompt for API keys if not set
if "%CALENDESK_API_KEY%"=="" (
    set /p CALENDESK_API_KEY="Enter your Calendesk API Key: "
)

if "%STRIPE_API_KEY%"=="" (
    set /p STRIPE_API_KEY="Enter your Stripe API Key: "
)

echo.
echo Starting data fetch process...
echo ==============================

REM Run the enhanced script
python enhanced_data_fetcher.py

echo.
echo Process completed. Press any key to exit...
pause >nul 