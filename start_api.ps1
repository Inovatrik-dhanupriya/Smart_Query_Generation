# Start the FastAPI backend
# Usage: .\start_api.ps1
Set-Location "$PSScriptRoot\nl_to_sql"
& "$PSScriptRoot\venv\Scripts\python.exe" -m uvicorn main:app --reload --port 8000
