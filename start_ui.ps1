# Start the Streamlit frontend
# Usage: .\start_ui.ps1
Set-Location "$PSScriptRoot\nl_to_sql"
& "$PSScriptRoot\venv\Scripts\python.exe" -m streamlit run ui/streamlit_app.py
