# PM2.5 Monitoring Program

In this program, a final report shows 
- a list of times when the level is above the danger threshold of 30
- the daily maximum, daily minimum, and daily average pollution value

## The workflow is
1. **Data Acquisition**: read the data for a device using the /device/<device_id>/history/ endpoint
2. **Data Backup**: save the raw data into SQLite with the appropriate schema
3. **Data Transformation**: 
- Keep the report-needed columns
- Generate the list of times when PM2.5 is over 30
- Calculate the daily maximum, daily minimum, and daily average pollution value
4. **Data Validation**: 
- make sure the level is over 30 for the list of time
- datatype keeps the same
- no duplicate records
5. **Data Reporting**: Power BI visualises the report-ready data and delivers insights

## Infrastructure
Used Techniques are:
- Data Orchestration + Containerization: Astro CLI (Apache Airflow + Docker Compose)
- Get data from API: Python
- Data storage: SQLite
- Data transformation + test: dbt

## How to run it
### Option 1 CodesSpaces (Linux environment)
#### Getting Start
- Step 1 Install **Astro Cli**
```bash
curl -sSL install.astronomer.io | sudo bash -s
```
- **Step 2 Create a Python virtual environment and activate it**
```bash
cd airflow/
# Setup Python, install python3-venv package
sudo apt-get update
sudo apt-get install python3-venv
# Create the virtual environment and activate it
python3 -m venv .venv
source .venv/bin/activate
```

- **Step 3 Start the containers to run Airflow**
```bash
astro dev start
```

- Go to port 8080 to check the data pipeline in the UI
URL: http://localhost:8080

### Option 2 Local Run (Windows environment)
#### Prerequisites
1. Docker (Link: https://docs.docker.com/get-docker/)
2. Python 3.8 or later
3. `pip` and `virtualenv` for Python
4. Clone this repository:
   ```bash
   git clone https://github.com/xiangivyli/pm25_monitoring.git
   cd pm25_monitoring
   ```

#### Getting Start, instructions use Git Bash to run these commands
1. Setup Python virtual environment
```bash
python -m venv .venv
source .venv/Scripts/activate
```
2. Install Astro Cli
```bash
cd airflow
winget install -e --id Astronomer.Astro
```
3. Run `astro dev start` in the airflow folder
4. After your Astro project has started. View the Airflow UI at `localhost:8080`.
5. View the streamlit app at `localhost:8501`.

  





