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
