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
#### Prerequisites
1. **Anaconda**
- Installation
```bash
# create a temporary directory 
mkdir -p ~/tmp
cd ~/tmp/
# download the installer
wget https://repo.anaconda.com/archive/Anaconda3-2024.02-1-Linux-x86_64.sh
# run the installer
bash Anaconda3-2024.02-1-Linux-x86_64.sh
```
Enter `yes` after reviewing the license agreement
Press ENTER to confirm an installation location

- Initialise Anaconda
```bash
conda init
# reload the shell configuration
source ~/.bashrc
# activate the base environment
conda activate
# verify the installation
conda --version
```

- Remove the Anaconda installer from tmp folder to free some space
```bash
rm ~/tmp/Anaconda3-2024.02-1-Linux-x86_64.sh
```
2. **Docker**
Step 1: `apt-get` gets docker package 
- Installation
```bash
sudo apt-get update
sudo apt-get install containerd
sudo apt-get install docker.io
```

Step 2: Add user to docker group (remove `sudo` for docker commands) [reference](https://github.com/sindresorhus/guides/blob/main/docker-without-sudo.md)
```bash
sudo groupadd docker
sudo gpasswd -a $USER docker
```
**LOG OUT AND LOG BACK** the codespace to refresh group membership

Restart the `docker` daemon
```bash
sudo service restart docker
```

Test docker without sudo
```bash
docker run hello-world
```
3. **Python Virtual Environment** and **Astro Cli**
- Step 1 Direct to the folder 
```bash
cd /workspaces/pm25_monitoring
```

- Step 2 Create a Python virtual environment and activate it
```bash
conda create -n airflow-env python=3.8
```
```bash
conda activate airflow-env
```

- Step 3 Install **Astro Cli**
```bash
curl -sSL install.astronomer.io | sudo bash -s
```

#### Getting start

- Go to Airflow folder amd start to run
```bash
cd /workspaces/pm25_monitoring/airflow
```
```bash
astro dev start
```

- Go to port 8080 to check the data pipeline
URL: https://localhost:8080






