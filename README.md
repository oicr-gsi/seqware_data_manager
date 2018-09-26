# seqware_data_manager

A project to collect seqware data management tasks.  Tasks include:
- updating analysis to a new lims provider

# Installation
```
/usr/bin/python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

# General usage
```
source ./venv/bin/activate
python cli.py --help
```

## Tasks
* load-analysis-lims-data: Task to extract and export analysis and lims data to a file
* change-analysis-lims-provider: Task to summarize changes and generate updates for analysis lims provider updates
