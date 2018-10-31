# seqware_data_manager

A project to collect seqware data management tasks.

# Requirements
- Python 3.6+
- virtualenv

# Installation
```
# initialize virtualenv (python 3.6+ is required)
/usr/bin/python3 -m venv venv

# or specify alternate python3.6+ path
# /.mounts/labs/PDE/Modules/sw/python/Python-3.6.4/bin/python3.6 -m venv venv

# activate virtualenv
source venv/bin/activate

# install dependencies
pip install -r requirements.txt

# run tests
python -m pytest
```

# General usage
```
source ./venv/bin/activate
python cli.py --help
```

## Tasks
* load-analysis-lims-data: Task to extract and export analysis and lims data to a file
* change-analysis-lims-provider: Task to summarize changes and generate updates for analysis lims provider updates
