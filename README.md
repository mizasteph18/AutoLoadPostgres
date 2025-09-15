# AutoLoadPostgres

PostgreSQL Intelligent Loader System

Overview

A robust, automated ETL solution for loading structured data files into PostgreSQL with advanced features including schema management, duplicate detection, change tracking, and Microsoft Teams notifications. Designed for reliable, scheduled execution in Windows environments.

Key Features

· Automated Schema Management: Auto-generates and updates mapping files
· Intelligent Change Detection: Processes only new or modified files
· Duplicate Handling: Identifies and exports duplicate records with resolution guidance
· Multiple Processing Modes: Insert, Audit, and Cancel & Replace modes
· Microsoft Teams Integration: Real-time notifications for run status and errors
· Concurrency Protection: Prevents multiple simultaneous runs
· Progress Tracking: Resumes from last processed state
· Comprehensive Logging: Detailed processing logs with run IDs

Installation

Prerequisites

· Python 3.8+
· PostgreSQL 9.5+
· Microsoft Teams channel (for notifications)
· Windows Task Scheduler (for automated runs)

Setup

1. Clone or download the script
   ```bash
   # Create project directory
   mkdir C:\ETL_Loader
   cd C:\ETL_Loader
   ```
2. Install required packages
   ```bash
   pip install pandas psycopg2-binary pyyaml requests
   ```
3. Create sample configuration
   ```bash
   python postgres_loader.py
   ```

Configuration

Database Connection

Edit the main execution block with your PostgreSQL credentials:

```python
db_config = {
    "dbname": "your_database",
    "user": "your_username",
    "password": "your_password",
    "host": "localhost",
    "port": 5432
}
```

Global Configuration

Modify global_loader_config.yaml:

```yaml
batch_size: 1000
max_connections: 5
min_connections: 1
retry_attempts: 3
enable_progress_tracking: true
enable_data_validation: true
timestamp_tolerance_seconds: 1.0
global_hash_exclude_columns: []
teams_webhook: "https://yourcompany.webhook.office.com/..."  # Teams webhook URL
lock_timeout: 3600  # Lock file timeout in seconds
```

Processing Rules

Create rule files in the rules directory:

Example: sales_rule.yaml

```yaml
directory: "./sales_data"
file_pattern: "^sales_report_(\\d{8})\\.csv$"
date_format: "%Y%m%d"
date_from_filename_col_name: "report_date"
start_row: 0
start_col: 0
mode: "cancel_and_replace"
hash_exclude_columns: []
```

File Structure

```
C:\ETL_Loader\
├── postgres_loader.py          # Main script
├── global_loader_config.yaml   # Global configuration
├── processing.log              # Log file (auto-generated)
├── processing_progress.json    # Progress tracking (auto-generated)
├── loader.lock                 # Lock file (auto-generated)
├── rules\                      # Processing rules directory
│   ├── sales_rule.yaml
│   ├── inventory_rule.yaml
│   └── financial_rule.yaml
├── sales_data\                 # Example data directory
│   ├── sales_mapping.csv       # Auto-generated mapping
│   └── sales_report_20230101.csv
├── inventory_data\             # Another data directory
│   ├── inventory_mapping.csv
│   └── inventory_2023-01-01.xlsx
└── duplicates\                 # Auto-created for duplicate files
    └── sales_report_20230101.csv
```

Usage

Manual Execution

```bash
# Run once
python postgres_loader.py

# Run with custom config
python postgres_loader.py --config custom_config.yaml
```

Scheduled Execution with Windows Task Scheduler

1. Create a batch file (run_loader.bat):
   ```batch
   @echo off
   cd C:\ETL_Loader
   C:\Python39\python.exe postgres_loader.py
   ```
2. Open Task Scheduler and create a new task:
   · General tab:
     · Name: "PostgreSQL ETL Loader"
     · Description: "Automated data loading process"
     · Security options: "Run whether user is logged on or not"
   · Triggers tab:
     · New → Daily
     · Repeat task every: 2 hours
     · Duration: Indefinitely
   · Actions tab:
     · Program/script: C:\ETL_Loader\run_loader.bat
     · Start in: C:\ETL_Loader
   · Conditions tab:
     · Wake the computer to run this task: Enabled
   · Settings tab:
     · Allow task to be run on demand: Enabled
     · Stop if running longer than: 1 hour
     · If task fails, restart every: 5 minutes (max 3 times)

Microsoft Teams Integration

1. Create an Incoming Webhook in your Teams channel:
   · Right-click channel → Connectors → Incoming Webhook
   · Configure with name "ETL Loader"
   · Copy the webhook URL
2. Add the webhook URL to your global config:
   ```yaml
   teams_webhook: "https://yourcompany.webhook.office.com/..."
   ```
3. Notification types you'll receive:
   · Run start/end notifications
   · New column detection alerts
   · Validation errors
   · Duplicate file detection
   · Processing errors
   · Database connection issues

Mapping Files

Mapping files define how source columns map to database tables:

Column Description Example
RawColumn Source column name "Product Name"
TargetColumn Database column name "product_name"
DataType PostgreSQL data type "TEXT"
LoadFlag Load inclusion flag ('Y', 'N', or '') "Y"
IndexColumn Business key flag "N"
data_source 'file' or 'system' "file"
order Column position 2

Processing Modes

1. Insert Mode: Adds new records only
2. Audit Mode: Updates changed records only (based on content hash)
3. Cancel & Replace Mode: Full refresh of file data

Troubleshooting

Common Issues

1. Database Connection Failed
   · Check PostgreSQL service is running
   · Verify credentials in db_config
   · Ensure network connectivity
2. New Columns Blocking Processing
   · Edit mapping file: set LoadFlag to 'Y' or 'N'
   · Ensure all columns have non-empty LoadFlag values
3. Lock File Issues
   · Script will auto-remove stale locks (>1 hour old)
   · Manually delete loader.lock if needed
4. Teams Notifications Not Working
   · Verify webhook URL is correct
   · Check network connectivity to Teams service

Logs

· Check processing.log for detailed execution history
· Each run has a unique ID for tracking
· Logs include timing information and error details

Support

For issues and questions:

1. Check the processing.log for error details
2. Verify database connectivity
3. Ensure all mapping files are properly configured
4. Confirm file permissions for the service account

Performance Tips

1. For large files, increase batch_size in global config
2. Exclude volatile columns from hashing to improve performance
3. Use appropriate processing mode for your use case:
   · Audit mode for incremental updates
   · Cancel & Replace for full refreshes
4. Schedule runs during off-peak hours for large datasets

License

This script is provided as-is for educational and commercial use. Please ensure you have appropriate licenses for all dependencies.