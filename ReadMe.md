PostgreSQL Data Loader - Complete User Guide (Enhanced with Failed Rows Recovery & Timestamped Logs)

🚀 Overview & Enhanced Directory Structure

Enhanced Organized Directory Structure with Failed Rows Recovery & Logs

```
data_loader/
├── loader_script.py              # Main loader script
├── global_loader_config.yaml     # Global configuration
├── processing_progress.json      # Progress tracking
├── loader.lock                   # Lock file (auto-generated)
├── logs/                         # 📁 NEW: Timestamped log files
│   ├── processing_20231201_143022.log
│   ├── processing_20231201_153045.log
│   └── processing_latest.log     # Symlink to latest log
├── rules/                        # ✅ ALL configuration files
│   ├── sales_rule.yaml
│   ├── inventory_rule.yaml
│   ├── weekly_rule.yaml
│   ├── sales_mapping.csv         # ✅ Mapping files in rules folder
│   ├── inventory_mapping.csv
│   └── weekly_mapping.csv
├── inputs/                       # ✅ ALL input data directories
│   ├── sales_data/
│   ├── inventory_data/
│   ├── weekly_reports/
│   └── custom_reports/
├── duplicates/                   # Auto-generated directories
│   ├── to_process/               # ✅ CORRECTED: Files to reprocess
│   └── processed/                # ✅ ENHANCED: Moved after processing
├── format_conflict/              # Auto-generated directories
│   ├── to_process/               # ✅ CORRECTED: Files to reprocess
│   └── processed/                # ✅ ENHANCED: Moved after processing
└── failed_rows/                  # ✅ NEW: Failed rows recovery
    ├── to_process/               # ✅ CORRECTED: Files to reprocess
    └── processed/                # ✅ ENHANCED: Moved after processing
```

📋 Quick Decision Guide

Use this flowchart to navigate to the right sections:

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                    Getting Started                                                          │
└─────────────────────────────────────────────────┬───────────────────────────────────────────┘
                                                  │
┌─────────────────────────────────────────────────▼───────────────────────────────────────────┐
│  What type of data are you loading?                                                        │
└─────────────────────────────────┬─────────────────────────────────┬─────────────────────────┘
                                  │                                 │
              ┌───────────────────▼─────────────────┐ ┌─────────────▼─────────────┐ ┌─────────▼─────────┐
              │ Single File                         │ │ Multi-Sheet               │ │ Complex Data     │
              │ CSV/JSON                            │ │ Excel                     │ │ Issues           │
              └───────────────────┬─────────────────┘ └─────────────┬─────────────┘ └─────────┬─────────┘
                                  │                                 │                         │
              ┌───────────────────▼─────────────────┐ ┌─────────────▼─────────────┐ ┌─────────▼─────────┐
              │ Section 4                          │ │ Section 5                 │ │ Sections          │
              │ Basic Config                       │ │ Excel Multi-Sheet         │ │ 7-10 Conflict     │
              │                                    │ │                           │ │ Resolution        │
              └────────────────────────────────────┘ └───────────────────────────┘ └───────────────────┘
```

✨ Key Benefits of Enhanced Structure

✅ Simplified Setup

· One configuration location: All rules and mappings in rules/ folder
· One data location: All input files in inputs/ folder
· Clear separation: No mixing of configs and data
· Organized logs: All logs in logs/ folder with timestamps

✅ Enhanced Error Recovery

· NEW: Failed rows recovery with automatic export and reprocessing
· Duplicate detection and resolution
· Format conflict handling
· Consistent reprocessing workflow across all error types
· Automatic file movement to processed/ folders

✅ Enhanced Logging

· 📁 NEW: Logs stored in logs/ directory
· ⏰ Timestamped filenames: processing_YYYYMMDD_HHMMSS.log
· 🔗 Symlink: processing_latest.log always points to most recent log
· 📊 Comprehensive tracking: All processing activities logged

✅ Easier Maintenance

· Backup strategy: Backup rules/ folder separately from inputs/
· Version control: Only rules/ folder needs version control
· Permissions: Different permissions for configs vs data
· Log management: Organized log files with automatic rotation

✅ Better Organization

· Scalable: Easy to add new data sources
· Clear structure: Intuitive for new team members
· Standardized: Consistent across projects

✅ Enhanced Security

· Isolate credentials: Database config in rules/ folder
· Access control: Different access for configs vs data files
· Audit trail: Clear separation for compliance

---

1. Installation & Setup (Enhanced)

Quick Installation

```bash
# Install required packages
pip install pandas psycopg2-binary pyyaml numpy openpyxl

# Create enhanced organized directory structure
mkdir -p rules inputs/sales_data inputs/inventory_data inputs/weekly_reports
mkdir -p duplicates/to_process duplicates/processed
mkdir -p format_conflict/to_process format_conflict/processed
mkdir -p failed_rows/to_process failed_rows/processed  # ✅ NEW
mkdir -p logs  # 📁 NEW: Log directory

# Generate sample configuration
python loader_script.py
```

Enhanced Initial Setup Verification

```bash
# Test the enhanced directory structure
ls -la rules/ inputs/ failed_rows/ logs/

# Test database connection
python loader_script.py --test-connection

# Extract file naming patterns
python loader_script.py --extract-pattern "sales_20230101.csv"

# Run initial setup
python loader_script.py --setup

# Check latest logs
tail -f logs/processing_latest.log
```

Setup Script

setup_directories.sh

```bash
#!/bin/bash
echo "Creating enhanced PostgreSQL Data Loader directory structure..."

# Configuration directories
mkdir -p rules

# Input data directories
mkdir -p inputs/sales_data
mkdir -p inputs/inventory_data
mkdir -p inputs/weekly_reports
mkdir -p inputs/regional_reports
mkdir -p inputs/customer_data

# Processing directories
mkdir -p duplicates/to_process
mkdir -p duplicates/processed
mkdir -p format_conflict/to_process
mkdir -p format_conflict/processed
mkdir -p failed_rows/to_process
mkdir -p failed_rows/processed

# 📁 NEW: Log directory
mkdir -p logs

echo "Enhanced directory structure created successfully!"
echo "Logs will be stored in: logs/processing_YYYYMMDD_HHMMSS.log"
```

---

2. Configuration (Enhanced Structure)

Global Configuration (global_loader_config.yaml)

```yaml
# Database Connection
dbname: "your_database_name"
user: "your_username"
password: "your_password"
host: "localhost"
port: 5432

# Processing Settings
batch_size: 1000
max_connections: 5
retry_attempts: 3

# Features
enable_progress_tracking: true
enable_data_validation: true
auto_add_columns: true

# File Handling
delete_files: "N"
lock_timeout: 3600

# Empty Sheet Handling
skip_empty_sheets: true
warn_on_empty_sheets: true
treat_empty_as_error: false

# ✅ ENHANCED: Failed Rows Recovery Settings
enable_row_level_recovery: true
fail_on_partial_insert: false    # If true, fails when any rows fail
retry_on_deadlock: true
max_retry_delay: 30              # Maximum seconds between retries
enable_batch_validation: true    # Pre-validate data before insert
chunk_size: 100                  # Process rows in chunks
max_chunk_failures: 5            # Stop if too many chunks fail
```

Enhanced Processing Rules (Now in rules/ folder)

Basic Rule Example (rules/sales_rule.yaml)

```yaml
base_name: "sales"
directory: "inputs/sales_data"           # ✅ Updated path
file_pattern: "sales_\\d{8}\\.csv"
date_format: "%Y%m%d"
start_row: 0
start_col: 0
mode: "insert"
date_from_filename_col_name: "file_date"
hash_exclude_columns: []
search_subdirectories: true
mapping_file: "rules/sales_mapping.csv"  # ✅ Mapping file in rules folder
```

Multi-Sheet Example (rules/inventory_rule.yaml)

```yaml
base_name: "inventory"
directory: "inputs/inventory_data"       # ✅ Updated path
file_pattern: "inventory_\\d{8}\\.xlsx"
date_format: "%Y%m%d"
start_row: 0
start_col: 0
mode: "insert"
date_from_filename_col_name: "file_date"
hash_exclude_columns: []
search_subdirectories: true
sheet_config:
  processing_method: "multiple"
  sheet_names: 
    - "Sheet1"
    - "Sheet2"
mapping_file: "rules/inventory_mapping.csv"  # ✅ Mapping file in rules
```

All Sheets Example (rules/weekly_rule.yaml)

```yaml
base_name: "weekly_reports"
directory: "inputs/weekly_reports"
file_pattern: "weekly_\\d{8}\\.xlsx"
date_format: "%Y%m%d"
start_row: 0
start_col: 0
mode: "insert"
date_from_filename_col_name: "report_date"
hash_exclude_columns: []
search_subdirectories: true
sheet_config:
  processing_method: "all"
mapping_file: "rules/weekly_mapping.csv"
```

Mapping Files

Sales Mapping (rules/sales_mapping.csv)

```csv
RawColumn,TargetColumn,DataType,LoadFlag,IndexColumn,data_source,definition,order
OrderID,orderid,INTEGER,Y,Y,file,,0
Customer,customer,TEXT,Y,N,file,,1
Amount,amount,NUMERIC,Y,N,file,,2
loaded_timestamp,loaded_timestamp,TIMESTAMP,Y,N,system,,3
source_filename,source_filename,TEXT,Y,N,system,,4
content_hash,content_hash,TEXT,Y,N,system,,5
operation,operation,TEXT,Y,N,system,,6
```

Inventory Mapping (rules/inventory_mapping.csv)

```csv
RawColumn,TargetColumn,DataType,LoadFlag,IndexColumn,data_source,definition,order
ItemID,itemid,INTEGER,Y,Y,file,,0
ItemName,itemname,TEXT,Y,N,file,,1
Stock,stock,INTEGER,Y,N,file,,2
_source_sheet,_source_sheet,TEXT,Y,N,file,,3
loaded_timestamp,loaded_timestamp,TIMESTAMP,Y,N,system,,4
source_filename,source_filename,TEXT,Y,N,system,,5
content_hash,content_hash,TEXT,Y,N,system,,6
operation,operation,TEXT,Y,N,system,,7
```

---

3. Input Data Organization (Enhanced)

Enhanced Input Directory Structure

```
inputs/
├── sales_data/           # Sales-related files
│   ├── sales_20230101.csv
│   ├── sales_20230102.csv
│   └── archive/          # Optional subdirectories
├── inventory_data/       # Inventory files
│   ├── inventory_20230101.xlsx
│   └── inventory_20230102.xlsx
├── weekly_reports/       # Weekly reports
│   ├── weekly_20230101.xlsx
│   └── weekly_20230108.xlsx
├── customer_data/        # Additional data sources
└── financial_data/
```

File Placement Examples

Sales Data:

· ✅ inputs/sales_data/sales_20230101.csv
· ✅ inputs/sales_data/sales_20230102.csv

Inventory Data:

· ✅ inputs/inventory_data/inventory_20230101.xlsx
· ✅ inputs/inventory_data/monthly/inventory_january.xlsx (with subdirectories)

---

4. Multi-Sheet Excel Configuration

Configuration Decision Tree

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│          Excel File Processing Method Selection                                             │
└─────────────────────────────────────────────────┬───────────────────────────────────────────┘
                                                  │
        How many sheets do you need to process?
                                                  │
        ┌─────────────────────┬───────────────────┬─────────────────────────────────────────┐
        │                     │                   │                                         │
    ┌───▼─────────────┐   ┌───▼─────────────┐   ┌─▼─────────────┐   ┌───▼─────────────┐
    │ One             │   │ Few             │   │ All           │   │ By              │
    │ Sheet           │   │ Named           │   │ Sheets        │   │ Pattern         │
    └───┬─────────────┘   │ Sheets          │   └─┬─────────────┘   └───┬─────────────┘
        │                 └───┬─────────────┘     │                     │
    ┌───▼─────────────┐   ┌───▼─────────────┐   ┌─▼─────────────┐   ┌───▼─────────────┐
    │ specific        │   │ multiple        │   │ all           │   │ pattern         │
    │                 │   │                 │   │               │   │                 │
    └─────────────────┘   └─────────────────┘   └───────────────┘   └─────────────────┘
```

Single Sheet Processing

```yaml
sheet_config:
  processing_method: "specific"
  specific_sheet: "Monthly Data"  # Process only this sheet
```

Multiple Named Sheets - IMPORTANT: LIST FORMAT REQUIRED

```yaml
sheet_config:
  processing_method: "multiple"
  sheet_names: 
    - "Sales"
    - "Inventory" 
    - "Expenses"   # ✅ CORRECT: List format with dashes
```

All Sheets

```yaml
sheet_config:
  processing_method: "all"  # Process every sheet in the file
```

Pattern-Based Sheet Selection

```yaml
sheet_config:
  processing_method: "pattern"
  sheet_name_pattern: "Region_.*"  # Process sheets like "Region_North", "Region_South"
```

YAML List Format Guide

✅ CORRECT Formats:

```yaml
# Multi-line list (recommended)
sheet_names: 
  - "Sheet1"
  - "Sheet2"
  - "Sheet3"

# Inline list (also works)
sheet_names: ["Sheet1", "Sheet2", "Sheet3"]
```

❌ INCORRECT Formats:

```yaml
# This will NOT work - not a list
sheet_names: "Sheet1, Sheet2, Sheet3"

# This will NOT work - comma-separated string
sheet_names: "Sheet1", "Sheet2", "Sheet3"
```

---

5. Processing Modes

Mode Selection Guide

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│              Processing Mode Selection                                                      │
└─────────────────────────────────────────────────┬───────────────────────────────────────────┘
                                                  │
        What should happen when reprocessing the same file?
                                                  │
        ┌─────────────────────┬───────────────────┬─────────────────────────────────────────┐
        │                     │                   │                                         │
    ┌───▼─────────────┐   ┌───▼─────────────┐   ┌─▼─────────────┐   ┌───▼─────────────┐
    │ Insert          │   │ Replace         │   │ Audit         │   │ Complex         │
    │ Only            │   │ Entire          │   │ Check         │   │ Update          │
    │ New             │   │ File            │   │ Only          │   │ Logic           │
    └───┬─────────────┘   └───┬─────────────┘   └─┬─────────────┘   └───┬─────────────┘
        │                     │                   │                     │
    ┌───▼─────────────┐   ┌───▼─────────────┐   ┌─▼─────────────┐   ┌───▼─────────────┐
    │ insert          │   │ cancel_and      │   │ audit         │   │ Custom          │
    │                 │   │ replace         │   │               │   │ Script          │
    │                 │   │                 │   │               │   │ Required        │
    └─────────────────┘   └─────────────────┘   └───────────────┘   └─────────────────┘
```

Insert Mode

```yaml
mode: "insert"
```

Behavior: Only insert new records, ignore duplicates
Use Case: Daily incremental loads, append-only data

Cancel and Replace Mode

```yaml
mode: "cancel_and_replace"
```

Behavior: Delete all records from same source file, then insert new data
Use Case: Full refreshes, corrected data files

Audit Mode

```yaml
mode: "audit"
```

Behavior: Skip processing if file content unchanged in database
Use Case: Efficient processing of unchanged files, backup verification

---

6. Enhanced Error Recovery Workflows

NEW: Failed Rows Recovery Process

When database insertion fails for specific rows, the loader now provides comprehensive recovery:

Step 1: Automatic Detection & Export

· Failed rows automatically detected during insertion
· Exported to failed_rows/ with ORIGINAL filename and format (Excel/CSV preserved)
· Error details in _error_message and _failed_reason columns
· Guidance provided in _GUIDANCE column

Step 2: Manual Correction

1. Open failed rows file in failed_rows/ directory
2. Review error details in _error_message column
3. Fix data issues based on failure reason
4. Automatic metadata removal: Metadata columns automatically removed during reprocessing

Step 3: Reprocessing

1. Save corrected file with original filename
2. Move to failed_rows/to_process/ directory
3. Run loader - corrected data will be processed automatically
4. ✅ ENHANCED: File automatically moved to failed_rows/processed/ after processing

Failure Reason Codes

Code Description
DUPLICATE_KEY Violates unique constraint
MISSING_REFERENCE Violates foreign key constraint
DATA_TYPE_MISMATCH Invalid input syntax
MISSING_REQUIRED_VALUE Null value in required column
VALUE_TOO_LONG Value exceeds column length
DEADLOCK Database deadlock detected
CONNECTION_ISSUE Database connection problem
UNKNOWN_ERROR Other database errors

Duplicate Resolution Process

Step 1: Automatic Export

· Duplicates exported to duplicates/ directory with ORIGINAL filename and format
· Original filename preserved
· Metadata columns added for resolution guidance

Step 2: Manual Resolution

1. Open duplicate file in duplicates/ directory
2. Review conflict types using _conflict_type column
3. Follow guidance in _GUIDANCE column
4. Resolve conflicts according to business rules
5. Automatic metadata removal: Metadata columns automatically removed during reprocessing

Step 3: Reprocessing

1. Save cleaned file with original filename
2. Move to duplicates/to_process/ directory
3. Run loader - it will automatically detect and process
4. ✅ ENHANCED: File automatically moved to duplicates/processed/ after processing

Format Conflict Resolution Process

Step 1: Automatic Export

· Conflicting rows exported to format_conflict/ directory with ORIGINAL filename and format
· Detailed error information in _conflict_details column
· Guidance provided in _GUIDANCE column

Step 2: Data Correction

1. Identify problematic values using _conflict_details
2. Correct data types - ensure numeric fields contain numbers, dates are valid, etc.
3. Automatic metadata removal: Metadata columns automatically removed during reprocessing

Step 3: Reprocessing

1. Save corrected file with original filename
2. Move to format_conflict/to_process/ directory
3. Run loader - corrected data will be processed
4. ✅ ENHANCED: File automatically moved to format_conflict/processed/ after processing

---

7. Data Quality & Validation (Enhanced)

Automatic Data Type Validation

The loader validates each column against its expected data type:

Validation Rules

· Numeric Columns: Must contain valid numbers or NULL
· Date Columns: Must be parsable dates or NULL
· Boolean Columns: Must be true/false, 1/0, or common boolean representations
· Text Columns: Any value accepted, but length may be constrained

Enhanced Insertion Error Handling

The loader now provides:

· Chunked processing with configurable chunk sizes
· Automatic retry on deadlocks and connection issues
· Row-level error tracking and reporting
· Configurable failure thresholds

Format Conflict Detection

When data doesn't match expected types, the loader:

1. Identifies conflicting rows
2. Separates them from clean data
3. Exports conflicts for manual correction (preserving original file format)
4. Processes only clean data

Duplicate Detection

Duplicate Types

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                    Duplicate Detection                                                      │
└─────────────────────────────────────────────────┬───────────────────────────────────────────┘
                                                  │
        What type of duplicate was found?
                                                  │
        ┌─────────────────────┬───────────────────┬─────────────────────────────────────────┐
        │                     │                   │                                         │
    ┌───▼─────────────┐   ┌───▼─────────────┐   ┌─▼─────────────┐   ┌───▼─────────────┐
    │ Exact           │   │ Business        │   │ Mixed         │   │ Unknown         │
    │ Match           │   │ Key             │   │ Both          │   │ Type            │
    │                 │   │ Conflict        │   │ Types         │   │                 │
    └───┬─────────────┘   └───┬─────────────┘   └─┬─────────────┘   └───┬─────────────┘
        │                     │                   │                     │
    ┌───▼─────────────┐   ┌───▼─────────────┐   ┌─▼─────────────┐   ┌───▼─────────────┐
    │ EXACT_DUPLICATE  │   │ BUSINESS_KEY_   │   │ EXACT_AND_    │   │ UNKNOWN_CONFLICT│
    │                 │   │ CONFLICT         │   │ BUSINESS_     │   │                 │
    │                 │   │                  │   │ CONFLICT      │   │                 │
    └─────────────────┘   └──────────────────┘   └───────────────┘   └─────────────────┘
```

Business Key Configuration

Configure business keys in your mapping file:

```csv
RawColumn,TargetColumn,DataType,LoadFlag,IndexColumn,data_source
order_id,order_id,INTEGER,Y,Y,file
customer_id,customer_id,INTEGER,Y,Y,file
order_date,order_date,DATE,Y,N,file
amount,amount,NUMERIC,Y,N,file
```

---

8. Advanced Features (Enhanced)

Enhanced Progress Tracking & Resume Capability

The loader uses a hybrid approach:

· File hashes for content changes
· Timestamps for quick unchanged detection
· Configuration hashes for rule changes
· Sheet-level tracking for multi-sheet Excel files

Benefits:

· Resume interrupted processing
· Skip unchanged files efficiently
· Handle configuration changes intelligently
· Track individual sheet processing

Automatic Schema Evolution

When new columns are detected:

1. Mapping file is updated automatically with correct positions
2. Database table is altered (if auto_add_columns: true)
3. Processing continues with new schema

Enhanced Concurrent Run Prevention

· Lock file prevents multiple instances
· Configurable timeout for stale locks
· Automatic cleanup on normal exit
· Stale lock detection and removal

Chunked Processing & Error Recovery

· Process large files in configurable chunks
· Automatic retry on database errors
· Row-level error tracking and export
· Configurable failure thresholds

Enhanced Logging System

· 📁 Organized Storage: All logs in logs/ directory
· ⏰ Timestamped Files: processing_YYYYMMDD_HHMMSS.log
· 🔗 Latest Access: processing_latest.log symlink
· 📊 Comprehensive Coverage: All processing activities logged

---

9. Troubleshooting Guide (Enhanced)

Common Issues & Solutions

Database Connection Issues

```bash
# Test connection manually
psql -h localhost -U your_username -d your_database

# Check configuration
cat global_loader_config.yaml | grep -E "(host|port|dbname|user)"

# Check logs
tail -f logs/processing_latest.log
```

File Processing Issues

```bash
# Check file permissions
ls -la inputs/sales_data/

# Verify file format
file inputs/sales_data/sales_20230101.csv
head -5 inputs/sales_data/sales_20230101.csv

# Check logs for detailed errors
grep -i "error" logs/processing_latest.log
```

Multi-Sheet Configuration Issues

Problem: Excel file processed but only some sheets loaded
Solution: Check sheet_names is in correct LIST format in YAML

Problem: "Sheet not found" errors
Solution: Verify exact sheet names match between file and configuration

Directory Structure Issues

Problem: "Directory not found" errors

```bash
# Solution: Verify directory structure
ls -la inputs/sales_data/
```

Problem: "Mapping file not found" errors

```bash
# Solution: Check mapping files are in rules folder
ls -la rules/*.csv
```

Problem: Rule validation failures

```yaml
# Solution: Ensure directory starts with inputs/
directory: "inputs/sales_data"  # ✅ Correct
directory: "sales_data"         # ❌ Incorrect
```

Failed Rows Issues

Problem: Failed rows not being exported
Solution: Check enable_row_level_recovery: true in global config

Problem: Failed rows reprocessing fails
Solution: Ensure corrected files are placed in failed_rows/to_process/ - metadata is auto-removed

Empty Sheet Handling

Problem: Excel files processed but no data loaded
Solution: Check logs for empty sheet warnings

```bash
grep -i "empty" logs/processing_latest.log
```

Duplicate Detection Issues

Problem: Unexpected duplicates reported
Solution: Review hash_exclude_columns in rule configuration

Log Analysis

Key Log Messages

```log
# Normal processing
INFO - Loaded 150 rows to sales table
INFO - Processing sheet 2/3: 'Inventory'

# Warnings (review but not critical)
WARNING - Sheet 'Marketing' is empty or contains only headers
WARNING - Exported 5 duplicate rows to duplicates/sales_20230101.csv
WARNING - Exported 3 failed rows to failed_rows/inventory_20230101.xlsx  # ✅ NEW

# Enhanced file movement
INFO - Moved duplicates file to processed: duplicates/processed/sales_20230101_20231201143022.csv
INFO - Moved failed_rows file to processed: failed_rows/processed/inventory_20230101_20231201143022.xlsx

# Errors (require action)
ERROR - Database connection failed
ERROR - Sheet 'Sales' not found in file
ERROR - sheet_names must be a list for processing_method 'multiple'
ERROR - Directory should be under inputs/ folder: sales_data
```

Enhanced Log Access

```bash
# View latest log
tail -f logs/processing_latest.log

# Search for errors in all logs
grep -r "ERROR" logs/

# Check processing statistics
grep -E "(Processed|Failed|Duration)" logs/processing_latest.log

# Monitor specific file processing
grep "sales_20230101" logs/processing_latest.log
```

Debug Mode

Enable detailed logging by modifying the script:

```python
logging.basicConfig(
    level=logging.DEBUG,  # Change from INFO to DEBUG
    # ... rest of configuration
)
```

---

10. Best Practices (Enhanced)

File Management

· Use consistent naming conventions
· Organize files by source system or frequency in inputs/ folder
· Implement retention policies for processed files
· Regularly clean to_process directories
· ✅ ENHANCED: Monitor processed/ directories for archive management

Multi-Sheet Excel Best Practices

· Use exact sheet names: Case-sensitive matching
· Validate sheet existence: Check sheets exist before configuring
· List format: Always use YAML list format for multiple sheets
· Consistent structure: Ensure all processed sheets have same columns
· Sheet naming: Use descriptive, consistent sheet names

Enhanced Organized Structure Best Practices

· Configuration management: Keep all configs in rules/ folder
· Data organization: Use clear subdirectory names in inputs/
· Backup strategy: Backup rules/ separately from inputs/
· Version control: Only commit rules/ folder to version control
· 📁 NEW: Log management: Regular log review and archiving

Failed Rows Recovery Best Practices

· Monitor failed_rows/ directory regularly
· Review failure patterns to identify systemic data quality issues
· Train team members on interpreting error messages
· Establish SLAs for failed rows resolution
· ✅ ENHANCED: Use the automatic file movement to track resolution progress

Data Quality

· Validate source data before loading into inputs/ folders
· Establish clear business rules for duplicate resolution
· Document data type expectations for each column
· Monitor format conflict patterns for systemic issues

Performance Optimization

· Use appropriate batch_size for your data volume
· Configure hash_exclude_columns for large text/binary columns
· Set search_subdirectories: false for flat directory structures
· Monitor database performance during large loads
· Adjust chunk_size based on database performance

Security

· Secure database credentials in configuration files in rules/ folder
· Restrict file permissions on sensitive data directories in inputs/
· Regularly review processing logs for anomalies
· Implement access controls for source directories
· 📁 NEW: Secure log files containing processing details

---

11. Example Workflows (Enhanced)

Daily Sales Data Load

```
1. Source: sales_YYYYMMDD.csv in inputs/sales_data/ directory
2. Processing: Insert mode, automatic duplicate detection
3. Error Handling: Failed rows automatically exported to failed_rows/ with original format
4. Output: Data loaded to sales table
5. Monitoring: Check logs/processing_latest.log for warnings/errors
6. ✅ ENHANCED: Logs stored with timestamp in logs/ directory
```

Monthly Multi-Sheet Report

```
1. Source: monthly_report_YYYYMM.xlsx with multiple sheets in inputs/inventory_data/
2. Configuration: Process specific sheets using LIST format in rules/inventory_rule.yaml
3. Processing: Each sheet loaded to inventory table
4. Error Handling: Sheet-level failed row tracking with Excel format preservation
5. Tracking: Source filename includes sheet name for audit
6. ✅ ENHANCED: Files moved to processed/ after successful processing
```

Data Correction Workflow

```
1. Issue: Format conflicts detected in weekly report
2. Export: Conflicting rows saved to format_conflict/ directory with original Excel format
3. Correction: Fix data type issues in exported file
4. Reprocessing: Move corrected file to format_conflict/to_process/
5. Verification: Check database for corrected data
6. ✅ ENHANCED: File automatically moved to format_conflict/processed/ after processing
```

NEW: Failed Rows Recovery Workflow

```
1. Detection: Database insertion fails for specific rows
2. Automatic Export: Failed rows saved to failed_rows/ with error details (original format preserved)
3. Analysis: Review _error_message and _failed_reason columns in logs
4. Correction: Fix data issues based on failure reason
5. Reprocessing: Move corrected file to failed_rows/to_process/ (metadata auto-removed)
6. Verification: Check processing_latest.log for successful insertion
7. ✅ ENHANCED: File automatically moved to failed_rows/processed/ after processing
```

Multi-Sheet Validation Workflow

```
1. Check sheets: python -c "import pandas as pd; print(pd.ExcelFile('inputs/file.xlsx').sheet_names)"
2. Configure: Use exact sheet names in YAML list format in rules/ folder
3. Test: Run loader with small test file
4. Monitor: Check logs/processing_latest.log for each sheet processing status
5. Error Handling: Review failed_rows/ for sheet-specific issues with format preservation
```

Enhanced Log Monitoring Workflow

```
1. Real-time Monitoring: tail -f logs/processing_latest.log
2. Error Tracking: grep "ERROR" logs/processing_*.log
3. Performance Analysis: grep -E "(Processed|Duration)" logs/processing_latest.log
4. Archive Management: Rotate old log files from logs/ directory
5. Audit Trail: Use timestamped log files for compliance reporting
```

---

12. Monitoring & Maintenance (Enhanced)

Regular Checks

· Review logs: Monitor logs/processing_latest.log daily
· Monitor database storage growth
· Verify data quality in target tables
· Check for stale files in to_process directories
· ✅ NEW: Monitor failed_rows/ directory for unresolved issues
· ✅ ENHANCED: Check processed/ directories for successful completions

Multi-Sheet Specific Checks

· Verify all configured sheets are being processed
· Check for sheet name changes in source files
· Monitor empty sheet warnings for data issues
· Validate sheet count matches expectations

Enhanced Organized Structure Maintenance

· Regularly verify directory structure integrity
· Backup rules/ folder configuration separately
· Archive old data from inputs/ folders as needed
· Clean up temporary processing directories
· ✅ NEW: Monitor and resolve failed rows backlog
· 📁 NEW: Manage log files in logs/ directory

Maintenance Tasks

```bash
# Clean old progress tracking data (keep last 30 days)
find . -name "processing_progress.json" -mtime +30 -exec rm {} \;

# Archive processed files (keep last 90 days)
find duplicates/processed/ -name "*.csv" -mtime +90 -exec rm {} \;
find format_conflict/processed/ -name "*.xlsx" -mtime +90 -exec rm {} \;
find failed_rows/processed/ -name "*.csv" -mtime +90 -exec rm {} \;

# Check for stale lock files
find . -name "loader.lock" -mtime +1 -exec rm {} \;

# Validate YAML syntax for all rule files
python -c "import yaml; [yaml.safe_load(open(f)) for f in ['rules/*.yaml']]"

# Check for old failed rows (older than 7 days)
find failed_rows/ -name "*.csv" -mtime +7 -exec ls -la {} \;

# 📁 NEW: Archive old log files (keep last 30 days)
find logs/ -name "processing_*.log" -mtime +30 -exec gzip {} \;
find logs/ -name "processing_*.log.gz" -mtime +90 -exec rm {} \;

# 📁 NEW: Check log directory size
du -sh logs/
```

Verification Script

verify_structure.sh

```bash
#!/bin/bash
echo "Verifying enhanced PostgreSQL Data Loader structure..."

# Check required directories
required_dirs=(
    "rules"
    "inputs/sales_data"
    "inputs/inventory_data" 
    "inputs/weekly_reports"
    "duplicates/to_process"
    "duplicates/processed"
    "format_conflict/to_process"
    "format_conflict/processed"
    "failed_rows/to_process"
    "failed_rows/processed"
    "logs"  # 📁 NEW: Log directory
)

for dir in "${required_dirs[@]}"; do
    if [ -d "$dir" ]; then
        echo "✅ $dir exists"
    else
        echo "❌ $dir missing"
    fi
done

# Check for latest log symlink
if [ -L "logs/processing_latest.log" ]; then
    echo "✅ Latest log symlink exists"
else
    echo "❌ Latest log symlink missing"
fi

echo ""
echo "Enhanced structure verification completed!"
```

Log Management Script

manage_logs.sh

```bash
#!/bin/bash
echo "Managing PostgreSQL Data Loader logs..."

# Compress logs older than 30 days
echo "Compressing logs older than 30 days..."
find logs/ -name "processing_*.log" -mtime +30 -exec gzip {} \;

# Delete compressed logs older than 90 days
echo "Deleting compressed logs older than 90 days..."
find logs/ -name "processing_*.log.gz" -mtime +90 -exec rm {} \;

# Show current log status
echo ""
echo "Current log status:"
ls -la logs/processing_latest.log
echo "Recent log files:"
ls -lt logs/processing_*.log | head -5
echo "Disk usage:"
du -sh logs/

echo "Log management completed!"
```

---

Support & Resources

Getting Help

1. Check logs: tail -f logs/processing_latest.log
2. Verify configuration: Validate YAML syntax and LIST formats in rules/ folder
3. Test connectivity: Use --test-connection option
4. Review examples: Refer to sample configurations in rules/ folder
5. ✅ NEW: Check failed_rows/ for detailed error information with original file formats
6. 📁 NEW: Examine timestamped logs in logs/ directory for historical analysis

Multi-Sheet Troubleshooting

```python
# Quick sheet name check
import pandas as pd
file_path = "inputs/your_file.xlsx"
sheet_names = pd.ExcelFile(file_path).sheet_names
print("Available sheets:", sheet_names)

# Verify YAML list format
import yaml
with open('rules/your_rule.yaml') as f:
    config = yaml.safe_load(f)
    print("Sheet names type:", type(config['sheet_config']['sheet_names']))
    print("Sheet names:", config['sheet_config']['sheet_names'])
```

Failed Rows Analysis

```python
# Analyze failed rows patterns
import pandas as pd
failed_file = "failed_rows/your_file.csv"
if os.path.exists(failed_file):
    df = pd.read_csv(failed_file)
    print("Failure reasons:", df['_failed_reason'].value_counts())
    print("Error messages sample:", df['_error_message'].head(3))
```

Enhanced Log Analysis

```bash
# Quick log analysis
echo "=== Recent Errors ==="
grep "ERROR" logs/processing_latest.log | tail -10

echo "=== Processing Summary ==="
grep -E "(Processed|Failed)" logs/processing_latest.log | tail -5

echo "=== Performance Metrics ==="
grep "Duration" logs/processing_latest.log | tail -3

echo "=== File Processing Status ==="
grep -E "(Processing|Loaded|Exported)" logs/processing_latest.log | tail -10
```

Common Patterns

· File pattern extraction: Use --extract-pattern option
· Rule validation: Check rule files with YAML validators
· Database testing: Test queries with psql or pgAdmin
· Sheet validation: Always verify sheet names exist in files
· Directory structure: Use the organized rules/ and inputs/ structure
· ✅ NEW: Failed rows resolution: Consistent workflow across all error types
· 📁 NEW: Log management: Use timestamped files for audit and debugging

Command Reference

```bash
# Basic usage
python loader_script.py

# Test database connection
python loader_script.py --test-connection

# Extract file pattern
python loader_script.py --extract-pattern "sales_20230101.csv"

# Run with file deletion
python loader_script.py --delete-files Y

# Create sample data
python create_sample_data.py

# 📁 NEW: Monitor logs in real-time
tail -f logs/processing_latest.log

# 📁 NEW: Check recent processing status
grep -E "(STARTING|Processed|Failed)" logs/processing_latest.log

# 📁 NEW: Search for specific file processing
grep "sales_20230101" logs/processing_*.log
```

This comprehensive user guide provides complete documentation for the enhanced PostgreSQL Data Loader with failed rows recovery, organized directory structure, timestamped logging, and comprehensive error handling capabilities.