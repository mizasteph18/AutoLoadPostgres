Format recommandé : .md (Markdown)

Voici le fichier Markdown complet que tu peux sauvegarder dans PyCharm :

Nom du fichier : PostgreSQL_Data_Loader_User_Guide.md

```markdown
# PostgreSQL Data Loader - Complete User Guide

## Table of Contents
1. [Installation & Setup](#installation-setup)
2. [Directory Structure](#directory-structure)
3. [Configuration](#configuration)
4. [Multi-Sheet Excel Configuration](#multi-sheet-excel-configuration)
5. [Processing Modes](#processing-modes)
6. [Enhanced Logging with Colors](#enhanced-logging-with-colors)
7. [Error Recovery](#error-recovery)
8. [Error Reference Table](#error-reference-table)
9. [Troubleshooting](#troubleshooting)
10. [Best Practices](#best-practices)
11. [Command Reference](#command-reference)

---

## 1. Installation & Setup

### Quick Installation

```bash
# Install required packages
pip install pandas psycopg2-binary pyyaml numpy openpyxl

# Create organized directory structure
mkdir -p rules inputs/sales_data inputs/inventory_data inputs/weekly_reports
mkdir -p duplicates/to_process duplicates/processed
mkdir -p format_conflict/to_process format_conflict/processed
mkdir -p failed_rows/to_process failed_rows/processed
mkdir -p logs

# Generate sample configuration
python loader_script.py
```

Setup Script

setup_directories.sh

```bash
#!/bin/bash
echo "Creating enhanced PostgreSQL Data Loader directory structure..."

mkdir -p rules
mkdir -p inputs/sales_data
mkdir -p inputs/inventory_data
mkdir -p inputs/weekly_reports
mkdir -p inputs/regional_reports
mkdir -p inputs/customer_data
mkdir -p duplicates/to_process
mkdir -p duplicates/processed
mkdir -p format_conflict/to_process
mkdir -p format_conflict/processed
mkdir -p failed_rows/to_process
mkdir -p failed_rows/processed
mkdir -p logs

echo "Enhanced directory structure created successfully!"
echo "Logs will be stored in: logs/"
```

---

2. Directory Structure

```
data_loader/
├── loader_script.py
├── global_loader_config.yaml
├── processing_progress.json
├── loader.lock
├── rules/
│   ├── sales_rule.yaml
│   ├── inventory_rule.yaml
│   ├── weekly_rule.yaml
│   ├── sales_mapping.csv
│   ├── inventory_mapping.csv
│   └── weekly_mapping.csv
├── inputs/
│   ├── sales_data/
│   ├── inventory_data/
│   └── weekly_reports/
├── duplicates/
│   ├── to_process/
│   └── processed/
├── format_conflict/
│   ├── to_process/
│   └── processed/
├── failed_rows/
│   ├── to_process/
│   └── processed/
└── logs/
    ├── processing_*.log
    └── processing_latest.log
```

---

3. Configuration

Global Configuration (global_loader_config.yaml)

```yaml
dbname: "your_database_name"
user: "your_username"
password: "your_password"
host: "localhost"
port: 5432
batch_size: 1000
max_connections: 5
min_connections: 1
retry_attempts: 3
enable_progress_tracking: true
enable_data_validation: true
auto_add_columns: true
timestamp_tolerance_seconds: 1.0
delete_files: "N"
lock_timeout: 3600
skip_empty_sheets: true
warn_on_empty_sheets: true
treat_empty_as_error: false
enable_row_level_recovery: true
fail_on_partial_insert: false
retry_on_deadlock: true
max_retry_delay: 30
enable_batch_validation: true
chunk_size: 100
max_chunk_failures: 5
generate_sample_files: false
global_hash_exclude_columns: []
```

Global Configuration Parameters

Parameter Description Default
dbname PostgreSQL database name "your_database_name"
user PostgreSQL username "your_username"
password PostgreSQL password "your_password"
host PostgreSQL server hostname "localhost"
port PostgreSQL server port 5432
batch_size Rows processed per batch 1000
max_connections Maximum pool connections 5
min_connections Minimum pool connections 1
retry_attempts Retry attempts for failures 3
enable_progress_tracking Track processed files true
enable_data_validation Validate data before insertion true
auto_add_columns Auto-add new columns true
timestamp_tolerance_seconds Time tolerance for processed files 1.0
delete_files Delete source after processing "N"
lock_timeout Lock timeout in seconds 3600
skip_empty_sheets Skip empty Excel sheets true
warn_on_empty_sheets Warn about empty sheets true
treat_empty_as_error Treat empty as error false
enable_row_level_recovery Row-level recovery true
fail_on_partial_insert Fail on partial insert false
retry_on_deadlock Retry on deadlock true
max_retry_delay Max retry delay (seconds) 30
enable_batch_validation Batch validation true
chunk_size Chunk size for batch 100
max_chunk_failures Max chunk failures 5
generate_sample_files Generate sample files false
global_hash_exclude_columns Columns excluded from hash []

Processing Rule Parameters

Parameter Description Example
base_name Target table name "sales"
directory Source directory "inputs/sales_data"
file_pattern Regex pattern "sales_\d{8}.csv"
date_format Date format in filename "%Y%m%d"
start_row Starting row (0-based) 0
start_col Starting column (0-based) 0
mode Processing mode "insert"
date_from_filename_col_name Column for extracted date "file_date"
hash_exclude_columns Columns excluded from hash []
search_subdirectories Search in subdirectories true
skip_subdirectories Subdirectories to skip ["archive"]
skip_file_patterns File patterns to skip [".test."]
mapping_file Mapping file path "rules/sales_mapping.csv"

---

4. Multi-Sheet Excel Configuration

Configuration Methods

Single Sheet:

```yaml
sheet_config:
  processing_method: "specific"
  specific_sheet: "Monthly Data"
```

Multiple Sheets:

```yaml
sheet_config:
  processing_method: "multiple"
  sheet_names: 
    - "Sales"
    - "Inventory"
    - "Expenses"
```

All Sheets:

```yaml
sheet_config:
  processing_method: "all"
```

Pattern-Based:

```yaml
sheet_config:
  processing_method: "pattern"
  sheet_name_pattern: "Region_.*"
```

---

5. Processing Modes

Processing Modes Comparison

Mode Duplicates Exported Files Skipped Performance Best Use Case
INSERT YES (duplicates/) NO Standard Initial load, new data
CANCEL_AND_REPLACE NO NO Heavy Corrections, replacement
AUDIT YES (duplicates/) YES (timestamp) Good Rarely modified files
SMART_AUDIT NO YES (hash) EXCELLENT Daily loads, stable data

MODE: INSERT

Process:

1. Scan inputs/ directory
2. Check progress (if enabled)
3. Load CSV/Excel file
4. Validate data
5. Detect duplicates (compare row hashes)
6. Export duplicates to duplicates/to_process/
7. Insert unique rows
8. Update progress tracking

MODE: CANCEL_AND_REPLACE

Process:

1. Scan inputs/ directory
2. Find all rows with same source_filename
3. DELETE existing rows
4. Load new data
5. Validate
6. Insert all new rows
7. Optionally delete source file

MODE: AUDIT (Classic)

Process:

1. Directory scan
2. Compare file timestamp with database
3. If similar timestamp → SKIP file
4. If different → Process like INSERT
5. Log skip/process decision

MODE: SMART_AUDIT

Process:

1. Check file hash vs progress tracking
2. If file unchanged → SKIP completely
3. If changed → Load data
4. Calculate row hashes (SHA256)
5. Compare with all existing hashes in DB
6. Keep only NEW rows (hash not in DB)
7. Silently ignore duplicates (NO export)
8. Insert new rows, log statistics

Benefits:

· Efficient deduplication (row-level)
· No export overhead
· Progress tracking prevents reprocessing
· Silent operation (no user intervention)

---

6. Enhanced Logging with Colors

Color System

Log Level Color ANSI Code Purpose
DEBUG Cyan \033[36m Detailed debugging
INFO White \033[37m Normal messages
WARNING Yellow \033[33m Warnings to review
ERROR Red \033[31m Errors requiring action
CRITICAL Red background \033[41m\033[37m Critical blocking errors

Visual Examples

```
[37m2024-01-15 10:30:45 - INFO - Database connection successful[0m
[33m2024-01-15 10:30:46 - WARNING - Found 5 duplicate rows[0m
[31m2024-01-15 10:30:47 - ERROR - Database connection lost[0m
[41m[37m2024-01-15 10:30:48 - CRITICAL - Cannot create table[0m
```

Log Management

```bash
# View colored logs in real-time
tail -f logs/processing_latest.log

# Search for errors (colored)
grep --color=always -i "error" logs/processing_latest.log

# Monitor SMART AUDIT statistics
grep "SMART AUDIT:" logs/processing_latest.log

# Check log files
ls -la logs/
```

---

7. Error Recovery

Failed Rows Recovery

Step 1: Automatic Export

· Failed rows exported to failed_rows/
· Error details in _error_message and _failed_reason
· Guidance in _GUIDANCE column

Step 2: Manual Correction

1. Open failed file in failed_rows/
2. Review error details
3. Fix data issues
4. Remove metadata columns

Step 3: Reprocessing

1. Save corrected file
2. Move to failed_rows/to_process/
3. Run loader → automatic processing

Failure Reason Codes

Code Description Solution
DUPLICATE_KEY Unique constraint violation Remove duplicates
MISSING_REFERENCE Foreign key violation Ensure referenced records exist
DATA_TYPE_MISMATCH Invalid input syntax Fix data types
MISSING_REQUIRED_VALUE Null in required column Provide values
VALUE_TOO_LONG Value exceeds column length Shorten values
DEADLOCK Database deadlock Automatic retry
CONNECTION_ISSUE Connection problem Check server/network
UNKNOWN_ERROR Other errors Check detailed message

---

8. Error Reference Table

Common Errors

Category Error Code Solution
Database Connection refused 08001 Start DB service, check host/port
Database Password authentication failed 28P01 Verify credentials
Database Unique violation 23505 Remove duplicates
Database Foreign key violation 23503 Ensure referenced records exist
File System Permission denied 13 Check write permissions
File System No such file or directory 2 Verify file exists
Excel Sheet not found N/A Check sheet names
Excel File is not a zip file N/A Verify Excel file integrity

---

9. Troubleshooting

Common Issues

Database Connection:

```bash
# Test connection
psql -h localhost -U your_username -d your_database

# Check config
cat global_loader_config.yaml | grep -E "(host|port|dbname|user)"
```

File Permissions:

```bash
# Check permissions
ls -la inputs/sales_data/
ls -la logs/

# Fix permissions
chmod 644 inputs/sales_data/*.csv
chmod 755 inputs/sales_data/
chmod 755 logs/
```

Lock File Issues:

```bash
# Remove stale lock
rm -f loader.lock

# Check lock age
if [ -f loader.lock ]; then
    lock_age=$(($(date +%s) - $(stat -f %m loader.lock)))
    if [ $lock_age -gt 3600 ]; then
        rm loader.lock
    fi
fi
```

Log Analysis

```bash
# View latest log
tail -f logs/processing_latest.log

# Search for errors
grep -i "error" logs/processing_latest.log
grep -i "warning" logs/processing_latest.log
grep -i "smart_audit" logs/processing_latest.log

# Check SMART AUDIT statistics
grep -r "SMART AUDIT:" logs/
```

---

10. Best Practices

File Management

· Use consistent naming in inputs/
· Organize by source system/frequency
· Implement retention policies
· Regular cleanup of to_process directories

SMART AUDIT Mode

· Use for daily incremental loads
· Monitor duplicate statistics
· Combine with progress tracking
· Ideal for high-volume data

Log Management

· Monitor logs/ directory size
· Implement log rotation
· Use processing_latest.log symlink
· Leverage color-coded logs for monitoring

Security

· Secure database credentials in rules/
· Restrict file permissions on inputs/
· Regular log review for anomalies
· Access controls for source directories

---

11. Command Reference

Basic Usage

```bash
# Standard processing
python loader_script.py

# Test connection
python loader_script.py --test-connection

# Delete files after processing
python loader_script.py --delete-files Y

# Setup sample configuration
python loader_script.py --setup
```

Utility Commands

```bash
# Check structure
find . -type d -name "inputs" -o -name "rules" -o -name "duplicates" -o -name "format_conflict" -o -name "failed_rows" -o -name "logs" | sort

# View logs
tail -n 50 logs/processing_latest.log

# Check lock files
find . -name "loader.lock" -mtime +1 -exec ls -la {} \;

# Archive processed files
tar -czf processed_$(date +%Y%m%d).tar.gz duplicates/processed/ format_conflict/processed/ failed_rows/processed/

# Monitor real-time
tail -f logs/processing_latest.log | grep --color=always -E "(ERROR|WARNING|CRITICAL)"
```

Verification Script

verify_structure.sh

```bash
#!/bin/bash
echo "Verifying enhanced PostgreSQL Data Loader structure..."

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
    "logs"
)

for dir in "${required_dirs[@]}"; do
    if [ -d "$dir" ]; then
        echo "✅ $dir exists"
    else
        echo "❌ $dir missing"
    fi
done

echo ""
echo "Structure verification completed!"
```

---

Support & Resources

Getting Help:

1. Check Logs: tail -f logs/processing_latest.log
2. Verify Configuration: Validate YAML in rules/
3. Test Connectivity: Use --test-connection option
4. Review Examples: Sample configs in rules/
5. Check Error Reference: Comprehensive table above
6. Examine Failed Rows: Files in failed_rows/
7. SMART AUDIT Statistics: Search for "SMART AUDIT:" in logs

---

Documentation Version: 2.0 | Last Updated: January 2024

```

**Instructions pour PyCharm :**

1. **Créer le fichier :**
   - Dans PyCharm → `File` → `New` → `File`
   - Nommer : `PostgreSQL_Data_Loader_User_Guide.md`

2. **Coller le contenu :**
   - Copier tout le texte Markdown ci-dessus
   - Coller dans le nouveau fichier

3. **Activer la prévisualisation :**
   - Clique droit sur le fichier → `Open in` → `Preview`
   - Ou utiliser l'onglet `Preview` en haut à droite

4. **Navigation par liens :**
   - Les liens internes (comme `#installation-setup`) fonctionnent en mode prévisualisation
   - Ctrl+click sur un lien pour naviguer

5. **Formatage automatique :**
   - PyCharm formate automatiquement le Markdown
   - Les tableaux et blocs de code sont bien rendus

**Avantages du format .md :**
- ✅ Support natif dans PyCharm
- ✅ Prévisualisation intégrée
- ✅ Navigation par liens internes
- ✅ Formatage automatique
- ✅ Compatible avec Git
- ✅ Léger et rapide à charger

**Alternative :** Si tu préfères un format plus riche, tu peux aussi sauvegarder en `.html` ou `.pdf`, mais `.md` est le plus pratique pour l'édition et la navigation dans PyCharm.
```