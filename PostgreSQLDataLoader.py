#!/usr/bin/env python3
"""
PostgreSQL Data Loader - Enhanced with Failed Rows Recovery
Organized Directory Structure: rules/ for configs, inputs/ for data
"""

import os
import json
import yaml
import pandas as pd
import psycopg2
import hashlib
import re
import logging
import numpy as np
import shutil
import time
import sys
import atexit
import dataclasses
from pathlib import Path
from datetime import datetime
from psycopg2 import sql, pool
from psycopg2.extras import execute_values
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass, field
from contextlib import contextmanager

# ===========================
# Constants
# ===========================
DEFAULT_BATCH_SIZE = 1000
PROGRESS_FILE = "processing_progress.json"
GLOBAL_CONFIG_FILE = "global_loader_config.yaml"
RESERVED_COLUMNS = {"loaded_timestamp", "source_filename", "content_hash", "operation"}
HASH_EXCLUDE_COLS = RESERVED_COLUMNS
DUPLICATES_ROOT_DIR = "duplicates"
DUPLICATES_TO_PROCESS_DIR = os.path.join(DUPLICATES_ROOT_DIR, "to_process")
DUPLICATES_PROCESSED_DIR = os.path.join(DUPLICATES_ROOT_DIR, "processed")
FORMAT_CONFLICT_DIR = "format_conflict"
FORMAT_CONFLICT_TO_PROCESS_DIR = os.path.join(FORMAT_CONFLICT_DIR, "to_process")
FORMAT_CONFLICT_PROCESSED_DIR = os.path.join(FORMAT_CONFLICT_DIR, "processed")
FAILED_ROWS_DIR = "failed_rows"
FAILED_ROWS_TO_PROCESS_DIR = os.path.join(FAILED_ROWS_DIR, "to_process")
FAILED_ROWS_PROCESSED_DIR = os.path.join(FAILED_ROWS_DIR, "processed")
SYSTEM_COLUMNS_ORDER = ["loaded_timestamp", "source_filename", "content_hash", "operation"]
LOCK_FILE = "loader.lock"
LOG_DIR = "logs"

# Metadata columns injected by conflict exports (automatically removed on reprocessing)
METADATA_COLUMNS = {
    "_conflict_type",
    "_conflict_details", 
    "_business_key",
    "_GUIDANCE",
    "_source_sheet",
    "_error_message",
    "_failed_reason",
    "_source_file",
    "_export_type"
}

# ===========================
# Configure logging - ENHANCED with timestamped files
# ===========================
def setup_logging():
    """Setup logging with timestamped log files in logs directory."""
    # Create logs directory if it doesn't exist
    os.makedirs(LOG_DIR, exist_ok=True)
    
    # Generate log filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"processing_{timestamp}.log"
    log_filepath = os.path.join(LOG_DIR, log_filename)
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_filepath),
            logging.StreamHandler()
        ]
    )
    
    # Also create a symlink to the latest log for easy access
    latest_log_path = os.path.join(LOG_DIR, "processing_latest.log")
    try:
        if os.path.exists(latest_log_path):
            os.remove(latest_log_path)
        os.symlink(log_filepath, latest_log_path)
    except Exception as e:
        print(f"Could not create latest log symlink: {e}")

# Call setup_logging at module import
setup_logging()
logger = logging.getLogger(__name__)

# ===========================
# Data classes
# ===========================
@dataclass
class ProcessingConfig:
    """Configuration class for processing parameters."""
    dbname: str = "your_database_name"
    user: str = "your_username"
    password: str = "your_password"
    host: str = "localhost"
    port: int = 5432
    batch_size: int = DEFAULT_BATCH_SIZE
    max_connections: int = 5
    min_connections: int = 1
    retry_attempts: int = 3
    enable_progress_tracking: bool = True
    enable_data_validation: bool = True
    timestamp_tolerance_seconds: float = 1.0
    global_hash_exclude_columns: List[str] = field(default_factory=list)
    lock_timeout: int = 3600
    auto_add_columns: bool = True
    delete_files: str = "N"
    skip_empty_sheets: bool = True
    warn_on_empty_sheets: bool = True
    treat_empty_as_error: bool = False
    
    # Enhanced insertion settings
    enable_row_level_recovery: bool = True
    fail_on_partial_insert: bool = False
    retry_on_deadlock: bool = True
    max_retry_delay: int = 30
    enable_batch_validation: bool = True
    chunk_size: int = 100
    max_chunk_failures: int = 5

    def get_db_config(self) -> Dict[str, Any]:
        """Return database configuration as a dictionary."""
        return {
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
            "host": self.host,
            "port": self.port
        }

@dataclass
class FileProcessingRule:
    """Defines rules for processing specific types of files with organized directory structure."""
    base_name: str
    directory: str
    file_pattern: str
    date_format: Optional[str] = None
    start_row: Optional[int] = None
    start_col: Optional[int] = None
    mode: Optional[str] = None
    date_from_filename_col_name: Optional[str] = None
    hash_exclude_columns: List[str] = field(default_factory=list)
    search_subdirectories: bool = True
    sheet_config: Dict[str, Any] = field(default_factory=dict)
    mapping_file: str = None  # Explicit mapping file path
    _compiled_pattern: Any = field(init=False, repr=False)

    def __post_init__(self):
        self._compiled_pattern = re.compile(self.file_pattern)
        # Set default mapping file path if not provided
        if self.mapping_file is None:
            self.mapping_file = f"rules/{self.base_name}_mapping.csv"

    def match(self, filename: str) -> Optional[re.Match]:
        return self._compiled_pattern.match(filename)

    @property
    def target_table(self) -> str:
        return self.base_name

    def validate(self) -> Tuple[bool, List[str]]:
        errors = []
        if not self.directory:
            errors.append("Directory is required")
        if not self.directory.startswith('inputs/'):
            errors.append("Directory should be under inputs/ folder")
        if not self.file_pattern:
            errors.append("File pattern is required")
        if self.mode and self.mode not in ["cancel_and_replace", "audit", "insert"]:
            errors.append(f"Invalid mode: {self.mode}")
        if self.date_from_filename_col_name and not self.date_format:
            errors.append("date_format is required when date_from_filename_col_name is specified")
        
        # Validate sheet_config
        if self.sheet_config:
            processing_method = self.sheet_config.get('processing_method', 'specific')
            valid_methods = ['specific', 'multiple', 'all', 'pattern']
            if processing_method not in valid_methods:
                errors.append(f"Invalid processing_method: {processing_method}. Must be one of {valid_methods}")
            
            if processing_method == 'specific' and not self.sheet_config.get('specific_sheet'):
                errors.append("specific_sheet is required when processing_method is 'specific'")
            
            if processing_method == 'multiple' and not self.sheet_config.get('sheet_names'):
                errors.append("sheet_names is required when processing_method is 'multiple'")
            
            if processing_method == 'pattern' and not self.sheet_config.get('sheet_name_pattern'):
                errors.append("sheet_name_pattern is required when processing_method is 'pattern'")
        
        return len(errors) == 0, errors

@dataclass
class FileContext:
    """Holds file processing context with organized directory structure."""
    filepath: Path
    filename: str
    target_table: str
    mapping_filepath: Path  # Now points to rules/mapping_file.csv
    extracted_timestamp_str: str
    file_modified_timestamp: datetime
    start_row: int
    start_col: int
    mode: str
    date_from_filename_col_name: Optional[str]
    hash_exclude_columns: List[str]
    sheet_config: Dict[str, Any] = field(default_factory=dict)
    is_duplicate: bool = False
    is_format_conflict: bool = False
    is_failed_row: bool = False  # For failed rows processing
    source_sheet: str = ""

# ===========================
# Database Manager
# ===========================
class DatabaseManager:
    """Handles database connections and operations with automatic table creation."""

    def __init__(self, db_config: Dict[str, Any], config: ProcessingConfig):
        self.db_config = db_config
        self.config = config
        self.connection_pool = None
        self._initialize_pool()

    def _initialize_pool(self) -> None:
        try:
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                self.config.min_connections,
                self.config.max_connections,
                **self.db_config
            )
            logger.info("Database connection pool initialized")
        except psycopg2.Error as e:
            logger.critical(f"Failed to initialize connection pool: {e}")
            raise

    @contextmanager
    def get_connection(self):
        conn = None
        try:
            conn = self.connection_pool.getconn()
            yield conn
        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            logger.error(f"Database operation failed: {e}")
            raise
        finally:
            if conn:
                self.connection_pool.putconn(conn)

    def test_connection(self) -> bool:
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_name = %s
                        );
                    """, (table_name,))
                    return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            return False

    def create_table_if_not_exists(self, table_name: str, mapping: pd.DataFrame) -> bool:
        """
        Create table based on mapping file if it doesn't exist.
        """
        try:
            # Check if table already exists
            if self.table_exists(table_name):
                logger.info(f"Table {table_name} already exists")
                return True

            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    columns_to_create = []

                    # Process file columns with LoadFlag='Y'
                    file_columns = mapping[
                        (mapping['data_source'] == 'file') &
                        (mapping['LoadFlag'] == 'Y')
                    ]

                    for _, row in file_columns.iterrows():
                        col_def = f"{row['TargetColumn']} {row['DataType']}"
                        columns_to_create.append(col_def)

                    # Add system columns
                    system_columns = [
                        ("loaded_timestamp", "TIMESTAMP"),
                        ("source_filename", "TEXT"),
                        ("content_hash", "TEXT"),
                        ("operation", "TEXT")
                    ]

                    for col_name, col_type in system_columns:
                        columns_to_create.append(f"{col_name} {col_type}")

                    if columns_to_create:
                        create_query = sql.SQL("CREATE TABLE {} ({})").format(
                            sql.Identifier(table_name),
                            sql.SQL(", ".join(columns_to_create))
                        )
                        cursor.execute(create_query)
                        conn.commit()
                        logger.info(f"Created table {table_name} with {len(columns_to_create)} columns")
                        return True
                    else:
                        logger.error(f"No columns configured for loading in table {table_name}")
                        return False
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            return False

    def get_latest_timestamp_for_filename(self, target_table: str, source_filename: str) -> Optional[datetime]:
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = sql.SQL("""
                        SELECT MAX(loaded_timestamp) 
                        FROM {} 
                        WHERE source_filename = %s
                    """).format(sql.Identifier(target_table))
                    cursor.execute(query, (source_filename,))
                    return cursor.fetchone()[0]
        except psycopg2.errors.UndefinedTable:
            return None
        except Exception as e:
            logger.error(f"Error getting latest timestamp: {e}")
            raise

    def delete_by_source_filename(self, conn, target_table: str, source_filename: str) -> int:
        try:
            with conn.cursor() as cursor:
                query = sql.SQL("DELETE FROM {} WHERE source_filename = %s").format(sql.Identifier(target_table))
                cursor.execute(query, (source_filename,))
                return cursor.rowcount
        except psycopg2.errors.UndefinedTable:
            return 0
        except Exception as e:
            logger.error(f"Error deleting records: {e}")
            raise

    def file_exists_in_db(self, target_table: str, file_modified_timestamp: datetime, source_filename: str) -> bool:
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = sql.SQL("""
                        SELECT 1 FROM {} 
                        WHERE source_filename = %s 
                        AND ABS(EXTRACT(EPOCH FROM (loaded_timestamp - %s))) <= %s
                        LIMIT 1
                    """).format(sql.Identifier(target_table))
                    cursor.execute(query, (
                        source_filename,
                        file_modified_timestamp,
                        self.config.timestamp_tolerance_seconds
                    ))
                    return cursor.fetchone() is not None
        except psycopg2.errors.UndefinedTable:
            return False
        except Exception as e:
            logger.error(f"File existence check failed: {e}")
            return False

    def get_existing_hashes(self, target_table: str, source_filename: str) -> set:
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = sql.SQL("""
                        SELECT content_hash FROM {}
                        WHERE source_filename = %s
                    """).format(sql.Identifier(target_table))
                    cursor.execute(query, (source_filename,))
                    return {row[0] for row in cursor.fetchall()}
        except psycopg2.errors.UndefinedTable:
            return set()
        except Exception as e:
            logger.error(f"Failed to get existing hashes: {e}")
            return set()

    def column_exists(self, table_name: str, column_name: str) -> bool:
        """Check if a column exists in the specified table."""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    query = sql.SQL("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = %s AND column_name = %s
                    """)
                    cursor.execute(query, (table_name, column_name))
                    return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking column existence: {e}")
            return False

    def alter_table_add_columns(self, table_name: str, new_columns: List[Dict[str, str]]) -> bool:
        """
        Alter table to add new columns based on mapping configuration.
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    for col_info in new_columns:
                        column_name = col_info['TargetColumn']
                        data_type = col_info['DataType']

                        if not self.column_exists(table_name, column_name):
                            alter_query = sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(
                                sql.Identifier(table_name),
                                sql.Identifier(column_name),
                                sql.SQL(data_type)
                            )
                            cursor.execute(alter_query)
                            logger.info(f"Added column {column_name} to table {table_name}")

                    conn.commit()
                    return True
        except Exception as e:
            logger.error(f"Failed to alter table {table_name}: {e}")
            return False

# ===========================
# Data Validator
# ===========================
class DataValidator:
    """Handles data validation and quality checks."""

    def __init__(self, config: ProcessingConfig):
        self.config = config

    def validate_dataframe(self, df: pd.DataFrame, mapping: pd.DataFrame, filename: str) -> Tuple[bool, List[str]]:
        errors = []

        if not self.config.enable_data_validation:
            return True, errors

        # Check for reserved column names
        reserved_cols = [col for col in RESERVED_COLUMNS if col in df.columns]
        if reserved_cols:
            errors.append(f"Critical: Reserved column names used: {', '.join(reserved_cols)}")
            return False, errors

        # Blockers from mapping
        condition1 = mapping[
            (mapping['data_source'] == 'file') &
            (mapping['LoadFlag'] == 'Y') &
            (mapping['order'].isnull())
        ]
        if not condition1.empty:
            errors.append(f"Critical: Found file columns with LoadFlag='Y' and order=null: {condition1['RawColumn'].tolist()}")

        condition2 = mapping[
            (mapping['LoadFlag'] == 'N') &
            (mapping['IndexColumn'] == 'Y')
        ]
        if not condition2.empty:
            errors.append(f"Critical: Found columns with LoadFlag='N' and IndexColumn='Y': {condition2['RawColumn'].tolist()}")

        condition3 = mapping[
            (mapping['LoadFlag'] == '') |
            (mapping['LoadFlag'].isna())
        ]
        if not condition3.empty:
            errors.append(f"Critical: Found unconfigured new columns: {condition3['RawColumn'].tolist()}")

        required_load_cols = mapping[mapping['LoadFlag'] == 'Y']['TargetColumn'].values
        missing_required_cols = [col for col in required_load_cols if col not in df.columns]
        if missing_required_cols:
            errors.append(f"Critical: Missing columns: {', '.join(missing_required_cols)}")

        if errors:
            return False, errors

        return True, errors

# ===========================
# Hybrid Progress Tracker
# ===========================
class HybridProgressTracker:
    """Tracks processing progress for resume capability with hybrid timestamp/hash checking."""

    def __init__(self, progress_file: str = PROGRESS_FILE):
        self.progress_file = progress_file
        self.processed_files = self._load_progress()

    def _load_progress(self) -> dict:
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not load progress file: {e}")
        return {}

    def save_progress(self) -> None:
        try:
            with open(self.progress_file, 'w') as f:
                json.dump(self.processed_files, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save progress: {e}")

    def get_tracking_key(self, filepath: Path, file_context: FileContext) -> str:
        # Include sheet name in tracking key for multi-sheet Excel files
        sheet_suffix = f"::{file_context.source_sheet}" if file_context.source_sheet else ""
        return f"{filepath}{sheet_suffix}||{file_context.target_table}"

    def calculate_file_hash(self, filepath: Path) -> str:
        hasher = hashlib.sha256()
        try:
            with open(filepath, 'rb') as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except Exception as e:
            logger.error(f"Error calculating file hash for {filepath}: {e}")
            return ""

    def _calculate_config_hash(self, file_context: FileContext) -> str:
        config_data = {
            'target_table': file_context.target_table,
            'start_row': file_context.start_row,
            'start_col': file_context.start_col,
            'mode': file_context.mode,
            'hash_exclude_columns': sorted(file_context.hash_exclude_columns),
            'sheet_config': file_context.sheet_config,
            'source_sheet': file_context.source_sheet
        }
        return hashlib.sha256(json.dumps(config_data, sort_keys=True).encode()).hexdigest()

    def needs_processing(self, filepath: Path, file_context: FileContext) -> bool:
        if not filepath.exists():
            return False

        tracking_key = self.get_tracking_key(filepath, file_context)
        current_mod_time = datetime.fromtimestamp(filepath.stat().st_mtime)
        stored_info = self.processed_files.get(tracking_key)

        # New file - definitely process
        if not stored_info:
            return True

        try:
            stored_dt = datetime.fromisoformat(stored_info['timestamp'])
            time_diff = abs((current_mod_time - stored_dt).total_seconds())

            if time_diff <= 1.0:
                current_config_hash = self._calculate_config_hash(file_context)
                stored_config_hash = stored_info.get('config_hash', '')

                if current_config_hash == stored_config_hash:
                    return False
                else:
                    return True
        except Exception as e:
            logger.warning(f"Error checking timestamp for {filepath}: {e}")

        current_content_hash = self.calculate_file_hash(filepath)
        current_config_hash = self._calculate_config_hash(file_context)

        if not current_content_hash:
            return True

        stored_content_hash = stored_info.get('hash', '')
        stored_config_hash = stored_info.get('config_hash', '')

        if (current_content_hash == stored_content_hash and
                current_config_hash == stored_config_hash):
            self.mark_processed(filepath, file_context)
            return False

        return True

    def mark_processed(self, filepath: Path, file_context: FileContext) -> None:
        tracking_key = self.get_tracking_key(filepath, file_context)
        current_content_hash = self.calculate_file_hash(filepath)
        current_config_hash = self._calculate_config_hash(file_context)
        current_mod_time = datetime.fromtimestamp(filepath.stat().st_mtime)

        self.processed_files[tracking_key] = {
            "timestamp": current_mod_time.isoformat(),
            "hash": current_content_hash,
            "config_hash": current_config_hash
        }
        self.save_progress()

# ===========================
# File Processor with Multi-Sheet Excel Support
# ===========================
class FileProcessor:
    """Handles file operations and data extraction with multi-sheet Excel support."""

    def __init__(self, config: ProcessingConfig):
        self.config = config

    def load_file(self, file_path: Path, start_row: int = 0, start_col: int = 0, 
                  sheet_config: Dict[str, Any] = None) -> pd.DataFrame:
        file_ext = file_path.suffix.lower()
        
        try:
            if file_ext == '.csv':
                return self._load_csv(file_path, start_row, start_col)
            elif file_ext in ['.xlsx', '.xls']:
                return self._load_excel(file_path, start_row, start_col, sheet_config)
            elif file_ext == '.parquet':
                return pd.read_parquet(file_path).iloc[:, start_col:]
            elif file_ext == '.json':
                return pd.read_json(file_path).iloc[:, start_col:]
            else:
                raise ValueError(f"Unsupported file format: {file_ext}")
        except Exception as e:
            logger.error(f"Error loading file {file_path}: {e}")
            raise

    def _load_csv(self, file_path: Path, start_row: int, start_col: int) -> pd.DataFrame:
        if start_row and start_row > 0:
            return pd.read_csv(file_path, skiprows=range(0, start_row), header=0).iloc[:, start_col:]
        else:
            return pd.read_csv(file_path, header=0).iloc[:, start_col:]

    def _load_excel(self, file_path: Path, start_row: int, start_col: int, 
                    sheet_config: Dict[str, Any]) -> pd.DataFrame:
        """Load Excel file with enhanced empty sheet detection."""
        
        if not sheet_config:
            sheet_config = {}
            
        processing_method = sheet_config.get('processing_method', 'specific')
        
        if processing_method == 'specific':
            sheet_name = sheet_config.get('specific_sheet', 'Sheet1')
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=start_row, header=0)
                if self._is_sheet_empty(df, start_col):
                    if self.config.warn_on_empty_sheets:
                        logger.warning(f"Sheet '{sheet_name}' is empty or contains only headers")
                    return pd.DataFrame()  # Return empty DataFrame
                df['_source_sheet'] = sheet_name
                return df.iloc[:, start_col:]
            except ValueError as e:
                logger.error(f"Sheet '{sheet_name}' not found in {file_path}: {e}")
                raise
        
        # Multiple sheet processing
        dfs = []
        sheets_processed = 0
        
        if processing_method == 'multiple':
            sheet_names = sheet_config.get('sheet_names', [])
        elif processing_method == 'all':
            all_sheets = pd.read_excel(file_path, sheet_name=None)
            sheet_names = list(all_sheets.keys()) if all_sheets else []
        elif processing_method == 'pattern':
            pattern = sheet_config.get('sheet_name_pattern', '.*')
            all_sheets = pd.read_excel(file_path, sheet_name=None)
            sheet_names = [name for name in (all_sheets.keys() if all_sheets else []) if re.match(pattern, name)]
        else:
            sheet_names = []
        
        for sheet_name in sheet_names:
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=start_row, header=0)
                if self._is_sheet_empty(df, start_col):
                    if self.config.warn_on_empty_sheets:
                        logger.info(f"Skipping empty sheet: {sheet_name}")
                    continue
                    
                df['_source_sheet'] = sheet_name
                dfs.append(df.iloc[:, start_col:])
                sheets_processed += 1
            except Exception as e:
                logger.warning(f"Error processing sheet '{sheet_name}': {e}")
        
        if sheets_processed == 0:
            if self.config.warn_on_empty_sheets:
                logger.warning(f"No data found in any sheets for {file_path}")
            return pd.DataFrame()
        
        return pd.concat(dfs, ignore_index=True)

    def _is_sheet_empty(self, df: pd.DataFrame, start_col: int) -> bool:
        """Check if sheet is truly empty (beyond headers and start column)."""
        if df is None or df.empty:
            return True
        
        # Check if there are any data rows (beyond header)
        if len(df) <= 0:
            return True
        
        # Check if all data cells are empty/NaN after start_col
        data_section = df.iloc[:, start_col:]
        if data_section.empty:
            return True
        
        if data_section.isna().all().all():
            return True
        
        # Check if all values are empty strings or whitespace
        if data_section.applymap(lambda x: str(x).strip() if pd.notna(x) else '').eq('').all().all():
            return True
        
        return False

    def get_excel_sheet_names(self, file_path: Path) -> List[str]:
        """Get all sheet names from an Excel file."""
        try:
            xl_file = pd.ExcelFile(file_path)
            return xl_file.sheet_names
        except Exception as e:
            logger.error(f"Error reading Excel file {file_path}: {e}")
            return []

    def calculate_row_hashes(self, df: pd.DataFrame, exclude_columns: Set[str]) -> List[str]:
        """Calculate content hashes for each row with custom exclusions."""
        hashes = []
        for _, row in df.iterrows():
            exclude = HASH_EXCLUDE_COLS | set(exclude_columns)
            data = {k: v for k, v in row.items() if k not in exclude}
            hasher = hashlib.sha256()
            hasher.update(json.dumps(data, sort_keys=True, default=str).encode('utf-8'))
            hashes.append(hasher.hexdigest())
        return hashes

    def _is_numeric(self, value) -> bool:
        if pd.isna(value):
            return True
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False

    def _is_date(self, value) -> bool:
        if pd.isna(value):
            return True
        if isinstance(value, (datetime, pd.Timestamp)):
            return True
        if isinstance(value, str):
            try:
                pd.to_datetime(value)
                return True
            except (ValueError, TypeError):
                return False
        return False

    def _is_boolean(self, value) -> bool:
        if pd.isna(value):
            return True
        if isinstance(value, bool):
            return True
        if isinstance(value, (int, float)):
            return value in [0, 1]
        if isinstance(value, str):
            return value.lower() in ['true', 'false', 'yes', 'no', '1', '0', 't', 'f', 'y', 'n']
        return False

# ===========================
# Main Loader with Enhanced Error Handling
# ===========================
class PostgresLoader:
    """Main loader class with organized directory structure and enhanced error handling."""

    def __init__(self, global_start_row: int = 0, global_start_col: int = 0,
                 delete_files: str = "N",
                 global_config_file: str = GLOBAL_CONFIG_FILE,
                 rules_folder_path: str = "rules"):

        self.config = self._load_global_config(global_config_file)
        self.config.delete_files = delete_files.upper()

        self.processing_rules = self._load_processing_rules(rules_folder_path)
        self.db_manager = DatabaseManager(self.config.get_db_config(), self.config)
        self.validator = DataValidator(self.config)
        self.file_processor = FileProcessor(self.config)
        self.progress_tracker = HybridProgressTracker() if self.config.enable_progress_tracking else None

        self.global_start_row = global_start_row
        self.global_start_col = global_start_col
        self.delete_files = delete_files.upper() == "Y"
        self.lock_acquired = False
        self.run_id = datetime.now().isoformat()

        # Create organized directory structure
        self._create_directory_structure()

        # Acquire lock to prevent concurrent runs
        self._acquire_lock()
        atexit.register(self._release_lock)

        self._validate_setup()

    def _create_directory_structure(self):
        """Create the organized directory structure."""
        # Configuration directories
        os.makedirs("rules", exist_ok=True)
        
        # Input data directories
        os.makedirs("inputs/sales_data", exist_ok=True)
        os.makedirs("inputs/inventory_data", exist_ok=True)
        os.makedirs("inputs/weekly_reports", exist_ok=True)
        
        # Processing directories
        os.makedirs(DUPLICATES_ROOT_DIR, exist_ok=True)
        os.makedirs(DUPLICATES_TO_PROCESS_DIR, exist_ok=True)
        os.makedirs(DUPLICATES_PROCESSED_DIR, exist_ok=True)
        os.makedirs(FORMAT_CONFLICT_DIR, exist_ok=True)
        os.makedirs(FORMAT_CONFLICT_TO_PROCESS_DIR, exist_ok=True)
        os.makedirs(FORMAT_CONFLICT_PROCESSED_DIR, exist_ok=True)
        
        # Failed rows directories
        os.makedirs(FAILED_ROWS_DIR, exist_ok=True)
        os.makedirs(FAILED_ROWS_TO_PROCESS_DIR, exist_ok=True)
        os.makedirs(FAILED_ROWS_PROCESSED_DIR, exist_ok=True)

        # Log directory (already created in setup_logging, but ensure it exists)
        os.makedirs(LOG_DIR, exist_ok=True)

    # ---------------------------
    # Locking
    # ---------------------------
    def _acquire_lock(self):
        """Prevent multiple concurrent runs with lock file"""
        if os.path.exists(LOCK_FILE):
            lock_time = os.path.getmtime(LOCK_FILE)
            if time.time() - lock_time < self.config.lock_timeout:
                logger.error("Another instance is running. Exiting.")
                sys.exit(0)
            else:
                logger.warning("Stale lock file detected. Removing.")
                os.remove(LOCK_FILE)

        with open(LOCK_FILE, 'w') as f:
            f.write(str(os.getpid()))
        self.lock_acquired = True
        logger.info(f"Lock acquired for run {self.run_id}")

    def _release_lock(self):
        """Clean up lock file on exit"""
        if self.lock_acquired and os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
            logger.info("Lock released")

    # ---------------------------
    # Configuration / Rules Loading
    # ---------------------------
    def _load_global_config(self, config_file: str) -> ProcessingConfig:
        if os.path.exists(config_file):
            try:
                with open(config_file, 'r') as f:
                    if config_file.endswith('.yaml') or config_file.endswith('.yml'):
                        config_data = yaml.safe_load(f)
                    else:
                        config_data = json.load(f)
                return ProcessingConfig(**config_data)
            except Exception as e:
                logger.warning(f"Could not load global config: {e}")

        # Return default config if file doesn't exist or can't be loaded
        return ProcessingConfig()

    def _load_processing_rules(self, rules_folder: str) -> List[FileProcessingRule]:
        rules_folder_path = Path(rules_folder)
        if not rules_folder_path.exists():
            logger.error(f"Rules folder not found: {rules_folder_path}")
            return []

        rules = []
        for rule_file_path in rules_folder_path.iterdir():
            if rule_file_path.is_file() and rule_file_path.name.endswith("_rule.yaml"):
                try:
                    base_name = rule_file_path.stem.replace("_rule", "")
                    with open(rule_file_path, 'r') as f:
                        rule_data = yaml.safe_load(f)

                    # Ensure mapping file path is in rules folder
                    mapping_file = rule_data.get('mapping_file', f"rules/{base_name}_mapping.csv")
                    
                    rule = FileProcessingRule(
                        base_name=base_name,
                        directory=rule_data.get('directory'),
                        file_pattern=rule_data.get('file_pattern'),
                        date_format=rule_data.get('date_format'),
                        start_row=rule_data.get('start_row'),
                        start_col=rule_data.get('start_col'),
                        mode=rule_data.get('mode', 'insert'),
                        date_from_filename_col_name=rule_data.get('date_from_filename_col_name'),
                        hash_exclude_columns=rule_data.get('hash_exclude_columns', []),
                        search_subdirectories=rule_data.get('search_subdirectories', True),
                        sheet_config=rule_data.get('sheet_config', {}),
                        mapping_file=mapping_file
                    )

                    is_valid, errors = rule.validate()
                    if not is_valid:
                        logger.error(f"Invalid rule {base_name}: {', '.join(errors)}")
                        continue

                    rules.append(rule)
                    logger.info(f"Loaded rule '{base_name}' with directory: {rule.directory}, mapping: {rule.mapping_file}")
                except Exception as e:
                    logger.error(f"Failed to load rule: {e}")

        return rules

    # ---------------------------
    # Pattern extraction helpers
    # ---------------------------
    @staticmethod
    def extract_pattern_and_date_format(filename: str) -> Tuple[Optional[str], Optional[str]]:
        date_patterns = [
            (r'(\d{4})(\d{2})(\d{2})', '%Y%m%d'),
            (r'(\d{2})(\d{2})(\d{2})', '%m%d%y'),
            (r'(\d{2})(\d{2})(\d{4})', '%m%d%Y'),
            (r'(\d{4})-(\d{2})-(\d{2})', '%Y-%m-%d'),
            (r'(\d{2})-(\d{2})-(\d{4})', '%m-%d-%Y'),
            (r'(\d{2})/(\d{2})/(\d{4})', '%m/%d/%Y'),
            (r'(\d{2})(\d{2})(\d{2})', '%d%m%y'),
            (r'(\d{2})(\d{2})(\d{4})', '%d%m%Y'),
        ]

        for date_regex, date_format in date_patterns:
            match = re.search(date_regex, filename)
            if match:
                date_str = ''.join(match.groups())
                try:
                    clean_format = date_format.replace('-', '').replace('/', '')
                    datetime.strptime(date_str, clean_format)

                    pattern = re.sub(date_regex, r'(\d{' + r'\d{'.join([str(len(g)) for g in match.groups()]) + '})', filename)
                    pattern = re.sub(r'([^a-zA-Z0-9\(\)])', r'\\\1', pattern)
                    pattern = '^' + pattern + '$'
                    return pattern, date_format
                except ValueError:
                    continue

        return None, None

    @staticmethod
    def test_pattern_on_filename(pattern: str, filename: str, date_format: Optional[str] = None) -> bool:
        match = re.match(pattern, filename)
        if not match:
            return False

        if date_format and match.groups():
            date_str = ''.join(match.groups())
            try:
                clean_format = date_format.replace('-', '').replace('/', '')
                datetime.strptime(date_str, clean_format)
                return True
            except ValueError:
                return False

        return True

    # ---------------------------
    # Column name sanitization & type inference
    # ---------------------------
    def _sanitize_column_name(self, column_name: str) -> str:
        sanitized = column_name.lower()
        sanitized = re.sub(r'[^a-z0-9_]', '_', sanitized)
        sanitized = sanitized.strip('_')
        if sanitized and sanitized[0].isdigit():
            sanitized = 'col_' + sanitized
        reserved_keywords = {'select', 'insert', 'update', 'delete', 'where', 'join', 'table', 'column', 'index', 'primary', 'key', 'foreign'}
        if sanitized in reserved_keywords:
            sanitized = 'col_' + sanitized
        return sanitized

    def _infer_postgres_type(self, pandas_type: str, sample_value: Any = None) -> str:
        pandas_type_str = str(pandas_type).lower()
        if 'int' in pandas_type_str:
            return 'INTEGER'
        elif 'float' in pandas_type_str:
            return 'NUMERIC'
        elif 'datetime' in pandas_type_str:
            return 'TIMESTAMP'
        elif 'bool' in pandas_type_str:
            return 'BOOLEAN'
        elif 'object' in pandas_type_str:
            if sample_value is not None:
                if isinstance(sample_value, (int, np.integer)):
                    return 'INTEGER'
                elif isinstance(sample_value, (float, np.floating)):
                    return 'NUMERIC'
                elif isinstance(sample_value, (datetime, pd.Timestamp)):
                    return 'TIMESTAMP'
                elif isinstance(sample_value, bool):
                    return 'BOOLEAN'
                elif isinstance(sample_value, str):
                    try:
                        datetime.strptime(sample_value, '%Y-%m-%d')
                        return 'DATE'
                    except (ValueError, TypeError):
                        try:
                            datetime.strptime(sample_value, '%Y-%m-%d %H:%M:%S')
                            return 'TIMESTAMP'
                        except (ValueError, TypeError):
                            if len(sample_value) <= 255:
                                return 'VARCHAR(255)'
                            else:
                                return 'TEXT'
        return 'TEXT'

    # ---------------------------
    # Setup validation & mapping generation
    # ---------------------------
    def _validate_setup(self) -> None:
        logger.info("Validating setup with organized directory structure...")
        for rule in self.processing_rules:
            rule_source_dir = Path(rule.directory)
            if not rule_source_dir.exists():
                rule_source_dir.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created directory: {rule_source_dir}")

            mapping_filepath = Path(rule.mapping_file)

            if not mapping_filepath.exists():
                logger.warning(f"Creating mapping file: {mapping_filepath}")
                sample_file = next(rule_source_dir.glob("*.*"), None)

                if sample_file:
                    self._generate_mapping_file(
                        source_filepath=sample_file,
                        mapping_filepath=mapping_filepath,
                        start_row=rule.start_row or self.global_start_row,
                        start_col=rule.start_col or self.global_start_col,
                        sheet_config=rule.sheet_config
                    )
                else:
                    logger.error(f"No sample file found for {rule.base_name} in {rule.directory}")

            if mapping_filepath.exists():
                try:
                    mapping = pd.read_csv(mapping_filepath)
                    table_created = self.db_manager.create_table_if_not_exists(rule.target_table, mapping)
                    if not table_created:
                        logger.error(f"Failed to ensure table exists: {rule.target_table}")
                except Exception as e:
                    logger.error(f"Error validating table for {rule.target_table}: {e}")

        if not self.db_manager.test_connection():
            raise ConnectionError("Database connection test failed")

        logger.info("Setup validation completed")

    def _generate_mapping_file(self, source_filepath: Path, mapping_filepath: Path,
                              start_row: int = 0, start_col: int = 0, 
                              sheet_config: Dict[str, Any] = None):
        try:
            # For Excel files with sheet config, sample from appropriate sheet
            if source_filepath.suffix.lower() in ['.xlsx', '.xls'] and sheet_config:
                processing_method = sheet_config.get('processing_method', 'specific')
                if processing_method == 'specific':
                    sheet_name = sheet_config.get('specific_sheet', 'Sheet1')
                    df_sample = pd.read_excel(source_filepath, sheet_name=sheet_name, 
                                            skiprows=start_row, nrows=100, header=0)
                else:
                    # For multiple sheets, sample from first available sheet
                    df_sample = pd.read_excel(source_filepath, sheet_name=0, 
                                            skiprows=start_row, nrows=100, header=0)
            else:
                # Existing logic for other file types
                if source_filepath.exists():
                    ext = source_filepath.suffix.lower()
                    if ext == '.csv':
                        df_sample = pd.read_csv(source_filepath, skiprows=range(1, start_row + 1), nrows=100, header=0)
                    elif ext in ['.xlsx', '.xls']:
                        df_sample = pd.read_excel(source_filepath, skiprows=start_row, nrows=100, header=0)
                    elif ext == '.parquet':
                        df_sample = pd.read_parquet(source_filepath).iloc[:100, :]
                    elif ext == '.json':
                        df_sample = pd.read_json(source_filepath).iloc[:100, :]
                    else:
                        df_sample = pd.DataFrame()
                else:
                    df_sample = pd.DataFrame()

            if not df_sample.empty:
                df_sample = df_sample.iloc[:, start_col:]
            columns = df_sample.columns.tolist()
            dtypes = df_sample.dtypes.apply(str).to_dict()

            sample_values = {}
            for col in columns:
                sample_values[col] = df_sample[col].iloc[0] if not df_sample[col].empty else None

            mapping_data = []
            for i, col in enumerate(columns):
                pd_type = dtypes.get(col, 'object')
                sample_value = sample_values.get(col)
                sql_type = self._infer_postgres_type(pd_type, sample_value)
                load_flag = 'Y'
                index_column = 'N'
                if any(pattern in col.lower() for pattern in ['id', 'key', 'code', 'num']):
                    index_column = 'Y'
                mapping_data.append({
                    'RawColumn': col,
                    'TargetColumn': self._sanitize_column_name(col),
                    'DataType': sql_type,
                    'LoadFlag': load_flag,
                    'IndexColumn': index_column,
                    'data_source': 'file',
                    'definition': '',
                    'order': i
                })

            system_columns = [
                ('loaded_timestamp', 'TIMESTAMP', 'Y', 'N'),
                ('source_filename', 'TEXT', 'Y', 'N'),
                ('content_hash', 'TEXT', 'Y', 'N'),
                ('operation', 'TEXT', 'Y', 'N')
            ]

            for col_name, col_type, load_flag, index_column in system_columns:
                mapping_data.append({
                    'RawColumn': col_name,
                    'TargetColumn': col_name,
                    'DataType': col_type,
                    'LoadFlag': load_flag,
                    'IndexColumn': index_column,
                    'data_source': 'system',
                    'definition': '',
                    'order': -1
                })

            mapping_df = pd.DataFrame(mapping_data)

            def custom_sort(row):
                if row['data_source'] == 'system':
                    try:
                        return (0, SYSTEM_COLUMNS_ORDER.index(row['RawColumn']))
                    except ValueError:
                        return (0, len(SYSTEM_COLUMNS_ORDER))
                else:
                    return (1, row['order'])

            mapping_df['sort_key'] = mapping_df.apply(custom_sort, axis=1)
            mapping_df = mapping_df.sort_values('sort_key').drop(columns='sort_key')

            mapping_filepath.parent.mkdir(parents=True, exist_ok=True)
            mapping_df.to_csv(mapping_filepath, index=False)
            logger.info(f"Created mapping file with intelligent defaults: {mapping_filepath}")
        except Exception as e:
            logger.error(f"Mapping generation failed: {e}")
            pd.DataFrame(columns=[
                'RawColumn', 'TargetColumn', 'DataType', 'LoadFlag', 'IndexColumn',
                'data_source', 'definition', 'order'
            ]).to_csv(mapping_filepath, index=False)

    # ---------------------------
    # New columns handling
    # ---------------------------
    def _handle_new_columns(self, df: pd.DataFrame, file_context: FileContext) -> bool:
        """Detect new columns, update mapping file, and block processing until configured."""
        mapping = pd.read_csv(file_context.mapping_filepath)

        current_cols = df.columns.tolist()
        existing_cols = set(mapping[mapping['data_source'] == 'file']['RawColumn'])
        new_cols = [col for col in current_cols if col not in existing_cols]

        if not new_cols:
            return False

        logger.warning(f"New columns detected: {', '.join(new_cols)}")

        col_positions = {col: idx for idx, col in enumerate(current_cols)}

        updated_mapping = []
        for _, row in mapping.iterrows():
            row_dict = row.to_dict()
            if row_dict['data_source'] == 'file':
                row_dict['order'] = col_positions.get(row_dict['RawColumn'], row_dict['order'])
            updated_mapping.append(row_dict)

        for col in new_cols:
            updated_mapping.append({
                'RawColumn': col,
                'TargetColumn': self._sanitize_column_name(col),
                'DataType': 'TEXT',
                'LoadFlag': '',
                'IndexColumn': 'N',
                'data_source': 'file',
                'definition': '',
                'order': col_positions[col]
            })

        updated_df = pd.DataFrame(updated_mapping)
        updated_df = updated_df.sort_values('order')

        updated_df.to_csv(file_context.mapping_filepath, index=False)
        logger.info(f"Updated mapping file with new columns at correct positions")

        configured_new_cols = []
        for _, row in updated_df.iterrows():
            if (row['RawColumn'] in new_cols and
                    row['LoadFlag'] == 'Y' and
                    pd.notna(row['DataType']) and
                    row['DataType'].strip() != ''):
                configured_new_cols.append({
                    'TargetColumn': row['TargetColumn'],
                    'DataType': row['DataType']
                })

        if configured_new_cols and self.config.auto_add_columns:
            logger.info(f"Adding {len(configured_new_cols)} configured new columns to table {file_context.target_table}")
            success = self.db_manager.alter_table_add_columns(file_context.target_table, configured_new_cols)
            if success:
                logger.info(f"Successfully added new columns to table {file_context.target_table}")
                return False
            else:
                logger.error(f"Failed to add new columns to table {file_context.target_table}")
                return True

        logger.warning(f"New columns detected but not configured. Please update {file_context.mapping_filepath}")
        return True

    def _check_and_add_configured_columns(self, file_context: FileContext) -> bool:
        if not self.config.auto_add_columns:
            return True

        mapping = pd.read_csv(file_context.mapping_filepath)

        configured_new_cols = []
        for _, row in mapping.iterrows():
            if (row['data_source'] == 'file' and
                    row['LoadFlag'] == 'Y' and
                    pd.notna(row['DataType']) and
                    row['DataType'].strip() != '' and
                    not self.db_manager.column_exists(file_context.target_table, row['TargetColumn'])):
                configured_new_cols.append({
                    'TargetColumn': row['TargetColumn'],
                    'DataType': row['DataType']
                })

        if configured_new_cols:
            logger.info(f"Adding {len(configured_new_cols)} configured new columns to table {file_context.target_table}")
            success = self.db_manager.alter_table_add_columns(file_context.target_table, configured_new_cols)
            if success:
                logger.info(f"Successfully added new columns to table {file_context.target_table}")
                return True
            else:
                logger.error(f"Failed to add new columns to table {file_context.target_table}")
                return False

        return True

    # ---------------------------
    # Data type validation
    # ---------------------------
    def _validate_data_types(self, df: pd.DataFrame, mapping: pd.DataFrame, filename: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        clean_rows = []
        conflict_rows = []

        file_columns_to_load = mapping[
            (mapping['data_source'] == 'file') &
            (mapping['['LoadFlag'] == 'YLoadFlag'] == 'Y')
        ]

        for index,')
        ]

        for index, row in df.iterrows():
            has_conflict = False
            conflict_details = []

            for _, map_row in file_columns_to_load.iterrows():
                col_name = map_row['RawColumn']
                if col_name in row.index:
                    expected_type = str(map_row['DataType']).upper()
                    value = row[col_name]

                    if expected_type in ['INTEGER', 'NUMERIC', 'DECIMAL', 'FLOAT', 'DOUBLE']:
                        if not self.file_processor._is_numeric(value):
                            has_conflict = True
                            conflict_details.append(f"{col_name}: expected numeric, got '{value}'")

                    elif expected_type.startswith('VARCHAR') or expected_type == 'TEXT':
                        if not isinstance(value, str) and not pd.isna(value):
                            pass

                    elif expected_type in ['DATE', 'TIMESTAMP', 'TIME']:
                        if not self.file_processor._is_date(value):
                            has_conflict = True
                            conflict_details.append(f"{col_name}: expected date/time, got '{value}'")

                    elif expected_type == 'BOOLEAN':
                        if not self.file_processor._is_boolean(value):
                            has_conflict = True
                            conflict_details.append(f"{col_name}: expected boolean, got '{value}'")

            if has_conflict:
                conflict_row = row.copy()
                conflict_row['_conflict_type'] = 'FORMAT_CONFLICT'
                conflict_row['_conflict_details'] = ' | '.join(conflict_details) if conflict_details else 'Unknown format conflict'
                conflict_rows.append(conflict_row)
            else:
                clean_rows.append(row)

        clean_df = pd.DataFrame(clean_rows) if clean_rows else pd.DataFrame()
        conflict_df = pd.DataFrame(conflict_rows) if conflict_rows else pd.DataFrame()

        return clean_df, conflict_df

    # ---------------------------
    # Export helpers - ENHANCED: Use same file format as original
    # ---------------------------
    def _get_conflict_guidance(self, conflict_type: str) -> str:
        guidance = {
            'EXACT_DUPLICATE':
                "ACTION: Delete all but one identical row. These are 100% identical duplicates.",
            'BUSINESS_KEY_CONFLICT':
                "DECISION REQUIRED: Different data for same business key. Keep only one version per key. Delete others or merge data.",
            'EXACT_AND_BUSINESS_CONFLICT':
                "CRITICAL: Both exact and business key duplicates exist. First resolve exact duplicates, then business key conflicts.",
            'FORMAT_CONFLICT':
                "Review data type inconsistencies. Fix the values to match the expected data types.",
            'UNKNOWN_CONFLICT':
                "Manual review required. Unknown conflict type detected."
        }
        return guidance.get(conflict_type, "Manual review required")

    def _export_format_conflicts(self, conflict_df: pd.DataFrame, file_context: FileContext):
        """Export format conflicts using the same file format as the original file."""
        export_filename = self._get_export_filename(file_context.filename, file_context.filepath.suffix)
        export_path = Path(FORMAT_CONFLICT_DIR) / export_filename

        if '_conflict_type' not in conflict_df.columns:
            conflict_df['_conflict_type'] = 'FORMAT_CONFLICT'

        if '_conflict_details' not in conflict_df.columns:
            conflict_df['_ row in df.iterrows():
            has_conflict = False
            conflict_details = []

            for _, map_row in file_columns_to_load.iterrows():
                col_name = map_row['RawColumn']
                if col_name in row.index:
                    expected_type = str(map_row['DataType']).upper()
                    value = row[col_name]

                    if expected_type in ['INTEGER', 'NUMERIC', 'DECIMAL', 'FLOAT', 'DOUBLE']:
                        if not self.file_processor._is_numeric(value):
                            has_conflict = True
                            conflict_details.append(f"{col_name}: expected numeric, got '{value}'")

                    elif expected_type.startswith('VARCHAR') or expected_type == 'TEXT':
                        if not isinstance(value, str) and not pd.isna(value):
                            pass

                    elif expected_type in ['DATE', 'TIMESTAMP', 'TIME']:
                        if not self.file_processor._is_date(value):
                            has_conflict = True
                            conflict_details.append(f"{col_name}: expected date/time, got '{value}'")

                    elif expected_type == 'BOOLEAN':
                        if not self.file_processor._is_boolean(value):
                            has_conflict = True
                            conflict_details.append(f"{col_name}: expected boolean, got '{value}'")

            if has_conflict:
                conflict_row = row.copy()
                conflict_row['_conflict_type'] = 'FORMAT_CONFLICT'
                conflict_row['_conflict_details'] = ' | '.join(conflict_details) if conflict_details else 'Unknown format conflict'
                conflict_rows.append(conflict_row)
            else:
                clean_rows.append(row)

        clean_df = pd.DataFrame(clean_rows) if clean_rows else pd.DataFrame()
        conflict_df = pd.DataFrame(conflict_rows) if conflict_rows else pd.DataFrame()

        return clean_df, conflict_df

    # ---------------------------
    # Export helpers - ENHANCED: Use same file format as original
    # ---------------------------
    def _get_conflict_guidance(self, conflict_type: str) -> str:
        guidance = {
            'EXACT_DUPLICATE':
                "ACTION: Delete all but one identical row. These are 100% identical duplicates.",
            'BUSINESS_KEY_CONFLICT':
                "DECISION REQUIRED: Different data for same business key. Keep only one version per key. Delete others or merge data.",
            'EXACT_AND_BUSINESS_CONFLICT':
                "CRITICAL: Both exact and business key duplicates exist. First resolve exact duplicates, then business key conflicts.",
            'FORMAT_CONFLICT':
                "Review data type inconsistencies. Fix the values to match the expected data types.",
            'UNKNOWN_CONFLICT':
                "Manual review required. Unknown conflict type detected."
        }
        return guidance.get(conflict_type, "Manual review required")

    def _export_format_conflicts(self, conflict_df: pd.DataFrame, file_context: FileContext):
        """Export format conflicts using the same file format as the original file."""
        export_filename = self._get_export_filename(file_context.filename, file_context.filepath.suffix)
        export_path = Path(FORMAT_CONFLICT_DIR) / export_filename

        if '_conflict_type' not in conflict_df.columns:
            conflict_df['_conflict_type'] = 'FORMAT_CONFLICT'

        if '_conflict_details' not in conflict_df.columns:
            conflict_df['_conflictconflict_details']_details'] = 'Unknown format conflict'

 = 'Unknown format conflict        conflict_df['_GUIDANCE'

        conflict_df['_GUIDANCE'] ='] = " "Review data type inconsistencies. Fix the values to match theReview data type inconsistencies. Fix the values to match the expected expected data types and place this file in the format_conf data types and place this file in the format_conflictlict/to_process folder for reprocessing."

        if '_business_key/to_process folder for reprocessing."

        if '_business_key' not' not in conflict_df.columns:
            in conflict_df.columns:
            conflict conflict_df['_business_key_df['_business_key'] = ''

        if '_'] = ''

        if '_businessbusiness_key' in conflict_df.columns_key' in conflict_df.columns:
            conflict_df = conflict:
            conflict_df = conflict_df_df.sort_values(by=['.sort_values(by=['_business_key_business_key', '_conf', '_conflict_type'])

lict_type'])

        # Use        # Use the same file the same file format as original format as
        self._save original
        self._save_dataframe_by_format(conflict_df, export_path, file_context_dataframe_by_format(conflict_df, export_path, file_context.filepath.filepath.suffix)
       .suffix)
        logger.warning(f"Exported {len(conf logger.warning(f"Exported {len(conflict_df)} format conflicts tolict_df)} format conflicts to {export_path}")
        logger.info(f"Review file: {export_path}")

    def _export_duplicates(self, conflict_df: pd.DataFrame, file_context: FileContext):
        """Export duplicates using the same file format as the original file."""
        dup_dir = Path(DUPLICATES_ROOT_DIR)
        dup_dir.mkdir(parents=True, exist_ok=True)

        export_filename = self._get_export_filename(file_context.filename, file_context.filepath.suffix)
        export_path = dup_dir / export_filename

        if '_conflict_type' not in conflict_df.columns:
            conflict_df['_conflict_type'] = 'UNKNOWN_CONFLICT'

        if '_business_key' not in conflict_df.columns:
            mapping = pd.read_csv(file_context.mapping_filepath)
            business_keys = mapping[
                (mapping['IndexColumn'] == 'Y') &
                (mapping['LoadFlag'] == 'Y')
            ]['RawColumn'].tolist()
            if business_keys:
                target_key_cols = []
                for raw_col in business_keys:
                    map_row = mapping[(mapping['RawColumn'] == raw_col) & (mapping['LoadFlag'] == 'Y')]
                    if not map_row.empty:
                        target_col = map_row.iloc[0]['TargetColumn']
                        if target_col in conflict_df.columns:
                            target_key_cols.append(target_col)
                if target_key_cols:
                    conflict_df['_business_key'] = conflict_df[target_key_cols].astype(str).agg('|'.join, axis=1)
                else:
                    conflict_df['_business_key'] = ''
            else:
                conflict_df['_business_key'] = ''

        conflict_df['_GUIDANCE'] = conflict_df['_conflict_type'].apply(self._get_conflict_guidance)

        if '_business_key' in conflict_df.columns:
            conflict_df = conflict_df.sort_values(by=['_business_key', '_conflict_type'])

        # Use the same file format as original
        {export_path}")
        logger.info(f"Review file: {export_path}")

    def _export_duplicates(self, conflict_df: pd.DataFrame, file_context: FileContext):
        """Export duplicates using the same file format as the original file."""
        dup_dir = Path(DUPLICATES_ROOT_DIR)
        dup_dir.mkdir(parents=True, exist_ok=True)

        export_filename = self._get_export_filename(file_context.filename, file_context.filepath.suffix)
        export_path = dup_dir / export_filename

        if '_conflict_type' not in conflict_df.columns:
            conflict_df['_conflict_type'] = 'UNKNOWN_CONFLICT'

        if '_business_key' not in conflict_df.columns:
            mapping = pd.read_csv(file_context.mapping_filepath)
            business_keys = mapping[
                (mapping['IndexColumn'] == 'Y') &
                (mapping['LoadFlag'] == 'Y')
            ]['RawColumn'].tolist()
            if business_keys:
                target_key_cols = []
                for raw_col in business_keys:
                    map_row = mapping[(mapping['RawColumn'] == raw_col) & (mapping['LoadFlag'] == 'Y')]
                    if not map_row.empty:
                        target_col = map_row.iloc[0]['TargetColumn']
                        if target_col in conflict_df.columns:
                            target_key_cols.append(target_col)
                if target_key_cols:
                    conflict_df['_business_key'] = conflict_df[target_key_cols].astype(str).agg('|'.join, axis=1)
                else:
                    conflict_df['_business_key'] = ''
            else:
                conflict_df['_business_key'] = ''

        conflict_df['_GUIDANCE'] = conflict_df['_conflict_type'].apply(self._get_conflict_guidance)

        if '_business_key' in conflict_df.columns:
            conflict_df = conflict_df.sort_values(by=['_business_key', '_conflict_type'])

        # Use the same file format as original
        self._save_dataframe self._save_dataframe_by_format(conflict_df, export_path,_by_format(conflict_df, export_path, file_context.filepath.suffix)
        logger.warning(f"Exported {len(conflict_df)} duplicates to {export_path}")
        logger.info(f"Review file: {export_path}")

    def _export_failed_rows(self, original_df: pd.DataFrame, error_details: List[Dict], 
                           file_context: FileContext, target_table: str):
        """Export failed rows using the same file format as the original file."""
        if not error_details:
            return
            
        # Extract failed row indices
        failed_indices = [error['row_index'] for error in error_details if 'row_index' in error]
 file_context.filepath.suffix)
        logger.warning(f"Exported {len(conflict_df)} duplicates to {export_path}")
        logger.info(f"Review file: {export_path}")

    def _export_failed_rows(self, original_df: pd.DataFrame, error_details: List[Dict], 
                           file_context: FileContext, target_table: str):
        """Export failed rows using the same file format as the original file."""
        if not error_details:
            return
            
        # Extract failed row indices
        failed_indices = [error['row_index'] for error in error_details if 'row_index        failed_df =' in error]
        failed_df = original_df.loc[failed_ind original_df.loc[failed_indices].copy()
        
        # Add errorices].copy()
        
        metadata columns
        for # Add error metadata columns
        for error in error_details:
            error in error_details:
            if ' if 'row_index' inrow_index' in error and error and error['row_index error['row_index'] in failed_df.index:
                failed'] in failed_df.index:
                failed_df.loc_df.loc[error['row_index'], '_error_message'] = error['error']
               [error['row_index'], '_error_message'] = error['error']
                failed_df.loc[error['row_index'], failed_df '_failed_reason'] = self._get_failure_re.loc[error['row_index'], '_failed_reason'] = self._get_failure_reason(error['error'])
        
       ason(error['error'])
        
        # Add guidance # Add guidance column
        column
        failed_df['_ failed_df['_GUIDGUIDANCE'] = "ANCE'] = "Fix theFix the data issues indicated above data issues indicated above. Save this. Save this file with corrections file with corrections and place it in and place it in the failed the failed_rows/to_process folder_rows/to_process folder. The loader will. The loader will automatically remove automatically remove the metadata columns during reprocessing."
        
        # the metadata columns during reprocessing."
        
        # Use the same file format as Use the same file format as original
        export_filename = original
        export_filename = self._get_export_filename(file self._get_export_filename(file_context.filename, file_context.filepath.suffix)
        export_context.filename, file_context.filepath.suffix)
        export_path = Path(FAILED_path = Path(FAILED_ROWS_DIR) / export_ROWS_DIR) / export_filename_filename
        
        self._save_data
        
        self._save_dataframeframe_by_format(failed_df, export_path, file_context.filepath.suffix)
        logger.warning(f"Exported_by_format(failed_df, export_path, file_context.filepath.suffix)
        logger.warning(f"Exported {len(failed_df)} failed {len(failed_df)} rows to {export_path failed rows to {export_path}")
        logger.info(f"Review}")
        logger.info(f"Review file file: {export_path}")

: {export_path}")

    def    def _get_export _get_export_filename(self_filename(self,, original_filename: original_filename: str, original str, original_suffix: str_suffix: str) -> str) -> str:
        """Generate export filename:
        """Generate export filename preserving original format."""
 preserving original format."""
        name_without_ext = Path        name_without_ext = Path(original_filename(original_filename).stem
        return).stem
        return f"{name f"{name_without_ext}{original_suffix_without_ext}{original_suffix}"

}"

    def _save_data    def _save_dataframe_byframe_by_format(self, df_format(self, df: pd: pd.DataFrame, filepath.DataFrame, filepath: Path: Path, original_suffix, original_suffix: str: str):
        """):
        """Save dataframe using the same format asSave dataframe using the same format as the original file the original file."""
        file_ext =."""
        file_ext = original_s original_suffix.lower()
        
        tryuffix.lower()
        
        try:
            if file_ext == '.csv:
            if file_ext == '.csv':
':
                df.to_csv(filepath, index=False                df.to_csv(filepath, index=False)
)
            elif file_ext in ['.xlsx            elif file_ext in ['.xlsx',', '.xls']:
                df.to '.xls']:
                df.to_ex_excel(filepath, index=Falsecel(filepath, index=False,, engine='openpyxl engine='openpyxl')
            elif file_ext ==')
            elif file_ext == '.parquet '.parquet':
                df':
                df.to_parquet.to_parquet(filepath, index(filepath, index=False)
=False)
            elif file_ext            elif file_ext == '. == '.json':
                dfjson':
                df.to_json(file.to_json(filepath,path, orient='records', indent orient='records', indent=2=2)
           )
            else:
                # Default else:
                # Default to CSV for to CSV for unknown formats
                unknown formats
                df.to df.to_csv(filepath.with_csv(filepath.with_suffix_suffix('.csv'), index('.csv'), index=False)
                logger=False)
                logger.warning.warning(f"Unknown file(f"Unknown file format { format {filefile_ext}, exported as CSV instead")
_ext}, exported as CSV instead")
        except Exception as e:
                   except Exception as e:
            logger logger.error(f"Error exporting to.error(f"Error exporting to { {filepath}: {efilepath}: {e}")
}")
            # Fallback to            # Fallback to CSV
 CSV
            df.to_csv            df.to_csv(filepath(filepath.with_suffix('.csv.with_suffix('.csv'),'), index=False)
            logger index=False)
            logger.info(f.info(f"Exported as CSV"Exported as CSV fall fallback: {back: {filepathfilepath.with_suffix('.csv').with_suffix('.csv')}")

}")

    def _get_failure_reason(self, error_message: str) -> str:
        """Convert    def _get_failure_reason(self, error_message: str) -> str:
        """ database errors to user-friendly reasons."""
        error_lowerConvert database errors to user-friendly reasons."""
        error_lower = = error_message.lower()
        
        if 'violates unique constraint' in error_lower:
 error_message.lower()
        
        if 'violates unique constraint' in error_lower:
            return '            return 'DUPLICATEDUPLICATE_KEY'
       _KEY'
        elif 'viol elif 'violates foreign key constraintates foreign key constraint' in' in error_lower:
 error_lower:
            return            return 'MISSING 'MISSING_REF_REFERENCE'
        elif 'ERENCE'
        elif 'invalid input syntaxinvalid input syntax' in error' in error_l_lower:
            returnower:
            return 'DATA_TYPE_M 'DATA_TYPE_MISMATCH'
ISMATCH'
        elif        elif 'null value in 'null value in column' column' in error_lower in error_lower:
            return ':
            return 'MISSMISSING_REING_REQUIRED_VALUE'
       QUIRED_VALUE'
        elif 'value elif 'value too long' in too long' in error_l error_lower:
ower:
                       return 'VALUE_TOO_L return 'VALUE_TOO_LONG'
       ONG'
        elif 'deadlock elif 'deadlock' in error_lower:
            return 'DEADLOCK'
        elif 'connection' in error_lower:
            return 'CONNECTION_ISSUE'
' in error_lower:
            return 'DEADLOCK'
        elif 'connection' in error_lower:
            return 'CONNECTION        else:
_ISSUE'
        else:
            return 'UNKNOWN_ERROR'

            return 'UNKNOWN_ERROR'

    #    # ---------------------------
    # Enhanced database insertion with error ---------------------------
    # Enhanced database insertion with error handling
    # ---------------------------
    def _bulk_insert_to handling
    # ---------------------------
    def _bulk_insert_to_db_db(self, df: pd.DataFrame, target(self, df: pd.DataFrame, target_table: str) -> bool_table: str) -> bool:
        """
        Enhanced bulk:
        """
        Enhanced bulk insert with comprehensive error handling, ret insert with comprehensive error handling, retries, and row-level trackingries, and row-level tracking.
        """
        if df is.
        """
        if df is None or df.empty:
            None or df.empty:
            logger.info("No rows to logger.info("No rows to insert.")
            return True

 insert.")
            return True

        total_rows =        total_rows = len(df)
        
        # Use chunk_size from config
        chunk_size = self.config.chunk_size
 len(df)
        
        # Use chunk_size from config
        chunk_size = self.config.chunk_size
        chunks = [df[i        chunks = [df[i:i + chunk_size] for:i + chunk_size] for i in range(0, i in range(0, total_rows, chunk_size)]
        
        total_rows, chunk_size)]
        
        logger.info logger.info(f(f""Processing {total_rows} rows in {len(chunks)} chunks (chunk_size: {chunk_size})")
        
        successful_rows = 0
        failed_rows = 0
        error_detailsProcessing {total_rows} rows in {len(chunks)} chunks (chunk_size: {chunk_size})")
        
        successful_rows = 0
        failed_rows = 0
        error_details = []
        chunk_failures = 0 = []
        chunk_failures = 0
        
        for chunk_idx
        
        for chunk_idx, chunk, chunk in enumerate(chunks):
 in enumerate(chunks):
                       chunk_result = self._ chunk_result = self._process_chprocess_chunk_with_retunk_with_retry(
ry(
                chunk, target                chunk, target_table,_table, chunk_idx, len chunk_idx, len(chunks(chunks)
            )
            
)
            )
            
            if            if chunk_result['success']:
 chunk_result['success']:
                               successful_rows += chunk successful_rows += chunk_result['processed_result['processed_rows']
                logger_rows']
                logger.info(f.info(f"Chunk {"Chunk {chunkchunk_idx_idx + 1}/{len + 1}/{len(chunks)}: {ch(chunks)}: {chunk_resultunk_result['processed_rows']} rows inserted")
            else['processed_rows']} rows inserted")
            else:
                chunk_f:
                chunk_failuresailures += 1
 += 1
                failed                failed_rows += len(chunk_rows += len(chunk)
               )
                error_details.extend(chunk error_details.extend(chunk_result_result['errors'])
                logger['errors'])
                logger.error.error(f"Chunk {(f"Chunk {chunkchunk_idx + 1_idx + 1}/{len}/{len(chunks)} failed:(chunks)} failed: {ch {chunk_result['errorsunk_result['errors']']}")
                
                # Try individual}")
                
                # Try individual row row insertion for failed chunks if insertion for failed chunks if enabled
                if self.config.enable enabled
                if self.config.enable_row_row_level_recovery:
                   _level_recovery:
                    row_results = self._insert row_results = self._insert_rows_rows_individually(chunk, target_table)
                    successful_individually(chunk, target_table)
                    successful_rows +=_rows += row_results['successful_rows']
                    failed_rows += row row_results['successful_rows']
                    failed_rows += row_results['failed_rows'] -_results['failed_rows'] - len len(chunk) (chunk)  # # Adjust count
                    error_details.extend Adjust count
                    error_details.extend(row(row_results['errors'])
            
           _results['errors'])
            
            # Stop if too many chunks # Stop if too many chunks fail
            if chunk_f fail
            if chunk_failures >= self.config.maxailures >= self.config.max_chunk_failures:
_chunk_failures:
                logger.error(f"Too many                logger.error(f"Too many chunk failures chunk failures ({ ({chunk_failures}), stopping processing")
                break

        # Store error details for potential export
        self._last_insertion_errors = error_details

        # Summary
        if successful_rowschunk_failures}), stopping processing")
                break

        # Store error details for potential export
        self._last_insertion_errors = error_details

        # Summary
        > 0:
            logger.info(f"Successfully if successful_rows > 0:
            logger.info(f"Successfully inserted { inserted {successful_rows}/{successful_rows}/{total_rowstotal_rows} rows into {} rows into {target_table}")
        
        if failed_rows > target_table}")
        
        if failed_rows > 0:
0:
            logger.error            logger.error(f"Failed to insert {failed_rows}/{(f"Failed to insert {failed_rows}/{total_rows} rows into {target_table}")
            
            if selftotal_rows} rows into {target_table}")
            
            if self.config.fail_on_partial_insert.config.fail_on_part and failed_rows > 0ial_insert and failed_rows > 0:
                return False

       :
                return False

        return successful_rows > 0

 return successful_rows > 0    def _process_ch

    def _process_chunk_with_retry(self,unk_with_retry(self chunk: pd.DataFrame, target_table, chunk: pd.DataFrame, target: str, 
                                 chunk_table: str, 
                                 chunk_idx: int, total_ch_idx: int, total_chunks:unks: int) -> Dict[str int) -> Dict[str, Any, Any]:
        """
        Process]:
        """
        Process a chunk with a chunk with retry logic and comprehensive error handling retry logic and comprehensive error handling.
       .
        """
        max_ """
        max_retriesretries = self.config. = self.config.retryretry_attempts
_attempts
        retry_delay =        retry_delay = 1  1  # seconds
        
 # seconds
        
        for attempt        for attempt in range(max_ in range(max_retriesretries + 1):
            + 1):
            try:
 try:
                with self                with self.db_manager.get.db_manager.get_connection() as_connection() as conn:
                    return self._ conn:
                    return self._insert_chinsert_chunk_transaction(unk_transaction(conn, chunkconn, chunk, target_table)
, target_table)
                    
            except                    
            except psycopg psycopg2.2.OperationalError asOperationalError as e:
 e:
                # Connection errors                # Connection errors - retry - retry with backoff with backoff
                if
                if attempt < max attempt < max_ret_retries:
                    waitries:
                    wait_time = min_time = min(retry_d(retry_delay * (elay * (2 ** attempt), self2 ** attempt), self.config.max.config.max_retry_d_retry_delay)
elay)
                                       logger logger.w.warning(farning(f"Chunk {chunk_idx"Chunk {chunk_idx + 1} + 1} attempt {attempt +  attempt {attempt + 1} failed: {1} failed: {e}. Retryinge}. Retrying in {wait_time}s... in {wait_time}s...")
                    time.sleep(wait_time")
                    time.sleep(wait_time)
                    continue
                else)
                    continue
                else:
                    return {
                        'success'::
                    return {
                        'success': False,
                        'processed False,
                        'processed_rows': 0,
                       _rows': 0,
                        'errors 'errors': [f"Oper': [f"OperationalError afterationalError after {max_ret {max_retries}ries} retries: { retries: {str(estr(e)}"]
                    }
)}"]
                    }
                    
                               
            except psycop except psycopg2.Error as e:
                # Databaseg2.Error as e:
                errors - may not be retry # Database errors - may not be retryable
able
                error_code                error_code = e.pgcode = e.pgcode if has if hasattr(e, 'pgattr(e, 'pgcodecode') else 'Unknown'
') else 'Unknown'
                
                               
                if self._is_ if self._is_retryretryable_error(error_codeable_error(error_code) and) and attempt < max_retries:
                    wait_time = min attempt < max_retries:
                    wait_time = min(retry_delay * (2 ** attempt), self.config(retry_delay * (2 ** attempt), self.config.max_retry_delay.max_retry_delay)
)
                    logger.warning(f                    logger.warning(f"Chunk {chunk"Chunk {chunk_idx + 1} attempt {attempt + 1} failed with retryable error {error_code}. Retrying in {wait_idx + 1} attempt {attempt + 1} failed with retryable error {error_code}. Retrying in {wait_time}s...")
                    time_time}s...")
                    time.sleep.sleep(wait(wait_time)
                    continue
                else:
                    return_time)
                    continue
                else:
                    return {
                        'success': {
                        'success': False,
                        'processed_rows': False,
                        'processed_rows': 0,
                        ' 0,
                        'errorserrors': [f"Database': [f"Database error {error_code}: {str(e)} error {error_code}: {str(e)}"]
                    }
                    
            except"]
                    }
                    
            except Exception Exception as e:
                # as e:
                # Unexpected errors Unexpected errors
                return {

                return {
                    '                    'success': False,
                    'success': False,
                    'processed_rows': 0,
processed_rows': 0,
                    'errors':                    'errors': [f"Unexpected error: { [f"Unexpected error: {strstr(e)}"]
                }
(e)}"]
                }
        
        return {
        
        return {
            'success':            'success': False,
            'processed_rows False,
            'processed_rows': ': 00,
            'errors': [",
            'errors': ["MaxMax retries exceeded"]
        }

    def _ retries exceeded"]
        }

    def _insert_chunk_transaction(self, conn, chunkinsert_chunk_transaction(self, conn, chunk: pd.DataFrame, target_table: str): pd.DataFrame, target_table: str) -> Dict[str, Any]:
        """
 -> Dict[str, Any]:
        """
               Insert a chunk within a single transaction with row Insert a chunk within a single transaction with row-level-level error handling.
        """
        cols = list(chunk.columns error handling.
        """
        cols = list(chunk.columns)
        records = chunk.where(pd.notnull(chunk),)
        records = chunk.where(pd.notnull(chunk), None).values.tolist()
 None).values.tolist        
        insert_query = sql()
        
        insert_query =.SQL("INSERT INTO {} sql.SQL("INSERT INTO {} ({}) VALUES %s"). ({}) VALUES %s").formatformat(
            sql.Identifier(target(
            sql.Identifier(target_table),
            sql.SQL_table),
            sql.SQL((',').join(map(sql',').join(map(sql.Identifier.Identifier, cols))
       , cols))
        )
        
        try:
            with conn.cursor() as cursor:
 )
        
        try:
            with conn.cursor() as cursor:
                # Use page_size=len                # Use page_size=len((records) to insert all rowsrecords) to insert all rows in one go
                execute in one go
                execute_values(cursor, insert_query_values(cursor, insert_query.as_string(conn), records, 
                             template=None,.as_string(conn), records, 
                             template=None, page_size= page_size=len(records))
len(records            conn.commit()
            
            return))
            conn.commit()
            
            return {
                'success': True {
                'success': True,
                'processed_rows': len,
                'processed_rows':(chunk),
                'errors len(chunk),
                '': []
            }
            
       errors': []
            }
            
        except psycopg except psycopg22.Error as e:
           .Error as e:
            conn. conn.rollback()
            raise erollback()
            raise e



    def _insert_rows    def _insert_rows_individ_individually(self, chunkually(self, chunk: pd: pd.DataFrame, target_table.DataFrame, target_table: str: str) -> Dict[str) -> Dict[str, Any, Any]:
        """
        Fallback]:
        """
        Fallback: Insert rows individually to identify: Insert rows individually to identify problematic problematic rows.
        """
        rows.
        """
        successful_rows = 0
 successful_rows = 0
        failed_rows = 0
               failed_rows = 0
        errors = []
        
 errors = []
        
        cols = list(chunk.columns)
        cols = list(chunk.columns)
        
        for idx        
        for idx, row, row in chunk.iterrows in chunk():
            try:
               .iterrows():
            try:
                with with self.db self.db_manager_manager.get_connection() as conn:
                   .get_connection() as conn:
                    with with conn.cursor() as cursor:
 conn.cursor() as cursor:
                        values = [row[col] if pd.notnull(row                        values = [row[col] if pd.notnull[col]) else None for col in cols]
(row[col]) else None for col in cols                        insert_query = sql.SQL("INSERT INTO {}]
                        insert_query = sql.SQL("INSERT INTO ({}) VALUES ({})").format(
                            sql.Identifier(target {} ({}) VALUES ({})").format(
                            sql.Identifier(target_table),
                            sql.SQL(','_table),
                            sql.SQL(',').join(map(sql).join(map(sql.Identifier, cols)),
                            sql.S.Identifier, cols)),
                            sql.SQLQL(',').join([(',').join([sqlsql.Placeholder()] * len.Placeholder()] * len(cols))
                        )
                       (cols))
                        )
                        cursor.execute(insert_query, cursor.execute(insert_query, values)
                    conn.commit()
                    values)
                    conn.commit()
 successful_rows += 1
                    
                    successful_rows += 1            except Exception as e:

                    
            except Exception as e                failed_rows += 1:
                failed_rows += 1
                errors.append({

                errors.append({
                    'row_index': idx,
                                       'row_index': idx,
 'row_data': row.to_dict                    'row_data': row(),
                    'error': str(e),
                    'error_type.to_dict(),
                    'error': str(e),
                    'error_type': type(e': type(e).__name__
               ).__name__
                })
 })
                logger.warning(f                logger.warning(f"Failed to insert row {idx}: {e}")
        
        return {
            'successful_rows"Failed to insert row {idx}: {e}")
        
        return {
            'successful_rows': successful_rows,
            'failed': successful_rows,
            'failed_rows': failed_rows,
            '_rows': failed_rows,
           errors': errors
        }

 'errors': errors
        }

    def _is_ret    def _is_retryableryable_error(self, error_error(self, error_code:_code: str) -> bool str) -> bool:
       :
        """
        Determine if an """
        Determine if an error error is retryable based is retryable based on PostgreSQL on PostgreSQL error codes.
        """
 error codes.
        """
        retryable_c        retryable_codes = {
odes = {
            '40001            '40001',  #',  # serialization_f serialization_failure
           ailure
            '40P '40P01',  # deadlock_det01',  # deadlock_detected
            '08006ected
            '08006', ',  # connection_failure
 # connection_failure
            '08000',  # connection            '08000', _exception
            '08003 # connection_exception
            '',  # connection_does08003',  # connection__not_exist
            'does_not_exist
            '08004', 08004',  # # connection connection_rejection
_rejection
            '            '08007',  # transaction_resolution_08007',  # transaction_resolution_unknown
           unknown
            '57014',  # '57014',  # query_cancel query_canceled
            '55Ped
            '55P03', 03',  # lock_not_ # lock_not_available
        }
available
        }
        return error_code in ret        return error_code in retryable_cryable_codes

    # ---------------------------
   odes

    # ---------------------------
    # Files discovery - COR # Files discovery - CORRECTED: Search in to_process directories
RECTED: Search in to_process directories
    # ---------------------------
    def get_files    # ---------------------------
    def get_files_to_process(self)_to_process(self) -> List -> List[FileContext]:
[FileContext]:
        all        all_potential_file_potential_file_contexts =_contexts = []

        for []

        for rule in self rule in self.processing_rules.processing_rules:
            rule:
            rule_source_dir = Path_source_dir = Path(rule(rule.directory)
           .directory)
            if not if not rule_source_dir.exists rule_source_dir.exists():
                logger():
                logger.warning(f.warning(f"Source directory not"Source directory not found: {rule_source_dir}")
 found: {rule                continue

            mapping_filepath =_source_dir}")
                continue

            mapping_filepath = Path(rule.mapping Path(rule.mapping_file)
_file)
            if not mapping            if not mapping_filepath_filepath.exists():
               .exists():
                logger.warning(f"Mapping file not logger.warning(f"Mapping file not found: {mapping_filepath}")
                continue

            if rule.search found: {mapping_filepath}")
                continue

            if rule.search_subdirectories:
_subdirectories:
                search_pattern = rule_source_dir.rglob                search_pattern = rule_source_dir.rglob('('*')
            else:
               *')
            else:
                search_pattern = rule_source_dir.iterdir()

            for file_path in search_pattern = rule_source_dir.iterdir()

            for file_path in search_pattern:
                search_pattern:
                if ( if (file_path.is_filefile_path.is_file() and
() and
                        file_path                        file_path.suffix.lower.suffix.lower() in() in ['.csv', ['.csv', '.xlsx', '. '.xlsx', '.xlsxls', '.parquet', '.parquet', '.', '.json']json'] and
                        not and
                        not any(part in any(part in [DUPL [DUPLICICATES_ROOT_DIR,ATES_ROOT_DIR, FORMAT_CONFLICT FORMAT_CONFLICT_DIR_DIR, FAILED_ROWS, FAILED_ROWS_DIR] for part in file_path.parts_DIR] for part in file_path.parts)):

                    filename = file_path)):

                    filename = file_path.name
                    match.name
                    match = rule = rule.match(filename)

                    if match.match(filename)

                    if match:
:
                        extracted_timestamp = ""
                        extracted_timestamp = ""
                        if rule.date                        if rule.date_format_format and and match match.groups():
                            try:
                                date_str = ""..groups():
                            try:
                                date_str = "".join(match.groups())
                                datetimejoin(match.groups())
                                datetime.strptime(date.strptime(date_str, rule.date_format)
                                extracted_str, rule.date_format)
                                extracted_timestamp = date_timestamp = date_str
                            except ValueError:
                                pass

_str
                            except ValueError:
                                pass

                        file_modified                        file_modified = datetime.fromtimestamp(file_path.stat = datetime.fromtimestamp(file_path.stat().st_mtime)

                       ().st_mtime)

                        file_context = FileContext(
                            file_context = FileContext(
                            filepath filepath=file_path,
=file_path,
                            filename=filename,
                            target_table                            filename=filename,
                            target_table=rule.target_table=rule.target_table,
                            mapping,
                            mapping_filepath=mapping_filepath=mapping_filepath_filepath,
                            extracted_timestamp,
                            extracted_timestamp_str=_str=extracted_timestampextracted_timestamp,
                           ,
                            file_ file_modified_timestamp=file_modified,
modified_timestamp=file_modified,
                            start_row=                            start_row=rule.startrule.start_row or self_row or self.global_start.global_start_row,
_row,
                            start                            start_col=rule.start_col=rule.start_col or self.global_col or self.global_start_start_col,
                           _col,
                            mode=rule.mode or mode=rule.mode or "insert",
 "insert",
                            date_from_filename                            date_from_filename_col_name_col_name=rule.date_from=rule.date_from_filename_col_name,
                            hash_exclude_columns=rule.hash_ex_filename_col_name,
                            hash_exclude_columns=rule.hash_exclude_columns,
                            sheet_config=rule.sheet_config
                       clude_columns,
                            sheet_config=rule.sheet_config )
                        all_potential
                        )
                        all_potential_file_contexts.append(file_file_contexts.append(file_context_context)

        # CORRECTED)

        # CORRECTED: Search in duplicates/to_process/
: Search in duplicates/to        duplicates_to_process_dir =_process/
        duplicates_to_process_dir = Path(DUPLIC Path(DUPLICATESATES_TO_PROCESS_DIR)
_TO_PROCESS_DIR)
        if duplicates_to_process        if duplicates_to_process_dir_dir.exists() and duplicates_to_process.exists() and duplicates_to_process_dir.is_dir.is_dir():
            for_dir():
            for file_path in duplicates_to_process file_path in duplicates_to_process_dir.iterdir():
                if (file_path.is_file() and
                        file_path.suffix.lower() in ['.csv', '.x_dir.iterdir():
                if (file_path.is_file() and
                        file_path.suffix.lower() in ['.csv',lsx', '.xls', '.xlsx', '.xls '.parquet', '.json', '.parquet', '.json']):

                    filename = file']):

                    filename = file_path.name
                    matching_path.name
                    matching_rule = None
                    for_rule = None
                    for rule rule in self.processing_r in self.processing_rules:
                        matchules:
                        match = rule = rule.match(filename)
                        if match:
                            matching_rule.match(filename)
                        if match:
                            matching_rule = rule
                            break

                    if matching_rule:
                        mapping = rule
                            break

                    if matching_rule:
                        mapping_filepath = Path(matching_rule.mapping_file)

                       _filepath = Path(matching_rule.mapping_file)

                        extracted_timestamp = ""
                        if matching_rule.date_format and match extracted_timestamp = ""
                        if matching_rule.date_format and match.groups():
                            try:
.groups():
                            try:
                                date_str = "".                                date_str = "".joinjoin(match.groups())
(match.groups())
                                datetime.strptime(date_str,                                datetime.strptime matching_rule.date_format)
(date_str, matching_rule.date_format)
                                extracted_timestamp = date                                extracted_timestamp = date_str_str
                            except ValueError:

                            except ValueError:
                                                               pass

                        file_ pass

                        file_modified = datetime.fromtimestamp(filemodified = datetime.fromtimestamp(file_path.stat().st_mtime)

                       _path.stat().st_mtime file_context = FileContext(
)

                        file_context = FileContext(
                            filepath=file_path                            filepath=file_path,
                           ,
                            filename=filename filename=filename,
                            target,
                            target_table=matching__table=matching_rule.targetrule.target_table,
                            mapping_table,
                            mapping_filepath=mapping_filepath,
                            extracted_filepath=mapping_filepath,
_timestamp_str=extracted                            extracted_timestamp_str=ext_timestamp,
                            file_modifiedracted_timestamp,
                            file_modified_timestamp=file__timestamp=file_modifiedmodified,
                            start_row,
                            start_row=matching_=matching_rule.start_row or selfrule.start_row.global_start_row,
                            start_col=matching_rule or self.global_start_row,
                            start_col=matching_rule.start_col or self.start_col or self.global_start.global_start_col,
                            mode=m_col,
                            mode=matching_atching_rule.mode orrule.mode or "insert "insert",
                            date_from",
                            date_from_filename_col_name=matching_rule.date_filename_col_name=matching__from_filename_col_name,
                           rule.date_from_filename_col_name,
                            hash_exclude_columns hash_exclude_columns=m=matching_rule.hash_excludeatching_rule.hash_exclude_columns,
                            sheet_config_columns,
                            sheet_config=matching_rule.sheet=matching_rule.sheet_config,
                            is_duplicate=True_config,
                            is_duplicate=True
                        )
                        all_p
                        )
                        all_potential_file_contexts.appendotential_file_contexts.append(file_context(file_context)

        # COR)

        # CORRECTED: Search in format_conflictRECTED: Search in format_conflict/to_process/
       /to_process/
        format_conf format_conflict_tolict_to_process_dir = Path_process_dir = Path(FORMAT_CON(FORMAT_CONFLICTFLICT_TO_PROCESS_DIR_TO_PROCESS_DIR)
        if)
        if format_conflict_to format_conflict_to_process_dir_process_dir.exists() and format.exists() and format_conflict_to_process_dir.is_conflict_to_process_dir.is_dir():
_dir():
                       for file_path in format_conflict_to_process_dir.iterdir():
                if (file_path.is_file for file_path in format_conflict_to_process_dir.iterdir():
                if (file_path.is() and
                        file_path.suffix.lower() in ['.csv', '.xlsx', '.x_file() and
                        file_path.suffix.lower() in ['.csv', '.xlsx', '.xls', '.parquet',ls', '.parquet', '.json']):

                    filename '.json']):

                    filename = file_path.name
                    matching = file_path.name
                   _rule = None
                    for matching_rule = None
                    for rule in self.processing_r rule in self.processing_rules:
                        match = ruleules:
                        match = rule.match(filename)
                        if match.match(filename)
                        if match:
                            matching_rule:
                            matching_ = rule
                            break

                    ifrule = rule
                            break

                    if matching_rule:
                        matching_rule:
                        mapping mapping_filepath = Path(matching__filepath = Path(matching_rule.mapping_file)

                       rule.mapping_file)

                        extracted_timestamp = ""
                        extracted_timestamp = ""
                        if matching_ if matching_rule.date_format and match.groupsrule.date_format and match.g():
                            try:
                                date_strroups():
                            try:
                                date_str = "".join(match.g = "".join(match.groupsroups())
                                datetime.strptime(date())
                                datetime.strptime(date_str_str, matching_rule.date_format)
                                extracted_timestamp, matching_rule.date_format)
                                extracted_timestamp = date = date_str
                            except ValueError:
                                pass_str
                            except ValueError:
                                pass

                        file_modified

                        file_modified = datetime.from = datetime.fromtimestamp(file_path.stat().sttimestamp(file_path.stat().st_mtime)

                        file_context = File_mtime)

                        file_context = FileContext(
                            filepath=Context(
                            filepath=file_path,
                            filename=file_path,
                            filename=filenamefilename,
                            target_table=m,
                            target_table=matching_rule.target_table,
                            mappingatching_rule.target_table,
                           _filepath=mapping_filepath mapping_filepath=mapping_file,
                            extracted_timestamp_strpath,
                            extracted_timestamp_str=extracted_timestamp,
=extracted_timestamp,
                            file_modified_timestamp                            file_modified_timestamp=file_modified,
                            start_row=file_modified,
                           =matching_rule.start_row start_row=matching_rule.start_row or self.global_start or self.global_start_row_row,
                            start_col=matching,
                            start_col=matching_rule_rule.start_col or self.start_col or self.global.global_start_col,
                           _start_col,
                            mode=matching_rule.mode or "insert",
                            date_from_filename_col mode=matching_rule.mode or "insert",
                            date_from_filename_col_name=matching__name=matching_rule.daterule.date_from_filename_col_name,
_from_filename_col_name,
                                                       hash_exclude_ hash_exclude_columnscolumns=matching_rule.hash=matching_rule.hash_exclude_exclude_columns,
                            sheet_config=matching_columns,
                            sheet_config=matching_rule.sheet_config,
                            is_format_conflict=True
                        )
                        all_p_rule.sheet_config,
                            is_format_conflict=True
                        )
                        all_potential_file_contextotential_file_contexts.appends.append(file_context)

        # COR(file_context)

        # CORRECTED: Search in failedRECTED: Search in failed_rows/to_process/
        failed_rows/to_process/
        failed_rows_to_process_dir = Path(_rows_to_process_dir = Path(FAILED_ROWS_TOFAILED_ROWS_TO_PROC_PROCESS_DIR)
        ifESS_DIR)
        if failed failed_rows_to_process_dir.exists()_rows_to_process_dir.exists() and failed_rows_to_process_dir and failed_rows_to_process_dir.is_dir():
            for file_path.is_dir():
            for file_path in failed_rows_to_process_dir.iterdir():
                if in failed_rows_to_process_dir.iterdir():
                if (file_path.is_file() and (file_path.is_file()
                        file_path.suffix.lower and
                        file_path.suffix.lower() in ['.csv',() in ['.csv '.xlsx', '.xls', '.xlsx', '.x', '.parquet', '.json']):

                    filename = filels', '.parquet', '.json']):

                    filename = file_path.name
_path.name
                    matching_                    matching_rule = None
rule = None
                    for                    for rule in self.pro rule in self.processing_rcessing_rules:
                        matchules:
                        match = rule.match(filename)
                        if match:
                            = rule.match(filename)
                        if match:
                            matching_ matching_rule = rule
rule = rule
                            break

                            break

                    if matching                    if matching_rule:
_rule:
                        mapping_filepath                        mapping_filepath = Path(m = Path(matching_rule.matching_rule.mapping_fileapping_file)

                        extracted_timestamp)

                        extracted_timestamp = ""
                        if matching_rule.date_format = ""
                        if matching_rule.date_format and and match.groups():
 match.groups():
                            try:
                                date                            try:
                                date_str =_str = "".join(m "".join(match.gatch.groups())
                                datetime.strptroups())
                                datetime.strptime(date_str,ime(date_str, matching_rule.date_format)
 matching_rule.date_format)
                                extracted_timestamp = date_str
                                                           extracted_timestamp = date_str
                            except except ValueError:
                                pass

                        file_modified = datetime ValueError:
                                pass

                        file_modified = datetime.fromtimestamp(file_path.stat().st_mtime)

                       .fromtimestamp(file_path.stat().st_mtime)

                        file_context = FileContext(
                            file_context = FileContext(
                            filepath=file_path filepath=file_path,
                           ,
                            filename=filename,
                            target filename=filename,
                            target_table_table=matching_rule=matching_rule.target_table.target_table,
                            mapping_filepath=m,
                            mapping_filepath=mapping_filepath,
                           apping_filepath,
                            extracted extracted_timestamp_str=extracted_timestamp,
_timestamp_str=extracted_timestamp,
                                                       file_modified_timestamp=file_modified file_modified_timestamp=file_modified,
                            start_row=matching_rule.start_row or self.global,
                            start_row=matching_rule.start_row or self.gl_start_row,
                            start_colobal_start_row,
                            start_col=matching_rule.start_col=matching_rule.start_col or self.global_start_col or self.global_start_col,
,
                            mode=matching_                            mode=matching_rule.mode or "rule.mode or "insert",
                            date_from_filename_col_name=minsert",
                            date_from_filename_col_name=matching_rule.date_fromatching_rule.date_from_filename_filename_col_name,
_col_name,
                                                       hash_exclude_columns=matching_ hash_exclude_columns=matching_rule.hash_exclude_columns,
                           rule.hash_exclude_columns,
                            sheet_config=matching sheet_config=matching_rule_rule.sheet_config,
                           .sheet_config,
                            is_failed is_failed_row=True
_row=True
                        )
                                               )
                        all_pot all_potential_file_contexts.append(file_context)

       ential_file_contexts.append(file_context)

        # Filter based on # Filter based on progress tracker progress tracker and audit checks
 and audit checks
        files        files_to_process = []
_to_process = []
        for        for fc in all_p fc in all_potential_file_contexts:
            if anyotential_file_contexts:
            if any([fc.is_du([fc.is_duplicate,plicate, getattr getattr(fc, '(fc, 'is_format_conflict', Falseis_format_conflict', False), getattr(fc, 'is_failed_row), getattr(fc, 'is_failed_row', False)]', False)]):
                files):
                files_to_process.append(fc)
               _to_process.append(fc continue

            if self.progress)
                continue

            if self.progress_tracker:
                needs_tracker:
                needs_processing_processing = self.progress = self.progress_tracker_tracker.needs_processing.needs_processing(fc.filepath, fc)
                if not(fc.filepath, fc)
                if not needs_processing:
 needs_processing:
                    logger                    logger.info(f"Ski.info(f"Skipping unchangedpping unchanged file: {fc file: {fc.filename.filename}")
                    continue

}")
                    continue

            if fc.mode == "audit            if fc.mode == "audit" and not any" and not any([fc.is([fc.is_duplicate, get_duplicate, getattrattr(fc, 'is(fc, 'is_format_format_conflict', False), get_conflict', False), getattrattr(fc, 'is(fc, 'is_failed_row', False)]):
               _failed_row', False)] try:
                    if self.db):
                try:
                    if self.db_manager.file_exists_in_db(f_manager.file_exists_in_db(fc.targetc.target_table_table, fc, fc.file_.file_modified_timestamp, fc.filename):
modified_timestamp, fc.filename):
                        logger                        logger.info(f"Skipping {.info(f"Skipping {fc.ffc.filename}: Content unchanged in database")
                       ilename}: Content unchanged in database")
                        if self.progress_tracker:
                            self.pro if self.progress_tracker:
                            self.progress_tracker.mark_processed(fc.filegress_tracker.mark_processed(fc.filepath, fcpath, fc)
                        continue
                except Exception as e:
)
                        continue
                except Exception as e:
                    logger.error(f"Audit check failed: {e                    logger.error(f"Audit check failed: {e}")

            files}")

            files_to_to_process.append_process.append(fc)

       (fc)

        logger.info(f"Files to logger.info(f"Files to process: {len(files_to_process)} process: {len(files_to_process)}")
        return files")
        return files_to_to_process

    #_process

    # ---------------------------
 ---------------------------
    # Processing    # Processing loop - ENHANCED: Consistent file movement loop - ENHANCED: Consistent file movement to processed folders to processed folders
    # -------------------------
    # ---------------------------
   --
    def process_files def process_files(self) -> Dict(self) -> Dict[str, int[str, int]:
        start]:
        start_time = time.time()
        logger.info(f_time = time.time()
        logger.info(f"=== STARTING RUN {self.run_id}"=== STARTING RUN {self.run_id} ===")

        file_context ===")

        file_contexts = self.get_files_tos = self.get_files_to_process()
        processed, failed =_process()
        processed, failed = 0, 0

 0, 0

        for fc in        for fc in file_contexts:
            if self.process_file(fc file_contexts:
            if self.process_file(fc):
                processed +=):
                processed += 1 1
           
            else:
 else:
                failed +=                failed += 1

        1

        duration = time.time() - start_time
        logger.info duration = time.time() - start_time
        logger.info(f"Run(f"Run completed. Processed completed. Processed: {: {processed}, Failed:processed}, Failed: {failed {failed}, Duration: {}, Duration: {duration:.duration:.1f}s")

1f}s")

        return {"processed": processed, "failed": failed}

    def process_file        return {"processed": processed, "failed": failed}

    def process_file(self, file_context:(self, file_context: File FileContext) -> bool:
       Context) -> bool:
        try:
            mapping = pd try:
            mapping = pd.read_csv(file_context.mapping_filepath.read_csv(file_context.mapping_filepath)
            if not)
            if not self self.db_manager.create_table_if_not_exists.db_manager.create_table_if_not_exists(file_context.target_table(file_context.target_table, mapping):
                logger.error(f"Cannot process, mapping):
                logger.error(f"C {file_context.filenameannot process {file_context.filename}: Table {}: Table {filefile_context_context.target_table} creation failed.target_table} creation failed")
")
                return False

            # CONS                return False

            # CONSISTENTISTENT PROCESSING FLOW FOR ALL T PROCESSING FLOW FOR ALL TYPES
            if getattr(file_context, 'isYPES
            if getattr(file_context,_failed_row', False):
                'is_failed_row', False):
                logger.info(f" logger.info(f"ProcessingProcessing failed rows file: { failed rows file: {filefile_context.filename}")
_context.filename}")
                success = self._process_f                success = self._process_failed_rows_file(file_context)
                # ENHANCED: Move to processed folderailed_rows_file(file_context)
                # ENHANCED: Move to processed folder regardless of success/f regardless of success/failureailure
                if success:

                if success:
                    self._move_to_processed(file                    self._move_to_processed(file_context.filepath, "failed_rows")
                else:
                    logger.error_context.filepath, "failed_rows")
                else:
                    logger.error(f"Failed to process failed rows file:(f"Failed to process failed rows file: {file {file_context.filename}")
                    
            elif file_context.is_du_context.filename}")
                    
            elif file_context.is_duplicate:
                logger.info(fplicate:
                logger.info(f"Processing duplicate file: {file"Processing duplicate file: {_context.filename}")
                success =file_context.filename}")
                success = self._process_duplicate_file(file_context)
                # self._process_duplicate_file(file_context)
                # ENHANCED: Move to processed folder regardless of success/failure
                if success:
                    self._move_to_processed(file_context.filepath, "duplicates ENHANCED: Move to processed folder regardless of success/failure
                if success:
                    self._move_to_processed(file_context.filepath, "duplicates")
                else:
                    logger.error(f"Failed to process duplicate file: {file_context.filename}")
                    
            elif getattr(file_context, 'is_format_conflict', False):
                logger.info(f"Processing format")
                else:
                    logger.error(f"Failed to process duplicate file: {file_context.filename}")
                    
            elif getattr(file_context, 'is_format_conflict', False):
                logger.info(f"Processing format conflict file: {file_context.filename}")
                success = self._process_format_conflict_file(file_context)
                # ENHANCED: Move conflict file: {file_context.filename}")
                success = self._process_format_conflict_file(file_context)
                # ENHANCED: Move to processed folder regardless of success/failure
                if success:
                    self._move_to_processed(file_context.filepath, "format_conf to processed folder regardless of success/failure
                if success:
                    self._move_to_processed(file_context.filepath, "format_conflict")
                else:
                    logger.error(f"Failed to process format conflict file: {file_context.filename}")
                    
            else:
                # Check if it's anlict")
                else:
                    logger.error(f"Failed to process format conflict file: {file_context.filename}")
                    
            else:
                # Check if it's an Excel file Excel file with multi-sheet configuration
                if (file_context.filepath.suffix.lower() in ['.xlsx', '.xls'] and 
                    file_context.sheet with multi-sheet configuration
                if (file_context.filepath.suffix.lower() in ['.xlsx', '.xls'] and 
                    file_context.sheet_config.get_config.get('processing_method', 'specific') != 'specific'):
                    logger.info(f"Processing multi-sheet Excel file: {file_context.filename}")
                    success = self._process_excel_file(file_context)
                else:
                    logger.info(f"Processing:('processing_method', 'specific') != 'specific'):
                    logger.info(f"Processing multi-sheet Excel file: {file_context.filename}")
                    success = self._process_excel_file(file_context)
                else:
                    logger.info(f"Processing: {file_context.f {file_context.filename} -> Table: {file_contextilename} -> Table: {file_context.target_table}")
.target_table}")
                    success = self._                    success = self._process_single_sheet_file(file_context)

process_single_sheet_file(file                # ENHANCED:_context)

                # ENHANCED: Mark progress for regular files Mark progress for regular files
               
                if success and self if success and self.progress_tracker:
                    self.progress_tracker:
                    self.progress_tracker.mark.progress_tracker.mark_processed(file_processed(file_context_context.filepath,.filepath, file_context)

            # file_context)

            # Optionally delete file after successful processing (only for Optionally delete file after successful processing (only for original original files)
            if success files)
            if success and self.delete_files and self.delete_files and not any and not any([
                file_context.is_duplicate,
([
                file_context.is_duplicate,
                getattr(file                getattr(file_context, '_context, 'is_format_conflictis_format_conflict',', False),
                False),
                getattr(file getattr(file_context, 'is_f_context, 'is_failed_row', False)
            ]):
               ailed_row', False)
            ]):
                try:
                    os try:
                    os.remove(file.remove(file_context.filepath)
_context.filepath)
                    logger.info                    logger.info(f"Deleted(f"Deleted source file: {file_context.filepath}")
 source file: {file_context.filepath}")
                except Exception as                except Exception as e:
                    e:
                    logger.warning(f logger.warning(f"Could"Could not delete file { not delete file {file_contextfile_context.filepath}: {e}")

            return success

.filepath}: {e}")

            return success

        except Exception        except Exception as e:
            as e:
            logger.error logger.error(f"Error processing(f"Error processing {file {file_context.filename}:_context.filename}: {e}", exc_info=True)
            return {e}", exc_info=True)
            return False

    False

    # ---------------------------
 # ---------------------------
    #    # Metadata removal helper
    # ---------------------------
    Metadata removal helper
    # ---------------------------
    def _ def _drop_metadata_drop_metadata_columns(self, dfcolumns(self, df: pd: pd.DataFrame) ->.DataFrame) -> pd.DataFrame pd.DataFrame:
       :
        """Remove loader-generated """Remove loader-generated metadata columns if present."""
        metadata columns if present."""
        if df is None if df is None or df.empty:
            return or df.empty:
            return df
        df
        drop_cols = [c for c in df.columns if c in METADATA drop_cols = [c for c in df.columns_COLUMNS]
        if drop_cols:
 if c in METADATA_COLUMNS]
        if drop_cols:
            logger.debug(f"D            logger.debug(f"Dropping metadata columns: {dropropping metadata columns: {drop_cols}")
            df_cols}")
            df = df.drop(columns=drop_cols, errors = df.drop(columns=drop_cols, errors="ignore")
       ="ignore")
        return df return df

    # -----------------

    # ---------------------------
   ----------
    # Multi-sheet Excel processing
    # ----------------- # Multi-sheet Excel processing
    # ---------------------------
    def _----------
    def _process_excel_file(self, file_contextprocess_excel_file(self, file_context: FileContext): FileContext) -> bool -> bool:
        """Process Excel:
        """Process Excel file with multiple sheets."""
        processing file with multiple sheets."""
       _method = file_context.sheet processing_method = file_context.sheet_config.get('processing_method',_config.get('processing_method', ' 'specific')
        success =specific')
        success = True
        sheets_processed = 0
        total_sheets True
        sheets_processed = 0
        total_sheets = 0
        
        if processing_method == 'specific':
            # Single sheet processing
            sheet_result = = 0
        
        if processing_method == 'specific':
            # Single sheet processing
            sheet_result = self._process_s self._process_single_exingle_excel_sheetcel_sheet(file_context)
(file_context)
            if sheet            if sheet_result:
                sheets_result:
                sheets_processed += 1
           _processed += 1
            else:
                success = else:
                success = False
 False
        else:
            #        else:
            # Multiple sheet processing
            sheet_names = self._ Multiple sheet processing
            sheet_names = self._get_sheetsget_sheets_to_process(file_to_process(file_context)
            total_context)
            total_sheets = len_sheets = len(sheet_names)
            
            for(sheet_names)
            
            for sheet_name in sheet_name in sheet_names:
                logger.info(f"Processing sheet { sheet_names:
                logger.info(f"Processing sheet {sheets_processedsheets_processed + 1 + 1}/{total_s}/{total_sheets}: '{sheetheets}: '{sheet_name}'_name}'")
                
                # Create a copy of file_context")
                
                # Create a copy of file_context for this sheet
 for this sheet
                sheet                sheet_context = datac_context = dataclasses.replacelasses.replace(file_context)
               (file_context)
                sheet_context.source_sheet = sheet_name sheet_context.source_sheet = sheet_name
               
                sheet sheet_context.sheet_context.sheet_config['_config['specific_sheet'] =specific_sheet'] = sheet_name
                sheet sheet_name
                sheet_context.sheet_config['processing_method'] = 'specific'
                
               _context.sheet_config['processing_method'] = 'specific'
                
                sheet_result = self sheet_result = self._process_single_excel._process_single_excel_sheet(s_sheet(sheet_context)
heet_context)
                
                if sheet_result:
                    sheets_                
                if sheet_result:
                    sheets_processed += 1
processed += 1
                else:
                    success =                else:
                    success = False False
                    logger.error(f
                    logger.error(f"Failed to process sheet '{sheet_name"Failed to process sheet '{sheet_name}'")
        
       }'")
        
        if sheets if sheets_processed == 0_processed == 0 and total_s and total_sheets > 0:
heets > 0:
            if            if self.config.treat_empty_as_error self.config.treat_empty_as_error:
                logger.error(f"All {total_sheets} sheets in:
                logger.error(f"All {total_sheets} sheets in {file_context.filename {file_context.filename} were empty")
} were empty")
                success = False                success = False
            else:
               
            else:
                logger.warning(f"All {total_sheets} sheets in { logger.warning(f"All {total_sheets} sheets in {file_contextfile_context.filename} were empty")
                success = True  # Empty sheets are not.filename} were empty")
                success = True  # Empty sheets are not considered errors
        
        logger.info considered errors
        
        logger.info(f"Processed {sheets(f"Processed {sheets__processed} sheet(s) from {processed} sheet(s) from {file_context.filename}")
       file_context.filename}")
        return success

    def _ return success

    def _get_sheets_to_process(selfget_sheets_to_process(self, file_context: FileContext, file_context: FileContext)) -> List[str]:
        -> List[str]:
        """ """Get list of sheet names toGet list of sheet names to process based on configuration."""
 process based on configuration        file_processor = FileProcessor(self."""
        file_processor = FileProcessor(self.config)
        all_sheets = file_processor.get_excel_sheet_names(file_context.filepath.config)
        all_sheets = file_processor.get_excel_sheet_names(file_context.filepath)
        
        processing_method)
        
        processing_method = file_context.sheet = file_context.sheet_config.get('processing_method', 'specific_config.get('processing_method', 'specific')
        
        if')
        
        if processing_method == 'multiple':
            specified processing_method == 'multiple':
            specified_sheets = file_context.sheet_sheets = file_context.s_config.get('sheet_names',heet_config.get('sheet_names', [])
            return [ [])
            return [sheetsheet for sheet in specified_sheets if for sheet in specified_sheets if sheet in sheet in all all_s_sheetsheets]
        
        elif processing_method]
        
        elif processing_method == 'all':
            return all_sheets
        
        == 'all':
            return all_sheets
        
        elif processing_method == 'pattern':
            pattern elif processing_method == 'pattern':
            pattern = file = file_context.sheet_config.get('sheet_name_pattern_context.sheet_config.get('sheet_name_pattern', '.*')
            return [sheet', '.*')
            return [ for sheet insheet for sheet in all_sheets if re.match(pattern, all_sheets if re.match(pattern, sheet sheet)]
        
        else:
           )]
        
        else:
            return [file_context.sheet_config.get return [file_context.sheet_config.get('specific_sheet('specific_sheet', 'Sheet1')]

    def', 'Sheet1')]

    def _process_single_excel _process_single_excel_sheet(self, file_context:_sheet(self, file_context: FileContext) -> FileContext) -> bool bool:
        """Process a single:
        """Process a single sheet from an Excel file."""
        sheet from an Excel file."""
 original_filename = file_context        original_filename = file_context.f.filename
        
ilename
        
        try:
            df =        try:
            df = self.file_processor.load_file(
                self.file_processor.load_file(
                file_context.filepath,
                file_context file_context.filepath,
                file_context.start_row.start_row,
                file_context.start_col,
                file_context,
                file_context.start_col,
                file_context.sheet.sheet_config
            )
            
            # Enhanced empty detection
            if df is None or df.empty_config
            )
            
            # Enhanced empty detection
            if df is None or df.empty:
               :
                if file_context.source if file_context.source_sheet_sheet:
                    logger.w:
                    logger.warning(f"arning(f"Sheet contains no data: {file_context.fSheet contains no data: {file_context.filename} - {file_contextilename} - {file_context.source_sheet}")
                else.source_sheet}")
                else:
                    logger.warning(f"File:
                    logger.warning(f"File contains no data: contains no data: {file {file_context.filename}")
                
_context.filename}")
                
                # For empty sheets, we                # For empty sheets, we still want still want to mark as to mark as processed if using progress tracking
                processed if using progress tracking
                if self.progress_tracker and not if self.progress_tracker and not any([
                    get any([
                    getattr(file_context,attr(file_context, 'is_format 'is_format_conflict', False),
_conflict', False),
                    get                    getattr(file_context,attr(file_context, 'is_failed_row', 'is_failed_row', False)
                ]):
                    self.pro False)
                ]):
                    self.progress_tracker.mark_processedgress_tracker.mark_processed(file_context.filepath, file_context(file_context.filepath, file_context)
                
                return)
                
                return True  # Success - empty sheets are not True  # Success - empty sheets are not errors
            
 errors
            
            #            # Add sheet name to Add sheet filename for tracking
            if file_context.source_sheet:
                file_context.filename = f"{original_filename}::{ name to filename for tracking
            if file_context.source_sheet:
                file_context.filename = f"{original_filename}::{file_contextfile_context.source_sheet}"
.source_sheet}"
            
            # Process the dataframe
                       
            # Process the dataframe
            result = self._ result = self._process_dataframeprocess_dataframe(df, file_context(df, file_context)
            
           )
            
            return result
            
        return result
            
        except Exception as e:
            logger.error except Exception as e:
            logger.error(f"Error processing(f"Error processing sheet { sheet {file_context.source_sfile_context.source_sheet}: {e}")
            return False
heet}: {e}")
            return False
        finally:
            #        finally:
            # Rest Restore original filename
           ore original filename
            file_context.filename = original_filename

 file_context.filename = original_filename

    def _process    def _process_single_single_sheet_file(self_sheet_file(self, file_context: FileContext) -> bool:
, file_context: FileContext) -> bool:
        """Process non        """Process non-Excel-Excel files or single-s files or single-sheet Excelheet Excel files."""
        df files."""
        df = self.file_processor.load_file(
            = self.file_processor.load_file(
            file_context.file file_context.filepath,
            filepath,
            file_context.start_row,
_context.start_row,
            file_context.start_col,
            file            file_context.start_col,
            file_context.sheet_config_context.sheet_config
       
        )
        
        if )
        
        if df is None or df.empty:
            df is None or df.empty:
            logger.warning(f logger.warning(f"File is empty"File is empty: {file_context: {file_context.filename}")
            return True.filename}")
            return True
        
       
        
        return self._process_dataframe(df, file_context return self._process_dataframe(df, file_context)

    def _)

    def _process_dataframe(self,process_dataframe(self, df: pd.DataFrame, file_context: FileContext) -> df: pd.DataFrame, file_context: FileContext) -> bool:
        """Common dataframe processing logic for all file types."""
        bool:
        """Common dataframe processing logic for all file types."""
        # Automatically remove metadata columns if present
        df = self._drop_metadata_columns # Automatically remove metadata columns if present
        df = self._drop_metadata_columns(df)

        # Handle new columns ((df)

        # Handle new columns (this will update mapping and maythis will update mapping and may block)
        should_block = block)
        should_block = self._handle_new_columns self._handle_new_columns(df(df, file_context)
        if, file_context)
        if should_block should_block:
            logger.error(f"New columns detected in {:
            logger.error(f"New columns detected in {file_context.filenamefile_context.filename}. Please configure them}. Please configure them in {file_context.mapping in {file_context.mapping_filepath}.")
            return False

        # Check_filepath}.")
            return False

        # Check and add configured columns if necessary
 and add configured columns if necessary
        if not self._check        if not self._check_and_add_configured_columns(file_context):
            logger_and_add_configured_columns(file_context):
            logger.error(f"Failed to add.error(f"Failed to add configured columns to table {file_context.target configured columns to table {file_context_table}")
            return False

.target_table}")
            return False

        mapping = pd        mapping = pd.read_csv(file_context.mapping_filepath.read_csv(file_context.mapping_filepath)

        # Validate data types)

        # Validate data types and separate format conflicts
        clean and separate format conflicts
        clean_df, format_conflict_df_df, format_conflict_df = self._validate_data_types(df = self._validate_data_types(df, mapping, file, mapping, file_context.f_context.filename)

        #ilename)

        # Export format conflicts if any
        if not format_conflict_df.empty Export format conflicts if any
        if not format_conflict_df.empty:
           :
            self._export_format_conflicts self._export_format_conflicts(format_conflict_df, file(format_conflict_df, file_context)
            logger.warning_context)
            logger.warning(f"Found {len(format_conf(f"Found {len(format_conflict_df)} format conflicts inlict_df)} format conflicts in {file_context.filename}. These {file_context.filename}. These rows have been exported for manual rows have been exported for manual correction.")

        if clean_df correction.")

        if clean_df.empty:
            logger.warning.empty:
            logger.warning(f(f"No valid rows to process in"No valid rows to process in { {file_context.filename} after filtering format conflicts")
            returnfile_context.filename} after filtering format conflicts")
            return True

        # True

        # Drop metadata columns again on clean_df ( Drop metadata columns again on clean_df (just in case)
        clean_dfjust in case)
        clean_df = self._drop_metadata_ = self._drop_metadata_columns(clean_df)

       columns(clean_df)

        # Calculate hashes
        # Calculate hashes
        hash_exclude = set(file hash_exclude = set(file_context.hash_exclude_columns_context.hash_exclude_columns or [])
        clean_df['content_hash'] = self.file_ or [])
        clean_df['content_hash'] = self.file_processor.cprocessor.calculate_rowalculate_row_hashes(clean_df,_hashes(clean_df, hash_exclude hash_exclude)

        # Duplicate detection & export)

        # Duplicate detection & export
        existing_
        existing_hashes = self.dbhashes = self.db_manager.get_existing_hashes(file_context.target_manager.get_existing_hashes(file_context.target_table, file_context.f_table, file_context.filenameilename)
        duplicates_mask =)
        duplicates_mask = clean clean_df['content_hash_df['content_hash'].isin'].isin(existing(existing_hashes)
        duplicates_df = clean_df[duplicates_hashes)
        duplicates_df = clean_df[duplicates_mask]
        unique_df = clean_df[~du_mask]
        unique_df = clean_df[~duplicates_mask]

       plicates_mask]

        if not duplicates_df.empty:
            # Export duplicates for if not duplicates_df.empty:
            # Export duplicates for manual resolution
            self._export_du manual resolution
            self._export_duplicates(duplicates_dfplicates(duplicates_df, file, file_context)
            logger.warning_context)
            logger.warning(f"Exported {len(du(f"Exported {len(duplicates_df)} duplicateplicates_df)} duplicate rows from rows from {file_context.f {file_context.filename} to {DUPLICATESilename} to {DUPLICATES_ROOT_DIR}")

        if_ROOT_DIR}")

        if unique unique_df.empty:
_df.empty:
            logger.info(f"No new unique rows to load            logger.info(f"No new unique rows to load from {file_context from {file_context.filename.filename}")
            return True}")
            return True

        # Prepare for DB insert:

        # Prepare for rename columns to target names based on DB insert: rename columns to target names based on mapping
        mapping_df mapping
        mapping_df = pd.read_csv(file_context.mapping = pd.read_csv(file_context.mapping_filepath)
       _filepath)
        col_map = {row[' col_map = {row['RawColumn']: row['TargetColumnRawColumn']: row['TargetColumn'] for _, row'] for _, row in mapping in mapping_df.iterrows() if_df.iterrows() if row[' row['data_source'] == 'data_source'] == 'file'}
        insert_df = unique_dffile'}
        insert_df = unique_df.copy()
       .copy()
        insert_df = insert_df = insert_df.rename( insert_df.rename(columns=col_map)

        #columns=col_map)

        # Add system columns - FIXED: Add system columns - FIXED: Remove milliseconds Remove milliseconds from timestamp
        insert_df['loaded_timestamp from timestamp
        insert_df['loaded_timestamp'] = datetime.now'] = datetime.now().replace().replace(microsecond=(microsecond=0)  # No milliseconds
       0)  # No milliseconds
        insert_df['source_filename'] = file insert_df['source_filename'] = file_context.filename_context.filename  # Includes sheet name if applicable
        insert_df['operation  # Includes sheet name if applicable
       '] = file_context.mode insert_df

        # Load into DB with enhanced error handling
       ['operation'] = file_context.mode

        # Load into DB with enhanced error try:
            success = self handling
        try:
            success = self._bulk_insert_to_db(insert._bulk_insert_to_db(insert_df, file_context.target_df, file_context.target_table_table)
            
            # If there)
            
            # If there were failed rows during insertion, were failed rows during insertion, export them
            if export them
            if not success and hasattr(self, '_ not success and hasattr(self,last_insertion_ '_last_insertion_errors') anderrors') and self._last_insertion_errors:
 self._last_insertion_errors:
                self._export_failed_rows(unique_df, self._last                self._export_failed_rows(unique_df, self._last_insertion_errors, file_context, file_context.target_table)
                
           _insertion_errors, file_context, file_context.target_table)
                
            return success
        except Exception as e:
            logger return success
        except Exception as e:
            logger.error(f"Load failed for {.error(f"Load failed for {file_context.filename}: {efile_context.filename}: {e}", exc_info=True)
           }", exc_info=True)
            return False

    # ----------------- return False

    # ---------------------------
    # Reprocessing----------
    # Reprocessing methods - UPDATED for methods - UPDATED for consistency
    # ---------------------------
    consistency
    # ---------------------------
    def _process_duplicate_file(self, file_context: def _process_duplicate_file(self, file_context: FileContext) -> bool:
 FileContext) -> bool:
        """Process a resolved duplicate        """Process a resolved duplicate file placed in duplicates/to_process file placed in duplicates/to_process/"""
        df =/"""
        df = self.file_processor.load_file(
            file_context.filepath,
            file self.file_processor.load_file(
            file_context.filepath,
            file_context.start_row,
            file_context.start_row,
            file_context.start_col,
            file_context.start_col,
            file_context.sheet_config
        )
_context.sheet_config
        )
        if df is None        if df is None or df.empty:
            logger.w or df.empty:
            logger.warning(f"Duplicate file is emptyarning(f"Duplicate file is empty: {file_context: {file_context.filename.filename}")
            return True

        # Remove metadata automatically - CONS}")
            return True

        # Remove metadata automatically - CONSISTENTISTENT BEHAVIOR
        df = self._ BEHAVIOR
       drop_metadata_columns(df df = self._drop_metadata_columns(df)

        # Proceed as a regular)

        # Proceed as a regular file from here
 file from here
        return self._        return self._process_dataframe(df, file_context)

   process_dataframe(df, file_context)

    def _process def _process_format_conflict_file_format_conflict_file(self, file(self, file_context: File_context: FileContext) -> bool:
        """Process a correctedContext) -> bool:
        """Process format conflict file placed in format a corrected format conflict file placed in format_conflict/to_process/"""
        df = self_conflict/to_process/"""
        df = self.file_processor.file_processor.load_file(
.load_file(
            file_context.file            file_context.filepath,
            file_context.start_row,
path,
            file_context.start_row,
            file_context.start            file_context.start_col,
_col,
            file_context.s            file_context.sheet_config
       heet_config
        )
        if df is None or df.empty:
            logger.warning(f" )
        if df is None or df.empty:
            logger.warning(f"Format conflictFormat conflict file is file is empty: empty: { {file_context.filename}")
            return Truefile_context.filename}")
            return True

        # Remove metadata

        # Remove metadata automatically - CONSISTENT BEHA automatically - CONSISTENT BEHAVIOR
        df = self._drop_metadataVIOR
        df = self._drop_metadata_columns(df)

_columns(df)

        #        # Proceed as regular file
        return self._ Proceed as regular file
process_dataframe(df, file        return self._process_dataframe(df, file_context)

    def_context)

    def _process_failed_rows_file(self, _process_failed_rows_file(self, file_context: File file_context: FileContext) ->Context) -> bool:
        bool:
        """Process a corrected failed rows """Process a corrected failed rows file - CONSISTENT with duplicates/conflicts file - CONSISTENT with duplicates/conflicts."""
       ."""
        df = self.file_processor.load_file(
            df = self.file_processor.load_file(
            file_context.filepath file_context.filepath,
           ,
            file_context.start_row file_context.start_row,
            file_context.start_col,
            file_context.start_col,
            file_context.sheet_config
,
            file_context.sheet_config
        )
        
        if df is        )
        
        if df is None or df.empty:
            logger.warning(f"Failed rows file is empty: {file_context None or df.empty:
            logger.warning(f"Failed rows file is empty: {file_context.f.filename}")
            return True

ilename}")
            return True

        # CONSISTENT: Auto-        # CONSISTENT: Auto-remove metadata, justremove metadata, just like duplicates like duplicates/conf/conflicts!
        df =licts!
        df = self._drop_metadata_columns self._drop_metadata_columns(df)
        
        # Process(df)
        
        # Process as regular file
 as regular file
        return        return self._process_dataframe(df, file self._process_dataframe(df, file_context)

    # ---------------------------
   _context)

    # ---------------------------
    # Utility methods - ENH # Utility methods - ENHANCANCED: Better file movement
    # ---------------------------
   ED: Better file movement
    # ---------------------------
    def def _move_to_processed(self _move_to_processed(self, filepath: Path,, filepath: Path, category: str):
        """ category: str):
        """Move file to processed directory with consistent naming."""
       Move file to processed directory with consistent naming."""
        processed_dir processed_dir = Path(f = Path(f"{category}/processed"{category}/processed")
        processed_dir.mkdir(exist")
        processed_dir.mkdir(exist_ok=True)
_ok=True)
        
        try        
        try:
            #:
            # Add timestamp to avoid name conflicts
            timestamp = Add timestamp to avoid name conflicts
            timestamp = datetime.now datetime.now().strftime("%().strftime("%Y%mY%m%d_%H%M%%d_%H%M%SS")
")
            new_name            new_name = f"{filepath.stem}_{timestamp}{filepath.suffix}"
            new_path = = f"{filepath.stem}_{timestamp}{filepath.suffix}"
 processed_dir / new_name
            
            new_path = processed_dir / new_name
            
            shutil.move(str(filepath), str(new            shutil.move(str(filepath), str(new_path))
            logger_path))
            logger.info(f.info(f"Moved {category"Moved {category} file to processed: {new_path}")
} file to processed: {new_path}")
        except Exception as e:
            logger.warning(f        except Exception as e:
            logger.warning(f"Could not move file to"Could not move file to processed directory: {e} processed directory: {e}")

#")

# ===========================
# Utilities: create sample configs ===========================
# Utilities: create sample configs & files
# ========================= & files
# ===========================
def create_sample_configs==
def create_sample_configs():
    """Create sample configuration():
    """Create sample configuration files with organized directory structure."""
 files with organized directory structure."""
    # Create organized directory structure    # Create organized directory structure
    os.makedirs("rules
    os.makedirs("", exist_ok=True)
rules", exist_ok=True)
    os.makedirs("    os.makedirs("inputs/sales_datainputs/sales_data", exist_ok=True)
", exist_ok=True)
    os.makedirs("inputs/in    os.makedirs("inputs/inventory_data", exist_ok=Trueventory_data", exist_ok)
    os.makedirs("=True)
    os.makedirs("inputs/weekly_reportsinputs/weekly_reports", exist_ok=True)
   ", exist_ok=True)
    os.makedirs(DU os.makedirs(DUPLICATES_ROOT_DIR,PLICATES_ROOT_DIR, exist_ok=True)
    exist_ok=True)
    os.makedirs(DUPLICATES_TO_PROCESS_DIR, exist_ok=True)
 os.makedirs(DUPLICATES_TO_PROCESS_DIR, exist_ok=True)
    os    os.makedirs(.makedirs(DUPLDUPLICATES_PROCICATES_PROCESSED_DIR, exist_ok=True)
   ESSED_DIR, exist_ok=True)
    os.makedirs(F os.makedirs(FORMORMAT_CONFLICT_DIR, existAT_CONFLICT_DIR, exist_ok=True)
   _ok=True)
    os.m os.makedirs(FORMakedirs(FORMAT_CONFLICT_TO_PROCESSAT_CONFLICT_TO_PROCESS_DIR, exist__DIR, exist_ok=Trueok=True)
    os)
    os.makedirs(FORMAT_CONFL.makedirs(FORMAT_CONFLICT_PROCESSED_DIR, exist_ok=True)
    
    # Failed rows directories
    osICT_PROCESSED_DIR, exist_ok=True)
    
    # Failed rows directories
    os.makedirs.makedirs(FAIL(FAILED_ROWSED_ROWS_DIR, exist_ok=True)
    os_DIR, exist_ok=True)
    os.makedirs(FAILED_RO.makedirs(FAILED_ROWS_TOWS_TO_PROCESS_DIR, exist_ok=True)
    os.makedirs(FAILED_PROCESS_DIR, exist_ok=True)
    os.makedirs(FAILED_ROWS_PROC_ROWS_PROCESSEDESSED_DIR, exist_ok_DIR, exist_ok=True)

    # Log directory
   =True)

    # Log directory
    os.makedirs os.makedirs(LOG(LOG_DIR, exist_ok_DIR, exist_ok=True)

    if not os.path.exists=True)

    if not os.path.exists(GLOBAL_CONFIG_FILE):
(GLOBAL_CONFIG_FILE):
               global_config = {
            " global_config = {
            "dbname": "your_databasedbname": "your_database_name",
            "user_name",
            "user": "your_username",
": "your_username",
            "password": "your            "password": "your_password",
_password",
            "host": "            "host": "localhost",
            "port":localhost",
            "port": 5432,
            "batch 5432,
            "batch_size": 100_size": 1000,
            "max_connections": 0,
            "max_connections": 5,
            "min_connections5,
            "min_connections": 1,
            "": 1,
            "retry_attempts": 3,
            "enable_proretry_attempts": 3,
            "enable_progressgress_tracking": True,
            "enable_data_validation":_tracking": True,
            "enable_data_validation": True,
            "timestamp_t True,
            "timestamp_tolerance_seconds": 1olerance_seconds": 1.0,
            "global.0,
            "global_hash_exclude_columns": [],
           _hash_exclude_columns": [],
            "lock_timeout": "lock_timeout":  3600,
           3600,
            "auto_add_columns": True,
 "auto_add_columns": True,
            "delete_files            "delete_files": "N",
": "N",
            "skip_empty            "skip_empty_sheets":_sheets": True,
            "warn_on True,
            "warn_on_empty_sheets": True,
            "_empty_sheets": True,
            "treat_empty_as_error": False,
            
            #treat_empty_as_error": False,
            
            # Enhanced insertion settings Enhanced insertion settings
            "enable
            "enable_row_level_row_level_recovery": True,
            "fail_on_part_recovery": True,
            "fail_on_partial_insert":ial_insert": False,
            "retry_on_deadlock False,
            "retry_on_deadlock": True,
": True,
            "max_            "max_retry_delay":retry_delay": 30,
            "enable_b 30,
            "enable_batch_validation": True,
            "chatch_validation": True,
            "chunk_size": 100,
            "maxunk_size": 100,
            "max_chunk_fail_chunk_failures": 5ures": 5
        }
        with open(GLOBAL
        }
        with open(GLOBAL_CONFIG_FILE, 'w_CONFIG_FILE, 'w') as f:
            yaml.dump') as f:
            yaml.dump(global_config(global_config, f)
, f)
        logger.info        logger.info(f"Created(f"Created sample global config: {GLOBAL_CONFIG_FILE}")
 sample global config: {GLOBAL_CONFIG_FILE}")
    else    else:
        logger.info(f:
        logger.info(f"Global config already exists: {GLOBAL"Global config already exists: {GLOBAL_CONFIG_FILE}")

_CONFIG_FILE}")

    rules =    rules = {
        "sales_rule.yaml": {
 {
        "sales_rule.yaml": {
            "base_name            "base_name": "": "sales",
            "directory":sales",
            "directory": "inputs/sales_data",
 "inputs/sales_data",
            "file_pattern": r            "file_pattern": r"sales_\d{8"sales_\d{8}\.csv",
            "}\.csv",
            "date_format": "%Y%m%ddate_format": "%Y%m%d",
            "start_row":",
            "start_row": 0,
            "start 0,
            "start_col_col": 0,
           ": 0,
            "mode": "insert",
            " "mode": "insert",
           date_from_filename_col_name": "date_from_filename_col_name": "file_date",
            "file_date",
            "hash_exclude_columns": "hash_exclude_columns": [],
            "search_subdirect [],
            "search_subdirectories": True,
ories": True,
            "            "mapping_file": "rulesmapping_file": "rules/sales_mapping.csv"
/sales_mapping.csv"
        },
        "inventory_        },
        "inventory_rule.yaml": {
           rule.yaml": {
            " "base_name": "inventory",
base_name": "inventory",
            "directory": "            "directory": "inputs/inventory_data",
            "inputs/inventory_data",
            "file_pattern": r"inventory_\file_pattern": r"inventory_\d{8}\.xlsx",
            "date_format": "%d{8}\.xlsx",
            "date_format": "%Y%m%dY%m%d",
           ",
            "start_row":  "start_row": 0,
            "start_col":0,
            "start_col": 0,
            "mode 0,
            "mode": "insert",
            "": "insert",
            "date_from_filename_col_name":date_from_filename_col_name": "file_date",
            " "file_date",
            "hashhash_exclude_columns":_exclude_columns": [],
            [],
            "search_subdirectories": True,
            "sheet_config "search_subdirectories": True,
            "sheet_config": {
                "processing_method": "multiple",
                "sheet_names": ["": {
                "processing_method": "multiple",
                "sheet_names": ["Sheet1", "Sheet2"]
            },
            "Sheet1", "Sheet2"]
            },
            "mapping_file": "rules/inventory_mapping.csv"
        },
        "weeklymapping_file": "rules/inventory_mapping.csv"
        },
        "_rule.yaml": {
           weekly_rule.yaml": {
            "base_name": "weekly "base_name": "weekly_reports",
            "directory_reports",
            "directory": "inputs/weekly_re": "inputs/weekly_reports",
            "file_pattern":ports",
            "file_pattern": r"weekly_\d{8 r"weekly_\d{8}\.xlsx",
           }\.xlsx",
            "date_format": "%Y "date_format": "%Y%m%d",
            "start%m%d",
            "start_row": 0,
_row": 0,
            "start_col": 0,
            "start_col": 0,
            "mode": "insert            "mode": "insert",
            "date_from",
            "date_from_filename_col_name_filename_col_name": "report_date",
            "hash_exclude_": "report_date",
            "hash_exclude_columns": [],
            "search_subdirectories": Truecolumns": [],
            "search_subdirectories": True,
            "sheet_config": {
               ,
            "sheet_config": {
                "processing_method": "all"
            },
            "mapping_file": "rules/weekly_mapping.csv"
        "processing_method": "all"
            },
            "mapping_file": "rules/weekly_mapping.csv"
        }
    }

    for rule }
    }

    for rule_file, rule_data in rules.items():
        rule_file, rule_data in rules.items():
        rule_path = Path("_path = Path("rules") / rule_file
rules") / rule_file
        if        if not rule_path.exists():
            with open(rule_path, not rule_path.exists():
            with open(rule_path, ' 'w') as f:
w') as f:
                yaml.dump(rule_data                yaml.dump(rule_data, f)
            logger.info, f)
            logger.info(f(f"Created sample rule file: {"Created sample rule file: {rule_path}")
        else:
            loggerrule_path}")
        else:
            logger.info(f"Rule.info(f"Rule file already file already exists: {rule_path} exists: {rule_path}")

    today = datetime.today()
")

    today = datetime.today()
    dates =    dates = [today - pd.Timedelta(d [today - pd.Timedelta(days=7 * iays=7 * i) for) for i in range( i in range(4)]
    for dt in dates:
        date4)]
    for dt in dates:
        date_str = dt.str_str = dt.strftime("%ftime("%Y%m%dY%m%d")
        
        # Sales")
        
        # Sales data data ( (CSV)
        sample_sCSV)
        sample_sales_file =ales_file = Path("inputs/sales_data") / f"sales_{date_str}.csv"
        if not sample Path("inputs/sales_data") / f"sales_{date_str}.csv"
        if not sample_sales_file_sales_file.exists():
            pd.DataFrame.exists():
            pd.DataFrame({
                "OrderID": [1, 2, 3],
({
                "OrderID": [1, 2, 3],
                "Customer": ["                "Customer": ["Alice", "Bob", "Charlie"],
                "Amount":Alice", "Bob", "Charlie"],
                "Amount": [120.5, [120.5, 85 85.0, .0, 42.75]
            }).to_csv42.75]
            }).to_csv(sample_sales(sample_sales_file,_file, index=False)
            logger.info index=False)
            logger.info(f"Created sample sales file:(f"Created sample sales file: {sample_sales_file {sample_sales_file}")

        # Inventory data (Excel}")

        # Inventory data (Excel with multiple sheets)
        sample with multiple sheets)
        sample_inventory_file = Path("_inventory_file = Path("inputs/inventory_data") /inputs/inventory_data") / f"inventory_{date_str}.xlsx"
        f"inventory_{date_str}.xlsx"
        if not if not sample_inventory_file.exists sample_inventory_file.exists():
            with pd.ExcelWriter():
            with pd.ExcelWriter(sample_inventory_file,(sample_inventory_file, engine='openpyxl') engine='openpyxl') as writer:
                pd.DataFrame({
                    as writer:
                pd.DataFrame({
                    "ItemID": "ItemID": [101, 102, 103 [101, 102, 103],
                    "ItemName],
                    "ItemName":": ["Widget", " ["Widget", "Gadget", "Thingy"],
                   Gadget", "Thingy "Stock": ["],
                    "Stock": [50, 20, 75]
                }).50, 20, 75]
                }).to_excel(wto_excel(writer,riter, sheet_name='Sheet sheet_name='Sheet1', index=False1', index=False)
                pd.DataFrame({
                    "ItemID":)
                pd.DataFrame({
                    "ItemID": [201,  [201, 202, 203202, 203],
                    "ItemName": ["Product A",],
                    "ItemName": ["Product A", "Product "Product B", B", "Product C"],
                    "Stock": [30 "Product C"],
                    "Stock": [30, 40,, 40, 60 60]
                }).to_excel(writer, sheet]
                }).to_excel(writer, sheet_name='Sheet2_name='Sheet2', index', index=False)
            logger=False)
            logger.info(f"Created sample inventory file with multiple.info(f"Created sample inventory file with multiple sheets: { sheets: {sample_inventory_file}")

        # Weekly reportssample_inventory_file}")

        # Weekly reports ( (ExcelExcel with with multiple sheets)
        sample_weekly_file = Path("inputs multiple sheets)
        sample_weekly/weekly_reports") / f"weekly_file = Path("inputs/weekly_reports") / f"weekly_{date_str}.xlsx"
        if not_{date_str}.xlsx"
        if not sample_ sample_weekly_file.exists():
           weekly_file.exists():
            with pd.ExcelWriter(sample_weekly with pd.ExcelWriter(sample_weekly_file, engine='_file, engine='openpyopenpyxl') as writerxl') as writer:
                pd.DataFrame({
                    "Week:
                pd.DataFrame({
                    "WeekStart": [dt.strStart": [dt.strftime("%Y-%m-%d")ftime("%Y-%m-%d")],
                    "TotalSales],
                    "TotalSales":": [round(np.random.un [round(np.random.uniform(1000, iform(1000, 2000), 2)],
                   2000), 2)],
                    "TotalOrders": [int "TotalOrders": [int(np.random.randint(20,(np.random.randint(20, 50))]
                }).to 50))]
                }).to_excel(writer, sheet_name_excel(writer, sheet_name='Summary', index=False='Summary', index=False)
               )
                pd.DataFrame({
                    "Department pd.DataFrame({
                    "Department": ["Sales", "Marketing": ["Sales", "Marketing", "Operations"],
                    "", "Operations"],
                    "WeeklyTotalWeeklyTotal": [500, 300, 400": [500, 300, 400]
]
                }).to_excel(writer,                }).to_excel(writer, sheet_name='Departments', index sheet_name='Departments', index=False=False)
            logger.info(f"Created sample)
            logger.info(f"Created sample weekly report file with multiple sheets: {sample_ weekly report file with multiple sheets: {sample_weekly_file}")

# ===========================
#weekly_file}")

# ===========================
# Main entrypoint
# =========================== Main entrypoint
# ===========================
if __name__ ==
if __name__ == "__main__":
    # Remove stale lock if "__main__":
    # Remove stale lock if older than older than lock_timeout ( lock_timeout (conservative)
    if os.path.exists(LOCK_FILE):
conservative)
    if os.path.exists(LOCK_FILE):
        try        try:
           :
            lock_age = time.time() - os.path.get lock_age = time.time() - os.path.getmtime(LOCKmtime(LOCK_FILE)
_FILE)
            if lock_age >            if lock_age > 3600:
                os.remove(LOCK 3600:
                os.remove(LOCK_FILE)
                print_FILE)
                print("Removed stale lock file")
       ("Removed stale lock file except Exception:
            pass

")
        except Exception:
            pass

    if len(sys.argv) > 1 and sys    if len(sys.argv).argv[ > 1 and sys.argv[11] == "--extract-p] == "--extract-pattern":
        ifattern":
        if len(sys.argv) < 3 len(sys.argv) < 3:
            print("Usage: python script.py --extract-pattern:
            print("Usage: python script.py --extract-pattern <filename> <filename>")
            sys.exit(1)

        filename =")
            sys.exit(1)

        filename = sys.argv[2 sys.argv[2]
        pattern, date_format = PostgresLoader.extract]
        pattern, date_format = PostgresLoader.extract_pattern_and_pattern_and_date_format(filename)

_date_format(filename)

        if pattern and date_format:
            print(f        if pattern and date_format:
            print(f"Filename: {"Filename: {filename}")
            print(f"Suggestedfilename}")
            print(f"Suggested pattern: {pattern}")
 pattern: {pattern}")
            print            print(f"Suggested date(f"Suggested date format: format: {date_format}")
            is_valid = PostgresLoader.test {date_format}")
            is_valid = PostgresLoader.test_pattern_on_pattern_on_filename(pattern, filename, date_format)
            print_filename(pattern, filename, date_format)
            print(f"(f"Pattern validation: {'Pattern validation: {'✓ PASS✓ PASS' if is_valid else '✗ FAIL' if is_valid else '✗ FAIL'}")
       '}")
        else:
            print else:
            print(f"Could not extract pattern from:(f"Could not extract pattern from: {filename}")
        sys.exit {filename}")
        sys.exit(0(0)

    create_sample_configs()

    try:
       )

    create_sample_configs()

    try:
        loader = PostgresLoader loader = PostgresLoader(
            global_config_file=GLOBAL(
            global_config_file=GLOBAL_CONFIG_FILE,
            rules_CONFIG_FILE,
            rules_folder_path='rules'
        )

_folder_path='rules'
        )

        result = loader.process_files        result = loader.process_files()
        print(f"Processed:()
        print(f"Processed: {result['processed {result['processed']}, Failed: {result']}, Failed: {result['failed']}")
    except Exception['failed']}")
    except Exception as e:
        print(f" as e:
        print(f"Processing failed: {e}")
        print("Check processing.log for details")