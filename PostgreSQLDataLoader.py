#!/usr/bin/env python3
"""
PostgreSQL Data Loader - Enhanced with Failed Rows Recovery & Precise OS Error Logging
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
import signal
import errno
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
# Configure logging - ENHANCED with OS error details
# ===========================
def setup_logging():
    """Setup logging with timestamped log files in logs directory with enhanced OS error details."""
    global LOG_DIR
    
    # Create logs directory if it doesn't exist
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        print(f"Log directory created/verified: {LOG_DIR}")
    except OSError as e:
        print(f"CRITICAL: Failed to create log directory {LOG_DIR}: {e}")
        # Fallback to current directory
        LOG_DIR = "."
        print(f"Using fallback log directory: {LOG_DIR}")
    
    # Generate log filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"processing_{timestamp}.log"
    log_filepath = os.path.join(LOG_DIR, log_filename)
    
    try:
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
            handlers=[
                logging.FileHandler(log_filepath, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        
        # Also create a symlink to the latest log for easy access
        latest_log_path = os.path.join(LOG_DIR, "processing_latest.log")
        try:
            if os.path.exists(latest_log_path):
                os.remove(latest_log_path)
            os.symlink(log_filepath, latest_log_path)
            print(f"Created latest log symlink: {latest_log_path} -> {log_filepath}")
        except OSError as e:
            print(f"Could not create latest log symlink: {e} (errno: {e.errno})")
            
    except Exception as e:
        print(f"CRITICAL: Logging setup failed: {e}")
        # Basic fallback logging
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    
    # Get logger after setup
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized. Log file: {log_filepath}")
    return logger

# Initialize logging
logger = setup_logging()

# Enhanced OS error logging decorator
def log_os_operations(func):
    """Decorator to add detailed OS error logging for file/directory operations."""
    def wrapper(*args, **kwargs):
        func_name = func.__name__
        try:
            result = func(*args, **kwargs)
            logger.debug(f"OS operation successful: {func_name}")
            return result
        except OSError as e:
            error_details = {
                'function': func_name,
                'error': str(e),
                'errno': e.errno,
                'strerror': os.strerror(e.errno) if e.errno else 'N/A',
                'filename': getattr(e, 'filename', 'N/A'),
                'filename2': getattr(e, 'filename2', 'N/A'),
                'args': str(args)[:200] + '...' if len(str(args)) > 200 else str(args),
                'kwargs': {k: '***' if 'password' in k.lower() else v for k, v in kwargs.items()}
            }
            logger.error(f"OS operation failed: {error_details}")
            # Handle specific OS errors gracefully
            if e.errno == errno.EINTR:
                logger.warning("Operation interrupted by signal, retrying...")
                return func(*args, **kwargs)
            raise
        except Exception as e:
            logger.error(f"Unexpected error in {func_name}: {type(e).__name__}: {e}")
            raise
    return wrapper

# ===========================
# Enhanced Lock Management with Signal Handling
# ===========================
class LockManager:
    """Enhanced lock management with signal handling and robust cleanup."""
    
    def __init__(self, lock_file: str = LOCK_FILE, timeout: int = 3600):
        self.lock_file = lock_file
        self.timeout = timeout
        self.lock_acquired = False
        self.original_signal_handlers = {}
        
    def setup_signal_handlers(self):
        """Setup signal handlers for graceful interruption."""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, performing graceful shutdown...")
            self.cleanup()
            sys.exit(1)
            
        signals = [signal.SIGINT, signal.SIGTERM]
        for sig in signals:
            self.original_signal_handlers[sig] = signal.signal(sig, signal_handler)
        logger.debug("Signal handlers installed for graceful shutdown")
    
    def restore_signal_handlers(self):
        """Restore original signal handlers."""
        for sig, handler in self.original_signal_handlers.items():
            if handler is not None:
                signal.signal(sig, handler)
        logger.debug("Original signal handlers restored")
    
    @log_os_operations
    def acquire_lock(self):
        """Acquire lock with enhanced error handling and stale lock detection."""
        max_attempts = 5
        attempt = 0
        
        logger.info(f"Attempting to acquire lock (timeout: {self.timeout}s, max_attempts: {max_attempts})")
        
        while attempt < max_attempts:
            if os.path.exists(self.lock_file):
                try:
                    lock_time = os.path.getmtime(self.lock_file)
                    current_time = time.time()
                    lock_age = current_time - lock_time
                    
                    logger.debug(f"Lock file exists: {self.lock_file}, age: {lock_age:.1f}s, timeout: {self.timeout}s")
                    
                    # Check if lock is stale (older than timeout)
                    if lock_age > self.timeout:
                        logger.warning(f"Stale lock file detected (age: {lock_age:.1f}s > timeout: {self.timeout}s). Removing.")
                        self._safe_remove_lock()
                        continue
                    
                    # Check if the process that created the lock is still running
                    try:
                        with open(self.lock_file, 'r') as f:
                            pid_str = f.read().strip()
                            if pid_str.isdigit():
                                pid = int(pid_str)
                                # Check if process exists
                                try:
                                    os.kill(pid, 0)  # This will raise OSError if process doesn't exist
                                    # Process is still running
                                    if attempt == max_attempts - 1:
                                        logger.error(f"Another instance is running (PID: {pid}) and lock is still valid. Exiting.")
                                        return False
                                    else:
                                        logger.warning(f"Another instance is running (PID: {pid}). Waiting... (Attempt {attempt + 1}/{max_attempts})")
                                        time.sleep(2)
                                        attempt += 1
                                        continue
                                except OSError as e:
                                    if e.errno == errno.ESRCH:  # No such process
                                        logger.warning(f"Stale lock file detected (process {pid} not running). Removing.")
                                        self._safe_remove_lock()
                                    else:
                                        logger.warning(f"Error checking process {pid}: {e}. Removing stale lock.")
                                        self._safe_remove_lock()
                        except (IOError, ValueError) as e:
                            # Lock file is corrupt or empty
                            logger.warning(f"Corrupt lock file detected: {e}. Removing.")
                            self._safe_remove_lock()
                    except OSError as e:
                        logger.warning(f"Error checking lock file {self.lock_file}: {e}. Removing stale lock.")
                        self._safe_remove_lock()
                except OSError as e:
                    logger.warning(f"Error accessing lock file {self.lock_file}: {e}. Removing stale lock.")
                    self._safe_remove_lock()

            # Try to create lock file
            try:
                with open(self.lock_file, 'w') as f:
                    f.write(str(os.getpid()))
                # Verify the lock was created successfully
                if os.path.exists(self.lock_file):
                    with open(self.lock_file, 'r') as f:
                        written_pid = f.read().strip()
                    if written_pid == str(os.getpid()):
                        self.lock_acquired = True
                        logger.info(f"Lock acquired successfully for PID: {os.getpid()}")
                        return True
                    else:
                        logger.warning(f"Lock file verification failed. Expected PID {os.getpid()}, got {written_pid}")
                else:
                    logger.warning("Lock file was not created successfully")
                    
            except (IOError, OSError) as e:
                if attempt == max_attempts - 1:
                    logger.error(f"Failed to acquire lock after {max_attempts} attempts: {e}")
                    return False
                else:
                    logger.warning(f"Could not acquire lock (attempt {attempt + 1}/{max_attempts}): {e}")
                    time.sleep(1)
                    attempt += 1

        logger.error("Failed to acquire lock after maximum attempts")
        return False

    def _safe_remove_lock(self):
        """Safely remove lock file with enhanced error handling."""
        try:
            if os.path.exists(self.lock_file):
                os.remove(self.lock_file)
                logger.info("Stale lock file removed successfully")
        except OSError as e:
            logger.error(f"Failed to remove stale lock file {self.lock_file}: {e}")

    @log_os_operations
    def release_lock(self):
        """Release lock with enhanced safety checks."""
        if not self.lock_acquired:
            logger.debug("No lock to release")
            return
            
        try:
            if os.path.exists(self.lock_file):
                # Verify we own the lock before removing it
                try:
                    with open(self.lock_file, 'r') as f:
                        lock_pid = f.read().strip()
                    if lock_pid == str(os.getpid()):
                        os.remove(self.lock_file)
                        self.lock_acquired = False
                        logger.info("Lock released successfully")
                        self.restore_signal_handlers()
                    else:
                        logger.warning(f"Lock file owned by different process (PID: {lock_pid}), not removing")
                except (IOError, ValueError) as e:
                    logger.warning(f"Could not read lock file: {e}, removing anyway")
                    os.remove(self.lock_file)
                    self.lock_acquired = False
            else:
                logger.warning("Lock file does not exist, nothing to release")
                self.lock_acquired = False
                
        except Exception as e:
            logger.error(f"Error releasing lock: {type(e).__name__}: {e}")
            self.lock_acquired = False

    def cleanup(self):
        """Comprehensive cleanup."""
        self.release_lock()
        self.restore_signal_handlers()

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

    # NEW: Sample file control
    generate_sample_files: bool = False

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
    
    # NEW: Skip configuration options
    skip_subdirectories: List[str] = field(default_factory=list)  # Directories to skip entirely
    skip_file_patterns: List[str] = field(default_factory=list)   # File patterns to skip
    
    _compiled_pattern: Any = field(init=False, repr=False)

    def __post_init__(self):
        try:
            self._compiled_pattern = re.compile(self.file_pattern)
        except re.error as e:
            logger.error(f"Invalid regex pattern '{self.file_pattern}' for rule {self.base_name}: {e}")
            raise
        # Set default mapping file path if not provided
        if self.mapping_file is None:
            self.mapping_file = f"rules/{self.base_name}_mapping.csv"

    def match(self, filename: str) -> Optional[re.Match]:
        """Check if filename matches the pattern with enhanced logging."""
        match = self._compiled_pattern.match(filename)
        if match:
            logger.debug(f"File '{filename}' MATCHED pattern '{self.file_pattern}' for rule '{self.base_name}'")
        else:
            logger.debug(f"File '{filename}' DID NOT MATCH pattern '{self.file_pattern}' for rule '{self.base_name}'")
        return match

    @property
    def target_table(self) -> str:
        return self.base_name

    def validate(self) -> Tuple[bool, List[str]]:
        errors = []
        if not self.directory:
            errors.append("Directory is required")
        if not self.file_pattern:
            errors.append("File pattern is required")
        
        # Validate regex pattern
        try:
            re.compile(self.file_pattern)
        except re.error as e:
            errors.append(f"Invalid regex pattern '{self.file_pattern}': {e}")
            
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
        
        # Validate skip patterns
        for pattern in self.skip_file_patterns:
            try:
                re.compile(pattern)
            except re.error as e:
                errors.append(f"Invalid skip pattern '{pattern}': {e}")
        
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
# Enhanced Database Manager with OS logging
# ===========================
class DatabaseManager:
    """Handles database connections and operations with automatic table creation."""

    def __init__(self, db_config: Dict[str, Any], config: ProcessingConfig):
        self.db_config = db_config
        self.config = config
        self.connection_pool = None
        self._initialize_pool()

    @log_os_operations
    def _initialize_pool(self) -> None:
        try:
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                self.config.min_connections,
                self.config.max_connections,
                **self.db_config
            )
            logger.info(f"Database connection pool initialized: {self.config.min_connections}-{self.config.max_connections} connections")
        except psycopg2.Error as e:
            logger.critical(f"Failed to initialize connection pool: {e} (pgcode: {getattr(e, 'pgcode', 'N/A')})")
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
            logger.error(f"Database operation failed: {e} (pgcode: {getattr(e, 'pgcode', 'N/A')})")
            raise
        finally:
            if conn:
                self.connection_pool.putconn(conn)

    def test_connection(self) -> bool:
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    logger.info("Database connection test successful")
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
                    exists = cursor.fetchone()[0]
                    logger.debug(f"Table existence check for '{table_name}': {exists}")
                    return exists
        except Exception as e:
            logger.error(f"Error checking table existence for '{table_name}': {e}")
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
                    result = cursor.fetchone()[0]
                    logger.debug(f"Latest timestamp for {source_filename} in {target_table}: {result}")
                    return result
        except psycopg2.errors.UndefinedTable:
            logger.debug(f"Table {target_table} does not exist yet")
            return None
        except Exception as e:
            logger.error(f"Error getting latest timestamp for {source_filename}: {e}")
            raise

    def delete_by_source_filename(self, conn, target_table: str, source_filename: str) -> int:
        try:
            with conn.cursor() as cursor:
                query = sql.SQL("DELETE FROM {} WHERE source_filename = %s").format(sql.Identifier(target_table))
                cursor.execute(query, (source_filename,))
                deleted_count = cursor.rowcount
                logger.info(f"Deleted {deleted_count} records from {target_table} for file {source_filename}")
                return deleted_count
        except psycopg2.errors.UndefinedTable:
            logger.debug(f"Table {target_table} does not exist, nothing to delete")
            return 0
        except Exception as e:
            logger.error(f"Error deleting records from {target_table}: {e}")
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
                    exists = cursor.fetchone() is not None
                    logger.debug(f"File existence check for {source_filename}: {exists}")
                    return exists
        except psycopg2.errors.UndefinedTable:
            logger.debug(f"Table {target_table} does not exist")
            return False
        except Exception as e:
            logger.error(f"File existence check failed for {source_filename}: {e}")
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
                    hashes = {row[0] for row in cursor.fetchall()}
                    logger.debug(f"Retrieved {len(hashes)} existing hashes for {source_filename}")
                    return hashes
        except psycopg2.errors.UndefinedTable:
            logger.debug(f"Table {target_table} does not exist")
            return set()
        except Exception as e:
            logger.error(f"Failed to get existing hashes for {source_filename}: {e}")
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
                    exists = cursor.fetchone() is not None
                    logger.debug(f"Column existence check for {column_name} in {table_name}: {exists}")
                    return exists
        except Exception as e:
            logger.error(f"Error checking column existence for {column_name} in {table_name}: {e}")
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
                    logger.info(f"Successfully added {len(new_columns)} columns to table {table_name}")
                    return True
        except Exception as e:
            logger.error(f"Failed to alter table {table_name}: {e}")
            return False

    def close(self):
        """Close the database connection pool."""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("Database connection pool closed")

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

    @log_os_operations
    def _load_progress(self) -> dict:
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    data = json.load(f)
                    logger.info(f"Loaded progress data from {self.progress_file}: {len(data)} files tracked")
                    return data
            except Exception as e:
                logger.warning(f"Could not load progress file {self.progress_file}: {e}")
        else:
            logger.info(f"Progress file {self.progress_file} does not exist, starting fresh")
        return {}

    @log_os_operations
    def save_progress(self) -> None:
        try:
            with open(self.progress_file, 'w') as f:
                json.dump(self.processed_files, f, indent=2)
            logger.debug(f"Progress saved to {self.progress_file}: {len(self.processed_files)} files")
        except Exception as e:
            logger.error(f"Could not save progress to {self.progress_file}: {e}")

    def get_tracking_key(self, filepath: Path, file_context: FileContext) -> str:
        # Include sheet name in tracking key for multi-sheet Excel files
        sheet_suffix = f"::{file_context.source_sheet}" if file_context.source_sheet else ""
        key = f"{filepath}{sheet_suffix}||{file_context.target_table}"
        logger.debug(f"Generated tracking key: {key}")
        return key

    @log_os_operations
    def calculate_file_hash(self, filepath: Path) -> str:
        hasher = hashlib.sha256()
        try:
            with open(filepath, 'rb') as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    hasher.update(chunk)
            file_hash = hasher.hexdigest()
            logger.debug(f"Calculated file hash for {filepath}: {file_hash[:16]}...")
            return file_hash
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
        config_hash = hashlib.sha256(json.dumps(config_data, sort_keys=True).encode()).hexdigest()
        logger.debug(f"Calculated config hash: {config_hash[:16]}...")
        return config_hash

    def needs_processing(self, filepath: Path, file_context: FileContext) -> bool:
        if not filepath.exists():
            logger.warning(f"File does not exist: {filepath}")
            return False

        tracking_key = self.get_tracking_key(filepath, file_context)
        current_mod_time = datetime.fromtimestamp(filepath.stat().st_mtime)
        stored_info = self.processed_files.get(tracking_key)

        # New file - definitely process
        if not stored_info:
            logger.debug(f"New file detected: {filepath}")
            return True

        try:
            stored_dt = datetime.fromisoformat(stored_info['timestamp'])
            time_diff = abs((current_mod_time - stored_dt).total_seconds())

            if time_diff <= 1.0:
                current_config_hash = self._calculate_config_hash(file_context)
                stored_config_hash = stored_info.get('config_hash', '')

                if current_config_hash == stored_config_hash:
                    logger.debug(f"File unchanged (timestamp match): {filepath}")
                    return False
                else:
                    logger.info(f"Configuration changed for: {filepath}")
                    return True
        except Exception as e:
            logger.warning(f"Error checking timestamp for {filepath}: {e}")

        current_content_hash = self.calculate_file_hash(filepath)
        current_config_hash = self._calculate_config_hash(file_context)

        if not current_content_hash:
            logger.warning(f"Could not calculate hash for {filepath}, will process")
            return True

        stored_content_hash = stored_info.get('hash', '')
        stored_config_hash = stored_info.get('config_hash', '')

        if (current_content_hash == stored_content_hash and
                current_config_hash == stored_config_hash):
            self.mark_processed(filepath, file_context)
            logger.debug(f"File unchanged (hash match): {filepath}")
            return False

        logger.info(f"File changed: {filepath}")
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
        logger.debug(f"Marked file as processed: {filepath}")

# ===========================
# Enhanced File Processor with Multi-Sheet Excel Support & OS logging
# ===========================
class FileProcessor:
    """Handles file operations and data extraction with multi-sheet Excel support and enhanced OS logging."""

    def __init__(self, config: ProcessingConfig):
        self.config = config

    @log_os_operations
    def load_file(self, file_path: Path, start_row: int = 0, start_col: int = 0, 
                  sheet_config: Dict[str, Any] = None) -> pd.DataFrame:
        file_ext = file_path.suffix.lower()
        file_size = file_path.stat().st_size if file_path.exists() else 0
        
        logger.info(f"Loading file: {file_path} (size: {file_size} bytes, start_row: {start_row}, start_col: {start_col})")
        
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
            logger.error(f"Error loading file {file_path}: {type(e).__name__}: {e}")
            raise

    @log_os_operations
    def _load_csv(self, file_path: Path, start_row: int, start_col: int) -> pd.DataFrame:
        # FIXED: start_row IS the header row, so skip (start_row-1) rows
        skip_rows = max(0, start_row - 1) if start_row > 0 else 0
        logger.debug(f"Loading CSV: {file_path}, skiprows: {skip_rows} (to reach header row {start_row}), usecols from: {start_col}")
        
        df = pd.read_csv(file_path, skiprows=skip_rows, header=0)
        
        # Apply start_col (1-based to 0-based conversion)
        if start_col > 0:
            effective_start_col = start_col - 1
            df = df.iloc[:, effective_start_col:]
        
        logger.info(f"Loaded CSV: {len(df)} rows, {len(df.columns)} columns")
        return df

    @log_os_operations
    def _load_excel(self, file_path: Path, start_row: int, start_col: int, 
                    sheet_config: Dict[str, Any]) -> pd.DataFrame:
        """Load Excel file with enhanced empty sheet detection and detailed logging."""
        
        if not sheet_config:
            sheet_config = {}
            
        processing_method = sheet_config.get('processing_method', 'specific')
        
        # FIXED: start_row IS the header row, so skip (start_row-1) rows
        skip_rows = max(0, start_row - 1) if start_row > 0 else 0
        logger.info(f"Loading Excel with method: {processing_method}, skip_rows: {skip_rows} (to reach header row {start_row}), start_col: {start_col}")
        
        if processing_method == 'specific':
            sheet_name = sheet_config.get('specific_sheet', 'Sheet1')
            try:
                logger.debug(f"Loading specific sheet: {sheet_name}")
                df = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=skip_rows, header=0)
                
                # Apply start_col (1-based to 0-based conversion)
                if start_col > 0:
                    effective_start_col = start_col - 1
                    df = df.iloc[:, effective_start_col:]
                    
                if self._is_sheet_empty(df, 0):  # start_col already applied
                    if self.config.warn_on_empty_sheets:
                        logger.warning(f"Sheet '{sheet_name}' is empty or contains only headers")
                    return pd.DataFrame()
                df['_source_sheet'] = sheet_name
                logger.info(f"Successfully loaded sheet '{sheet_name}': {len(df)} rows, {len(df.columns)} columns")
                return df
            except ValueError as e:
                logger.error(f"Sheet '{sheet_name}' not found in {file_path}: {e}")
                raise
        
        # Multiple sheet processing
        dfs = []
        sheets_processed = 0
        
        if processing_method == 'multiple':
            sheet_names = sheet_config.get('sheet_names', [])
            logger.info(f"Processing multiple sheets: {sheet_names}")
        elif processing_method == 'all':
            all_sheets = pd.read_excel(file_path, sheet_name=None)
            sheet_names = list(all_sheets.keys()) if all_sheets else []
            logger.info(f"Processing all sheets: {sheet_names}")
        elif processing_method == 'pattern':
            pattern = sheet_config.get('sheet_name_pattern', '.*')
            all_sheets = pd.read_excel(file_path, sheet_name=None)
            sheet_names = [name for name in (all_sheets.keys() if all_sheets else []) if re.match(pattern, name)]
            logger.info(f"Processing pattern-matched sheets: {sheet_names} (pattern: {pattern})")
        else:
            sheet_names = []
            logger.warning(f"Unknown processing method: {processing_method}")
        
        for sheet_name in sheet_names:
            try:
                logger.debug(f"Processing sheet: {sheet_name}")
                df = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=skip_rows, header=0)
                if self._is_sheet_empty(df, 0):  # start_col will be applied later
                    if self.config.warn_on_empty_sheets:
                        logger.info(f"Skipping empty sheet: {sheet_name}")
                    continue
                    
                # Apply start_col (1-based to 0-based conversion)
                if start_col > 0:
                    effective_start_col = start_col - 1
                    df = df.iloc[:, effective_start_col:]
                    
                df['_source_sheet'] = sheet_name
                dfs.append(df)
                sheets_processed += 1
                logger.info(f"Successfully processed sheet '{sheet_name}': {len(df)} rows")
            except Exception as e:
                logger.warning(f"Error processing sheet '{sheet_name}': {type(e).__name__}: {e}")
        
        if sheets_processed == 0:
            logger.warning(f"No data found in any sheets for {file_path} (checked {len(sheet_names)} sheets)")
            return pd.DataFrame()
        
        result_df = pd.concat(dfs, ignore_index=True)
        logger.info(f"Combined {sheets_processed} sheets into {len(result_df)} total rows")
        return result_df

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

    @log_os_operations
    def get_excel_sheet_names(self, file_path: Path) -> List[str]:
        """Get all sheet names from an Excel file with enhanced error handling."""
        try:
            logger.debug(f"Reading Excel sheet names from: {file_path}")
            xl_file = pd.ExcelFile(file_path)
            sheet_names = xl_file.sheet_names
            logger.info(f"Found {len(sheet_names)} sheets in {file_path}: {sheet_names}")
            return sheet_names
        except Exception as e:
            logger.error(f"Error reading Excel file {file_path}: {type(e).__name__}: {e}")
            return []

    def calculate_row_hashes(self, df: pd.DataFrame, exclude_columns: Set[str], source_sheet: str = "") -> List[str]:
        """Calculate content hashes for each row with custom exclusions and sheet awareness."""
        hashes = []
        logger.debug(f"Calculating row hashes for {len(df)} rows, excluding {len(exclude_columns)} columns")
        
        for _, row in df.iterrows():
            exclude = HASH_EXCLUDE_COLS | set(exclude_columns)
            data = {k: v for k, v in row.items() if k not in exclude}
            
            # Include sheet name in hash calculation for multi-sheet files
            if source_sheet and '_source_sheet' not in data:
                data['_sheet_context'] = source_sheet
                
            hasher = hashlib.sha256()
            hasher.update(json.dumps(data, sort_keys=True, default=str).encode('utf-8'))
            hashes.append(hasher.hexdigest())
        
        logger.debug(f"Calculated {len(hashes)} row hashes")
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
# Enhanced Main Loader with OS error logging
# ===========================
class PostgresLoader:
    """Main loader class with organized directory structure and enhanced error handling."""

    def __init__(self, global_start_row: int = 0, global_start_col: int = 0,
                 delete_files: str = "N",
                 global_config_file: str = GLOBAL_CONFIG_FILE,
                 rules_folder_path: str = "rules"):

        logger.info(f"Initializing PostgresLoader with config: {global_config_file}, rules: {rules_folder_path}")
        
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
        self.run_id = datetime.now().isoformat()

        # Enhanced lock management
        self.lock_manager = LockManager(timeout=self.config.lock_timeout)
        
        # Create organized directory structure
        self._create_directory_structure()

        # Acquire lock to prevent concurrent runs with enhanced signal handling
        if not self._acquire_lock():
            sys.exit(1)

        self._validate_setup()

    @log_os_operations
    def _create_directory_structure(self):
        """Create the organized directory structure with detailed logging."""
        directories = [
            # Configuration directories
            "rules",
            
            # Input data directories
            "inputs/sales_data",
            "inputs/inventory_data", 
            "inputs/weekly_reports",
            
            # Processing directories
            DUPLICATES_ROOT_DIR,
            DUPLICATES_TO_PROCESS_DIR,
            DUPLICATES_PROCESSED_DIR,
            FORMAT_CONFLICT_DIR,
            FORMAT_CONFLICT_TO_PROCESS_DIR,
            FORMAT_CONFLICT_PROCESSED_DIR,
            
            # Failed rows directories
            FAILED_ROWS_DIR,
            FAILED_ROWS_TO_PROCESS_DIR,
            FAILED_ROWS_PROCESSED_DIR,

            # Log directory
            LOG_DIR
        ]

        for directory in directories:
            try:
                os.makedirs(directory, exist_ok=True)
                logger.debug(f"Directory created/verified: {directory}")
            except OSError as e:
                logger.error(f"Failed to create directory {directory}: {e} (errno: {e.errno})")
                raise

        logger.info("Directory structure created successfully")

    # ---------------------------
    # Enhanced Locking with signal handling
    # ---------------------------
    def _acquire_lock(self) -> bool:
        """Acquire lock with enhanced signal handling."""
        try:
            self.lock_manager.setup_signal_handlers()
            return self.lock_manager.acquire_lock()
        except Exception as e:
            logger.error(f"Failed to acquire lock: {e}")
            return False

    def _release_lock(self):
        """Release lock with cleanup."""
        self.lock_manager.cleanup()

    def cleanup(self):
        """Comprehensive cleanup method."""
        logger.info("Performing comprehensive cleanup...")
        try:
            self._release_lock()
            if hasattr(self, 'db_manager'):
                self.db_manager.close()
            logger.info("Cleanup completed successfully")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

    # ---------------------------
    # Configuration / Rules Loading
    # ---------------------------
    def _load_global_config(self, config_file: str) -> ProcessingConfig:
        logger.info(f"Loading global configuration from: {config_file}")
        if os.path.exists(config_file):
            try:
                with open(config_file, 'r') as f:
                    if config_file.endswith('.yaml') or config_file.endswith('.yml'):
                        config_data = yaml.safe_load(f)
                    else:
                        config_data = json.load(f)
                logger.info(f"Successfully loaded global configuration from {config_file}")
                return ProcessingConfig(**config_data)
            except Exception as e:
                logger.warning(f"Could not load global config {config_file}: {e}")

        # Return default config if file doesn't exist or can't be loaded
        logger.info("Using default configuration")
        return ProcessingConfig()

    def _load_processing_rules(self, rules_folder: str) -> List[FileProcessingRule]:
        rules_folder_path = Path(rules_folder)
        if not rules_folder_path.exists():
            logger.error(f"Rules folder not found: {rules_folder_path}")
            return []

        rules = []
        rule_files = list(rules_folder_path.iterdir())
        logger.info(f"Found {len(rule_files)} files in rules folder")
        
        for rule_file_path in rule_files:
            if rule_file_path.is_file() and rule_file_path.name.endswith("_rule.yaml"):
                try:
                    base_name = rule_file_path.stem.replace("_rule", "")
                    logger.debug(f"Loading rule from: {rule_file_path}")
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
                        mapping_file=mapping_file,
                        # NEW: Load skip configurations
                        skip_subdirectories=rule_data.get('skip_subdirectories', []),
                        skip_file_patterns=rule_data.get('skip_file_patterns', [])
                    )

                    is_valid, errors = rule.validate()
                    if not is_valid:
                        logger.error(f"Invalid rule {base_name}: {', '.join(errors)}")
                        continue

                    rules.append(rule)
                    logger.info(f"Loaded rule '{base_name}' with directory: {rule.directory}, pattern: '{rule.file_pattern}', skip_dirs: {rule.skip_subdirectories}, skip_patterns: {rule.skip_file_patterns}")
                except Exception as e:
                    logger.error(f"Failed to load rule from {rule_file_path}: {e}")

        logger.info(f"Successfully loaded {len(rules)} processing rules")
        return rules

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
    # Setup validation & mapping generation - FIXED VERSION
    # ---------------------------
    def _validate_setup(self) -> None:
        logger.info("Validating setup with organized directory structure...")
        
        valid_rules = []
        
        for rule in self.processing_rules:
            rule_source_dir = Path(rule.directory)
            if not rule_source_dir.exists():
                try:
                    rule_source_dir.mkdir(parents=True, exist_ok=True)
                    logger.info(f"Created directory: {rule_source_dir}")
                except OSError as e:
                    logger.error(f"Failed to create directory {rule_source_dir}: {e}")

            mapping_filepath = Path(rule.mapping_file)

            if not mapping_filepath.exists():
                logger.error(f"Mapping file not found for rule '{rule.base_name}': {mapping_filepath}")
                
                # Try to create mapping file if sample exists
                sample_file = next(rule_source_dir.glob("*.*"), None)
                if sample_file:
                    logger.warning(f"Creating template mapping file from sample: {sample_file}")
                    self._generate_mapping_file(
                        source_filepath=sample_file,
                        mapping_filepath=mapping_filepath,
                        start_row=rule.start_row or self.global_start_row,
                        start_col=rule.start_col or self.global_start_col,
                        sheet_config=rule.sheet_config
                    )
                    logger.error(f"Template mapping file created. Please configure LoadFlag values in {mapping_filepath} and rerun.")
                else:
                    logger.error(f"No sample file found for {rule.base_name} in {rule.directory} to generate mapping template")
                
                # Skip this rule and continue with others
                continue

            # If mapping file exists, validate it's properly configured
            try:
                mapping = pd.read_csv(mapping_filepath)
                
                # Check for unconfigured LoadFlags
                file_columns = mapping[mapping['data_source'] == 'file']
                unconfigured_columns = file_columns[
                    (file_columns['LoadFlag'].isna()) | 
                    (file_columns['LoadFlag'] == '') |
                    (file_columns['LoadFlag'].str.strip() == '')
                ]
                
                if not unconfigured_columns.empty:
                    unconfigured_names = unconfigured_columns['RawColumn'].tolist()
                    logger.error(
                        f"Rule '{rule.base_name}': Mapping file has unconfigured LoadFlags for columns: {unconfigured_names}. "
                        f"Please set LoadFlag to 'Y' or 'N' for these columns in {mapping_filepath} and rerun."
                    )
                    continue  # Skip this rule
                
                # If mapping is valid, create table and add to valid rules
                table_created = self.db_manager.create_table_if_not_exists(rule.target_table, mapping)
                if not table_created:
                    logger.error(f"Failed to ensure table exists: {rule.target_table}")
                    continue  # Skip this rule
                    
                valid_rules.append(rule)
                logger.info(f"Rule '{rule.base_name}' validated successfully")
                
            except Exception as e:
                logger.error(f"Error validating rule {rule.base_name}: {e}")
                continue  # Skip this rule

        # Update processing rules to only include valid ones
        self.processing_rules = valid_rules
        logger.info(f"Setup validation completed. {len(valid_rules)} rules are valid and ready for processing.")

        if not self.db_manager.test_connection():
            raise ConnectionError("Database connection test failed")

    @log_os_operations
    def _generate_mapping_file(self, source_filepath: Path, mapping_filepath: Path,
                              start_row: int = 0, start_col: int = 0, 
                              sheet_config: Dict[str, Any] = None):
        try:
            logger.info(f"Generating mapping file: {mapping_filepath} from sample: {source_filepath}")
            logger.info(f"Parameters - start_row: {start_row} (header row), start_col: {start_col}")
            
            # FIXED LOGIC: start_row IS the header row, so we skip (start_row - 1) rows to get to it
            skip_rows = max(0, start_row - 1) if start_row > 0 else 0
            logger.info(f"Calculated skip_rows: {skip_rows} (to reach header row {start_row})")
            
            # For Excel files with sheet config
            if source_filepath.suffix.lower() in ['.xlsx', '.xls'] and sheet_config:
                processing_method = sheet_config.get('processing_method', 'specific')
                if processing_method == 'specific':
                    sheet_name = sheet_config.get('specific_sheet', 'Sheet1')
                    try:
                        logger.debug(f"Reading Excel sheet '{sheet_name}' with skip_rows={skip_rows} to get header row {start_row}")
                        
                        # FIXED: Use skip_rows (start_row-1) to reach the header row, then use header=0
                        df_sample = pd.read_excel(source_filepath, sheet_name=sheet_name, 
                                                skiprows=skip_rows, nrows=100, header=0)
                        logger.info(f"Successfully read Excel file: {len(df_sample)} rows, {len(df_sample.columns)} columns")
                        
                        # Apply start_col AFTER reading
                        if not df_sample.empty and start_col > 0:
                            original_cols = len(df_sample.columns)
                            if start_col <= original_cols:
                                # FIXED: start_col is 1-based, so subtract 1 for 0-based indexing
                                effective_start_col = start_col - 1
                                df_sample = df_sample.iloc[:, effective_start_col:]
                                logger.info(f"Applied start_col={start_col}: {original_cols} -> {len(df_sample.columns)} columns")
                            else:
                                logger.warning(f"start_col={start_col} is > number of columns {original_cols}, ignoring")
                        
                    except Exception as e:
                        logger.error(f"Error reading specific sheet '{sheet_name}': {e}")
                        raise
                else:
                    # For multiple sheets, use the same logic
                    try:
                        df_sample = pd.read_excel(source_filepath, sheet_name=0, 
                                                skiprows=skip_rows, nrows=100, header=0)
                        # Apply start_col for multiple sheets too
                        if not df_sample.empty and start_col > 0:
                            original_cols = len(df_sample.columns)
                            if start_col <= original_cols:
                                effective_start_col = start_col - 1
                                df_sample = df_sample.iloc[:, effective_start_col:]
                                logger.info(f"Applied start_col={start_col}: {original_cols} -> {len(df_sample.columns)} columns")
                    except Exception as e:
                        logger.error(f"Error reading first sheet: {e}")
                        raise
            else:
                # Existing logic for other file types with the same fix
                if source_filepath.exists():
                    ext = source_filepath.suffix.lower()
                    try:
                        if ext == '.csv':
                            df_sample = pd.read_csv(source_filepath, skiprows=skip_rows, nrows=100, header=0)
                        elif ext in ['.xlsx', '.xls']:
                            df_sample = pd.read_excel(source_filepath, skiprows=skip_rows, nrows=100, header=0)
                        elif ext == '.parquet':
                            df_sample = pd.read_parquet(source_filepath).iloc[:100, :]
                        elif ext == '.json':
                            df_sample = pd.read_json(source_filepath).iloc[:100, :]
                        else:
                            df_sample = pd.DataFrame()
                        
                        # Apply start_col with same 1-based to 0-based conversion
                        if not df_sample.empty and start_col > 0:
                            original_cols = len(df_sample.columns)
                            if start_col <= original_cols:
                                effective_start_col = start_col - 1
                                df_sample = df_sample.iloc[:, effective_start_col:]
                                logger.info(f"Applied start_col={start_col}: {original_cols} -> {len(df_sample.columns)} columns")
                        
                        logger.info(f"Successfully read {ext} file: {len(df_sample)} rows, {len(df_sample.columns)} columns")
                    except Exception as e:
                        logger.error(f"Error reading file {source_filepath}: {e}")
                        raise
                else:
                    logger.error(f"Source file does not exist: {source_filepath}")
                    df_sample = pd.DataFrame()

            # Rest of the method remains the same...
            if df_sample.empty:
                logger.warning("No data found in sample file after applying start_row and start_col")
                # Create empty mapping file with correct structure
                pd.DataFrame(columns=[
                    'RawColumn', 'TargetColumn', 'DataType', 'LoadFlag', 'IndexColumn',
                    'data_source', 'definition', 'order'
                ]).to_csv(mapping_filepath, index=False)
                return

            columns = df_sample.columns.tolist()
            dtypes = df_sample.dtypes.apply(str).to_dict()

            sample_values = {}
            for col in columns:
                sample_values[col] = df_sample[col].iloc[0] if not df_sample.empty and not df_sample[col].empty else None

            mapping_data = []
            for i, col in enumerate(columns):
                pd_type = dtypes.get(col, 'object')
                sample_value = sample_values.get(col)
                sql_type = self._infer_postgres_type(pd_type, sample_value)
                load_flag = 'Y'  # Default to 'Y' for new columns
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
            logger.info(f"Created mapping file with {len(columns)} file columns: {mapping_filepath}")
            
        except Exception as e:
            logger.error(f"Mapping generation failed for {source_filepath}: {e}")
            # Create empty mapping file as fallback
            pd.DataFrame(columns=[
                'RawColumn', 'TargetColumn', 'DataType', 'LoadFlag', 'IndexColumn',
                'data_source', 'definition', 'order'
            ]).to_csv(mapping_filepath, index=False)

    # ---------------------------
    # New columns handling - FIXED VERSION
    # ---------------------------
    def _handle_new_columns(self, df: pd.DataFrame, file_context: FileContext) -> bool:
        """Detect new columns, update mapping file, and block processing until configured."""
        mapping = pd.read_csv(file_context.mapping_filepath)

        current_cols = df.columns.tolist()
        existing_cols = set(mapping[mapping['data_source'] == 'file']['RawColumn'])
        new_cols = [col for col in current_cols if col not in existing_cols]

        if not new_cols:
            return False

        logger.warning(f"New columns detected in {file_context.filename}: {', '.join(new_cols)}")

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
                'LoadFlag': '',  # Leave empty to force user configuration
                'IndexColumn': 'N',
                'data_source': 'file',
                'definition': f'NEW COLUMN DETECTED - Please set LoadFlag to Y or N',
                'order': col_positions[col]
            })

        updated_df = pd.DataFrame(updated_mapping)
        updated_df = updated_df.sort_values('order')

        updated_df.to_csv(file_context.mapping_filepath, index=False)
        logger.info(f"Updated mapping file with {len(new_cols)} new columns at correct positions")

        # Check if there are any unconfigured LoadFlags (including the new ones)
        unconfigured_new = updated_df[
            (updated_df['data_source'] == 'file') & 
            ((updated_df['LoadFlag'].isna()) | (updated_df['LoadFlag'] == ''))
        ]

        if not unconfigured_new.empty:
            new_col_names = unconfigured_new['RawColumn'].tolist()
            logger.error(
                f"New columns detected but not configured: {new_col_names}. "
                f"Please update {file_context.mapping_filepath} with LoadFlag values and rerun."
            )
            return True  # Block processing

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
            (mapping['LoadFlag'] == 'Y')
        ]

        logger.debug(f"Validating data types for {len(df)} rows in {filename}")

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

        if len(conflict_df) > 0:
            logger.warning(f"Found {len(conflict_df)} format conflicts in {filename}")

        return clean_df, conflict_df

    # ---------------------------
    # Export helpers - ENHANCED: Use same file format as original with OS logging
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
            conflict_df['_conflict_details'] = 'Unknown format conflict'

        conflict_df['_GUIDANCE'] = "Review data type inconsistencies. Fix the values to match the expected data types and place this file in the format_conflict/to_process folder for reprocessing."

        if '_business_key' not in conflict_df.columns:
            conflict_df['_business_key'] = ''

        if '_business_key' in conflict_df.columns:
            conflict_df = conflict_df.sort_values(by=['_business_key', '_conflict_type'])

        # Use the same file format as original
        self._save_dataframe_by_format(conflict_df, export_path, file_context.filepath.suffix)
        logger.warning(f"Exported {len(conflict_df)} format conflicts to {export_path}")
        logger.info(f"Review file: {export_path}")

    def _export_duplicates(self, conflict_df: pd.DataFrame, file_context: FileContext):
        """Export duplicates using the same file format as the original file."""
        dup_dir = Path(DUPLICATES_ROOT_DIR)
        try:
            dup_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logger.error(f"Failed to create duplicates directory {dup_dir}: {e}")

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
        self._save_dataframe_by_format(conflict_df, export_path, file_context.filepath.suffix)
        logger.warning(f"Exported {len(conflict_df)} duplicates to {export_path}")
        logger.info(f"Review file: {export_path}")

    def _export_failed_rows(self, original_df: pd.DataFrame, error_details: List[Dict], 
                           file_context: FileContext, target_table: str):
        """Export failed rows using the same file format as the original file."""
        if not error_details:
            return
            
        # Extract failed row indices
        failed_indices = [error['row_index'] for error in error_details if 'row_index' in error]
        failed_df = original_df.loc[failed_indices].copy()
        
        # Add error metadata columns
        for error in error_details:
            if 'row_index' in error and error['row_index'] in failed_df.index:
                failed_df.loc[error['row_index'], '_error_message'] = error['error']
                failed_df.loc[error['row_index'], '_failed_reason'] = self._get_failure_reason(error['error'])
        
        # Add guidance column
        failed_df['_GUIDANCE'] = "Fix the data issues indicated above. Save this file with corrections and place it in the failed_rows/to_process folder. The loader will automatically remove the metadata columns during reprocessing."
        
        # Use the same file format as original
        export_filename = self._get_export_filename(file_context.filename, file_context.filepath.suffix)
        export_path = Path(FAILED_ROWS_DIR) / export_filename
        
        self._save_dataframe_by_format(failed_df, export_path, file_context.filepath.suffix)
        logger.warning(f"Exported {len(failed_df)} failed rows to {export_path}")
        logger.info(f"Review file: {export_path}")

    def _get_export_filename(self, original_filename: str, original_suffix: str) -> str:
        """Generate export filename preserving original format."""
        name_without_ext = Path(original_filename).stem
        return f"{name_without_ext}{original_suffix}"

    @log_os_operations
    def _save_dataframe_by_format(self, df: pd.DataFrame, filepath: Path, original_suffix: str):
        """Save dataframe using the same format as the original file with enhanced error handling."""
        file_ext = original_suffix.lower()
        
        logger.info(f"Saving dataframe to {filepath} (format: {file_ext}, rows: {len(df)}, columns: {len(df.columns)})")
        
        try:
            if file_ext == '.csv':
                df.to_csv(filepath, index=False)
            elif file_ext in ['.xlsx', '.xls']:
                df.to_excel(filepath, index=False, engine='openpyxl')
            elif file_ext == '.parquet':
                df.to_parquet(filepath, index=False)
            elif file_ext == '.json':
                df.to_json(filepath, orient='records', indent=2)
            else:
                # Default to CSV for unknown formats
                fallback_path = filepath.with_suffix('.csv')
                df.to_csv(fallback_path, index=False)
                logger.warning(f"Unknown file format {file_ext}, exported as CSV instead: {fallback_path}")
                
            logger.info(f"Successfully saved {len(df)} rows to {filepath}")
            
        except Exception as e:
            logger.error(f"Error exporting to {filepath}: {type(e).__name__}: {e}")
            # Fallback to CSV
            try:
                fallback_path = filepath.with_suffix('.csv')
                df.to_csv(fallback_path, index=False)
                logger.info(f"Exported as CSV fallback: {fallback_path}")
            except Exception as fallback_error:
                logger.error(f"CSV fallback also failed: {fallback_error}")

    def _get_failure_reason(self, error_message: str) -> str:
        """Convert database errors to user-friendly reasons."""
        error_lower = error_message.lower()
        
        if 'violates unique constraint' in error_lower:
            return 'DUPLICATE_KEY'
        elif 'violates foreign key constraint' in error_lower:
            return 'MISSING_REFERENCE'
        elif 'invalid input syntax' in error_lower:
            return 'DATA_TYPE_MISMATCH'
        elif 'null value in column' in error_lower:
            return 'MISSING_REQUIRED_VALUE'
        elif 'value too long' in error_lower:
            return 'VALUE_TOO_LONG'
        elif 'deadlock' in error_lower:
            return 'DEADLOCK'
        elif 'connection' in error_lower:
            return 'CONNECTION_ISSUE'
        else:
            return 'UNKNOWN_ERROR'

    # ---------------------------
    # Enhanced database insertion with error handling
    # ---------------------------
    def _bulk_insert_to_db(self, df: pd.DataFrame, target_table: str) -> bool:
        """
        Enhanced bulk insert with comprehensive error handling, retries, and row-level tracking.
        """
        if df is None or df.empty:
            logger.info("No rows to insert.")
            return True

        total_rows = len(df)
        
        # Use chunk_size from config
        chunk_size = self.config.chunk_size
        chunks = [df[i:i + chunk_size] for i in range(0, total_rows, chunk_size)]
        
        logger.info(f"Processing {total_rows} rows in {len(chunks)} chunks (chunk_size: {chunk_size})")
        
        successful_rows = 0
        failed_rows = 0
        error_details = []
        chunk_failures = 0
        
        for chunk_idx, chunk in enumerate(chunks):
            chunk_result = self._process_chunk_with_retry(
                chunk, target_table, chunk_idx, len(chunks)
            )
            
            if chunk_result['success']:
                successful_rows += chunk_result['processed_rows']
                logger.info(f"Chunk {chunk_idx + 1}/{len(chunks)}: {chunk_result['processed_rows']} rows inserted")
            else:
                chunk_failures += 1
                failed_rows += len(chunk)
                error_details.extend(chunk_result['errors'])
                logger.error(f"Chunk {chunk_idx + 1}/{len(chunks)} failed: {chunk_result['errors']}")
                
                # Try individual row insertion for failed chunks if enabled
                if self.config.enable_row_level_recovery:
                    row_results = self._insert_rows_individually(chunk, target_table)
                    successful_rows += row_results['successful_rows']
                    failed_rows += row_results['failed_rows'] - len(chunk)  # Adjust count
                    error_details.extend(row_results['errors'])
            
            # Stop if too many chunks fail
            if chunk_failures >= self.config.max_chunk_failures:
                logger.error(f"Too many chunk failures ({chunk_failures}), stopping processing")
                break

        # Store error details for potential export
        self._last_insertion_errors = error_details

        # Summary
        if successful_rows > 0:
            logger.info(f"Successfully inserted {successful_rows}/{total_rows} rows into {target_table}")
        
        if failed_rows > 0:
            logger.error(f"Failed to insert {failed_rows}/{total_rows} rows into {target_table}")
            
            if self.config.fail_on_partial_insert and failed_rows > 0:
                return False

        return successful_rows > 0

    def _process_chunk_with_retry(self, chunk: pd.DataFrame, target_table: str, 
                                 chunk_idx: int, total_chunks: int) -> Dict[str, Any]:
        """
        Process a chunk with retry logic and comprehensive error handling.
        """
        max_retries = self.config.retry_attempts
        retry_delay = 1  # seconds
        
        for attempt in range(max_retries + 1):
            try:
                with self.db_manager.get_connection() as conn:
                    return self._insert_chunk_transaction(conn, chunk, target_table)
                    
            except psycopg2.OperationalError as e:
                # Connection errors - retry with backoff
                if attempt < max_retries:
                    wait_time = min(retry_delay * (2 ** attempt), self.config.max_retry_delay)
                    logger.warning(f"Chunk {chunk_idx + 1} attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    return {
                        'success': False,
                        'processed_rows': 0,
                        'errors': [f"OperationalError after {max_retries} retries: {str(e)}"]
                    }
                    
            except psycopg2.Error as e:
                # Database errors - may not be retryable
                error_code = e.pgcode if hasattr(e, 'pgcode') else 'Unknown'
                
                if self._is_retryable_error(error_code) and attempt < max_retries:
                    wait_time = min(retry_delay * (2 ** attempt), self.config.max_retry_delay)
                    logger.warning(f"Chunk {chunk_idx + 1} attempt {attempt + 1} failed with retryable error {error_code}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    return {
                        'success': False,
                        'processed_rows': 0,
                        'errors': [f"Database error {error_code}: {str(e)}"]
                    }
                    
            except Exception as e:
                # Unexpected errors
                return {
                    'success': False,
                    'processed_rows': 0,
                    'errors': [f"Unexpected error: {str(e)}"]
                }
        
        return {
            'success': False,
            'processed_rows': 0,
            'errors': ["Max retries exceeded"]
        }

    def _insert_chunk_transaction(self, conn, chunk: pd.DataFrame, target_table: str) -> Dict[str, Any]:
        """
        Insert a chunk within a single transaction with row-level error handling.
        """
        cols = list(chunk.columns)
        records = chunk.where(pd.notnull(chunk), None).values.tolist()
        
        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
            sql.Identifier(target_table),
            sql.SQL(',').join(map(sql.Identifier, cols))
        )
        
        try:
            with conn.cursor() as cursor:
                # Use page_size=len(records) to insert all rows in one go
                execute_values(cursor, insert_query.as_string(conn), records, 
                             template=None, page_size=len(records))
            conn.commit()
            
            return {
                'success': True,
                'processed_rows': len(chunk),
                'errors': []
            }
            
        except psycopg2.Error as e:
            conn.rollback()
            raise e

    def _insert_rows_individually(self, chunk: pd.DataFrame, target_table: str) -> Dict[str, Any]:
        """
        Fallback: Insert rows individually to identify problematic rows.
        """
        successful_rows = 0
        failed_rows = 0
        errors = []
        
        cols = list(chunk.columns)
        
        for idx, row in chunk.iterrows():
            try:
                with self.db_manager.get_connection() as conn:
                    with conn.cursor() as cursor:
                        values = [row[col] if pd.notnull(row[col]) else None for col in cols]
                        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                            sql.Identifier(target_table),
                            sql.SQL(',').join(map(sql.Identifier, cols)),
                            sql.SQL(',').join([sql.Placeholder()] * len(cols))
                        )
                        cursor.execute(insert_query, values)
                    conn.commit()
                    successful_rows += 1
                    
            except Exception as e:
                failed_rows += 1
                errors.append({
                    'row_index': idx,
                    'row_data': row.to_dict(),
                    'error': str(e),
                    'error_type': type(e).__name__
                })
                logger.warning(f"Failed to insert row {idx}: {e}")
        
        return {
            'successful_rows': successful_rows,
            'failed_rows': failed_rows,
            'errors': errors
        }

    def _is_retryable_error(self, error_code: str) -> bool:
        """
        Determine if an error is retryable based on PostgreSQL error codes.
        """
        retryable_codes = {
            '40001',  # serialization_failure
            '40P01',  # deadlock_detected
            '08006',  # connection_failure
            '08000',  # connection_exception
            '08003',  # connection_does_not_exist
            '08004',  # connection_rejection
            '08007',  # transaction_resolution_unknown
            '57014',  # query_canceled
            '55P03',  # lock_not_available
        }
        return error_code in retryable_codes

    # ---------------------------
    # Enhanced file discovery with OS logging - FIXED PATTERN FILTERING
    # ---------------------------
    def get_files_to_process(self) -> List[FileContext]:
        all_potential_file_contexts = []
        search_locations = []

        logger.info("Starting file discovery process")
        
        for rule in self.processing_rules:
            rule_source_dir = Path(rule.directory)
            if not rule_source_dir.exists():
                logger.warning(f"Source directory not found: {rule_source_dir}")
                continue

            mapping_filepath = Path(rule.mapping_file)
            if not mapping_filepath.exists():
                logger.error(f"Mapping file not found: {mapping_filepath}. Skipping rule '{rule.base_name}'.")
                continue

            # NEW: Get skip patterns from rule configuration
            skip_subdirectories = getattr(rule, 'skip_subdirectories', [])
            skip_file_patterns = getattr(rule, 'skip_file_patterns', [])
            
            search_locations.append(f"Rule '{rule.base_name}': {rule.directory} (subdirectories: {rule.search_subdirectories}, skip_dirs: {skip_subdirectories}, skip_patterns: {skip_file_patterns})")

            if rule.search_subdirectories:
                search_pattern = rule_source_dir.rglob('*')
            else:
                search_pattern = rule_source_dir.iterdir()

            files_found = 0
            files_skipped_pattern = 0
            files_skipped_directory = 0
            files_skipped_skip_pattern = 0
            
            for file_path in search_pattern:
                # Skip directories that are in skip list
                if file_path.is_dir():
                    continue

                # Check if file is in a skipped subdirectory
                if skip_subdirectories:
                    file_relative_path = file_path.relative_to(rule_source_dir)
                    # Check if any parent directory is in the skip list
                    skip_this_file = False
                    for parent in file_relative_path.parents:
                        if parent.name in skip_subdirectories:
                            files_skipped_directory += 1
                            logger.debug(f"Skipping file in excluded directory '{parent}': {file_path}")
                            skip_this_file = True
                            break
                    
                    if skip_this_file:
                        continue

                # Check file extension
                if file_path.suffix.lower() not in ['.csv', '.xlsx', '.xls', '.parquet', '.json']:
                    continue

                # Skip files in processing directories (existing logic)
                if any(part in [DUPLICATES_ROOT_DIR, FORMAT_CONFLICT_DIR, FAILED_ROWS_DIR] for part in file_path.parts):
                    continue

                filename = file_path.name
                
                # NEW: Check if file matches any skip patterns
                if skip_file_patterns:
                    skip_file = False
                    for skip_pattern in skip_file_patterns:
                        try:
                            if re.search(skip_pattern, filename):
                                files_skipped_skip_pattern += 1
                                logger.debug(f"Skipping file matching skip pattern '{skip_pattern}': {filename}")
                                skip_file = True
                                break
                        except re.error as e:
                            logger.warning(f"Invalid skip pattern '{skip_pattern}' for rule '{rule.base_name}': {e}")
                    if skip_file:
                        continue

                match = rule.match(filename)

                # STRICT PATTERN MATCHING: Only include files that match the pattern
                if match:
                    files_found += 1
                    extracted_timestamp = ""
                    if rule.date_format and match.groups():
                        try:
                            date_str = "".join(match.groups())
                            datetime.strptime(date_str, rule.date_format)
                            extracted_timestamp = date_str
                        except ValueError:
                            pass

                    file_modified = datetime.fromtimestamp(file_path.stat().st_mtime)

                    file_context = FileContext(
                        filepath=file_path,
                        filename=filename,
                        target_table=rule.target_table,
                        mapping_filepath=mapping_filepath,
                        extracted_timestamp_str=extracted_timestamp,
                        file_modified_timestamp=file_modified,
                        start_row=rule.start_row or self.global_start_row,
                        start_col=rule.start_col or self.global_start_col,
                        mode=rule.mode or "insert",
                        date_from_filename_col_name=rule.date_from_filename_col_name,
                        hash_exclude_columns=rule.hash_exclude_columns,
                        sheet_config=rule.sheet_config
                    )
                    all_potential_file_contexts.append(file_context)
                    logger.info(f"File '{filename}' MATCHED pattern '{rule.file_pattern}' for rule '{rule.base_name}'")
                else:
                    files_skipped_pattern += 1
                    logger.debug(f"Skipping file '{filename}' - does not match pattern '{rule.file_pattern}'")
            
            logger.info(f"Rule '{rule.base_name}': found {files_found} matching files, "
                       f"skipped {files_skipped_pattern} non-matching files, "
                       f"{files_skipped_directory} files in excluded directories, "
                       f"{files_skipped_skip_pattern} files matching skip patterns")

        # Search in special processing directories with enhanced logging
        special_dirs = [
            (DUPLICATES_TO_PROCESS_DIR, "duplicates", "is_duplicate"),
            (FORMAT_CONFLICT_TO_PROCESS_DIR, "format_conflict", "is_format_conflict"), 
            (FAILED_ROWS_TO_PROCESS_DIR, "failed_rows", "is_failed_row")
        ]

        for dir_path, category, attr_name in special_dirs:
            special_dir = Path(dir_path)
            if special_dir.exists() and special_dir.is_dir():
                files_in_dir = list(special_dir.iterdir())
                logger.info(f"Checking {category} directory: {dir_path} ({len(files_in_dir)} files)")
                
                matched_files = 0
                unmatched_files = 0
                
                for file_path in files_in_dir:
                    if (file_path.is_file() and
                            file_path.suffix.lower() in ['.csv', '.xlsx', '.xls', '.parquet', '.json']):

                        filename = file_path.name
                        matching_rule = None
                        for rule in self.processing_rules:
                            match = rule.match(filename)
                            if match:
                                matching_rule = rule
                                break

                        if matching_rule:
                            matched_files += 1
                            mapping_filepath = Path(matching_rule.mapping_file)

                            extracted_timestamp = ""
                            if matching_rule.date_format and match.groups():
                                try:
                                    date_str = "".join(match.groups())
                                    datetime.strptime(date_str, matching_rule.date_format)
                                    extracted_timestamp = date_str
                                except ValueError:
                                    pass

                            file_modified = datetime.fromtimestamp(file_path.stat().st_mtime)

                            file_context = FileContext(
                                filepath=file_path,
                                filename=filename,
                                target_table=matching_rule.target_table,
                                mapping_filepath=mapping_filepath,
                                extracted_timestamp_str=extracted_timestamp,
                                file_modified_timestamp=file_modified,
                                start_row=matching_rule.start_row or self.global_start_row,
                                start_col=matching_rule.start_col or self.global_start_col,
                                mode=matching_rule.mode or "insert",
                                date_from_filename_col_name=matching_rule.date_from_filename_col_name,
                                hash_exclude_columns=matching_rule.hash_exclude_columns,
                                sheet_config=matching_rule.sheet_config
                            )
                            setattr(file_context, attr_name, True)
                            all_potential_file_contexts.append(file_context)
                            logger.info(f"Found {category} file: {filename} matched by rule '{matching_rule.base_name}' (pattern: '{matching_rule.file_pattern}')")
                        else:
                            unmatched_files += 1
                            logger.warning(f"File {filename} in {category} directory did not match any rule. Skipping.")
                logger.info(f"{category} directory: {matched_files} files matched rules, {unmatched_files} files skipped (no matching rule)")
            else:
                logger.debug(f"{category} directory not found or not accessible: {dir_path}")

        # Filter based on progress tracker and audit checks
        files_to_process = []
        skipped_files = 0
        
        logger.info(f"Found {len(all_potential_file_contexts)} total potential files, applying filters...")

        for fc in all_potential_file_contexts:
            if any([fc.is_duplicate, getattr(fc, 'is_format_conflict', False), getattr(fc, 'is_failed_row', False)]):
                files_to_process.append(fc)
                logger.debug(f"Including {fc.filename} (special processing file)")
                continue

            if self.progress_tracker:
                needs_processing = self.progress_tracker.needs_processing(fc.filepath, fc)
                if not needs_processing:
                    logger.info(f"Skipping unchanged file: {fc.filename}")
                    skipped_files += 1
                    continue

            if fc.mode == "audit" and not any([fc.is_duplicate, getattr(fc, 'is_format_conflict', False), getattr(fc, 'is_failed_row', False)]):
                try:
                    if self.db_manager.file_exists_in_db(fc.target_table, fc.file_modified_timestamp, fc.filename):
                        logger.info(f"Skipping {fc.filename}: Content unchanged in database")
                        if self.progress_tracker:
                            self.progress_tracker.mark_processed(fc.filepath, fc)
                        skipped_files += 1
                        continue
                except Exception as e:
                    logger.error(f"Audit check failed for {fc.filename}: {e}")

            files_to_process.append(fc)

        logger.info(f"File discovery completed: {len(files_to_process)} to process, {skipped_files} skipped")
        return files_to_process

    # ---------------------------
    # Enhanced processing with detailed OS logging
    # ---------------------------
    def process_files(self) -> Dict[str, int]:
        start_time = time.time()
        logger.info(f"=== STARTING RUN {self.run_id} ===")
        logger.info(f"Configuration: batch_size={self.config.batch_size}, delete_files={self.delete_files}")

        file_contexts = self.get_files_to_process()
        processed, failed = 0, 0

        logger.info(f"Processing {len(file_contexts)} files...")

        for i, fc in enumerate(file_contexts):
            logger.info(f"Processing file {i+1}/{len(file_contexts)}: {fc.filename}")
            if self.process_file(fc):
                processed += 1
                logger.info(f"Successfully processed: {fc.filename}")
            else:
                failed += 1
                logger.error(f"Failed to process: {fc.filename}")

        duration = time.time() - start_time
        logger.info(f"Run completed. Processed: {processed}, Failed: {failed}, Duration: {duration:.1f}s")
        logger.info(f"=== RUN {self.run_id} COMPLETED ===")

        return {"processed": processed, "failed": failed}

    def process_file(self, file_context: FileContext) -> bool:
        try:
            mapping = pd.read_csv(file_context.mapping_filepath)
            if not self.db_manager.create_table_if_not_exists(file_context.target_table, mapping):
                logger.error(f"Cannot process {file_context.filename}: Table {file_context.target_table} creation failed")
                return False

            # CONSISTENT PROCESSING FLOW FOR ALL TYPES
            if getattr(file_context, 'is_failed_row', False):
                logger.info(f"Processing failed rows file: {file_context.filename}")
                success = self._process_failed_rows_file(file_context)
                # ENHANCED: Move to processed folder regardless of success/failure
                if success:
                    self._move_to_processed(file_context.filepath, "failed_rows")
                else:
                    logger.error(f"Failed to process failed rows file: {file_context.filename}")
                    
            elif file_context.is_duplicate:
                logger.info(f"Processing duplicate file: {file_context.filename}")
                success = self._process_duplicate_file(file_context)
                # ENHANCED: Move to processed folder regardless of success/failure
                if success:
                    self._move_to_processed(file_context.filepath, "duplicates")
                else:
                    logger.error(f"Failed to process duplicate file: {file_context.filename}")
                    
            elif getattr(file_context, 'is_format_conflict', False):
                logger.info(f"Processing format conflict file: {file_context.filename}")
                success = self._process_format_conflict_file(file_context)
                # ENHANCED: Move to processed folder regardless of success/failure
                if success:
                    self._move_to_processed(file_context.filepath, "format_conflict")
                else:
                    logger.error(f"Failed to process format conflict file: {file_context.filename}")
                    
            else:
                # Check if it's an Excel file with multi-sheet configuration
                if (file_context.filepath.suffix.lower() in ['.xlsx', '.xls'] and 
                    file_context.sheet_config.get('processing_method', 'specific') != 'specific'):
                    logger.info(f"Processing multi-sheet Excel file: {file_context.filename}")
                    success = self._process_excel_file(file_context)
                else:
                    logger.info(f"Processing: {file_context.filename} -> Table: {file_context.target_table}")
                    success = self._process_single_sheet_file(file_context)

                # ENHANCED: Mark progress for regular files
                if success and self.progress_tracker:
                    self.progress_tracker.mark_processed(file_context.filepath, file_context)

            # Optionally delete file after successful processing (only for original files)
            if success and self.delete_files and not any([
                file_context.is_duplicate,
                getattr(file_context, 'is_format_conflict', False),
                getattr(file_context, 'is_failed_row', False)
            ]):
                try:
                    os.remove(file_context.filepath)
                    logger.info(f"Deleted source file: {file_context.filepath}")
                except Exception as e:
                    logger.warning(f"Could not delete file {file_context.filepath}: {e}")

            return success

        except Exception as e:
            logger.error(f"Error processing {file_context.filename}: {e}", exc_info=True)
            return False

    # ---------------------------
    # Metadata removal helper
    # ---------------------------
    def _drop_metadata_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove loader-generated metadata columns if present."""
        if df is None or df.empty:
            return df
        drop_cols = [c for c in df.columns if c in METADATA_COLUMNS]
        if drop_cols:
            logger.debug(f"Dropping metadata columns: {drop_cols}")
            df = df.drop(columns=drop_cols, errors="ignore")
        return df

    # ---------------------------
    # Multi-sheet Excel processing
    # ---------------------------
    def _process_excel_file(self, file_context: FileContext) -> bool:
        """Process Excel file with multiple sheets."""
        processing_method = file_context.sheet_config.get('processing_method', 'specific')
        success = True
        sheets_processed = 0
        total_sheets = 0
        
        if processing_method == 'specific':
            # Single sheet processing
            sheet_result = self._process_single_excel_sheet(file_context)
            if sheet_result:
                sheets_processed += 1
            else:
                success = False
        else:
            # Multiple sheet processing
            sheet_names = self._get_sheets_to_process(file_context)
            total_sheets = len(sheet_names)
            
            for sheet_name in sheet_names:
                logger.info(f"Processing sheet {sheets_processed + 1}/{total_sheets}: '{sheet_name}'")
                
                # Create a copy of file_context for this sheet
                sheet_context = dataclasses.replace(file_context)
                sheet_context.source_sheet = sheet_name
                sheet_context.sheet_config['specific_sheet'] = sheet_name
                sheet_context.sheet_config['processing_method'] = 'specific'
                
                sheet_result = self._process_single_excel_sheet(sheet_context)
                
                if sheet_result:
                    sheets_processed += 1
                else:
                    success = False
                    logger.error(f"Failed to process sheet '{sheet_name}'")
        
        if sheets_processed == 0 and total_sheets > 0:
            if self.config.treat_empty_as_error:
                logger.error(f"All {total_sheets} sheets in {file_context.filename} were empty")
                success = False
            else:
                logger.warning(f"All {total_sheets} sheets in {file_context.filename} were empty")
                success = True  # Empty sheets are not considered errors
        
        logger.info(f"Processed {sheets_processed} sheet(s) from {file_context.filename}")
        return success

    def _get_sheets_to_process(self, file_context: FileContext) -> List[str]:
        """Get list of sheet names to process based on configuration."""
        file_processor = FileProcessor(self.config)
        all_sheets = file_processor.get_excel_sheet_names(file_context.filepath)
        
        processing_method = file_context.sheet_config.get('processing_method', 'specific')
        
        if processing_method == 'multiple':
            specified_sheets = file_context.sheet_config.get('sheet_names', [])
            return [sheet for sheet in specified_sheets if sheet in all_sheets]
        
        elif processing_method == 'all':
            return all_sheets
        
        elif processing_method == 'pattern':
            pattern = file_context.sheet_config.get('sheet_name_pattern', '.*')
            return [sheet for sheet in all_sheets if re.match(pattern, sheet)]
        
        else:
            return [file_context.sheet_config.get('specific_sheet', 'Sheet1')]

    def _process_single_excel_sheet(self, file_context: FileContext) -> bool:
        """Process a single sheet from an Excel file."""
        original_filename = file_context.filename
        
        try:
            df = self.file_processor.load_file(
                file_context.filepath,
                file_context.start_row,
                file_context.start_col,
                file_context.sheet_config
            )
            
            # Enhanced empty detection
            if df is None or df.empty:
                if file_context.source_sheet:
                    logger.warning(f"Sheet contains no data: {file_context.filename} - {file_context.source_sheet}")
                else:
                    logger.warning(f"File contains no data: {file_context.filename}")
                
                # For empty sheets, we still want to mark as processed if using progress tracking
                if self.progress_tracker and not any([
                    getattr(file_context, 'is_format_conflict', False),
                    getattr(file_context, 'is_failed_row', False)
                ]):
                    self.progress_tracker.mark_processed(file_context.filepath, file_context)
                
                return True  # Success - empty sheets are not errors
            
            # Add sheet name to filename for tracking
            if file_context.source_sheet:
                file_context.filename = f"{original_filename}::{file_context.source_sheet}"
            
            # Process the dataframe
            result = self._process_dataframe(df, file_context)
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing sheet {file_context.source_sheet}: {e}")
            return False
        finally:
            # Restore original filename
            file_context.filename = original_filename

    def _process_single_sheet_file(self, file_context: FileContext) -> bool:
        """Process non-Excel files or single-sheet Excel files."""
        df = self.file_processor.load_file(
            file_context.filepath,
            file_context.start_row,
            file_context.start_col,
            file_context.sheet_config
        )
        
        if df is None or df.empty:
            logger.warning(f"File is empty: {file_context.filename}")
            return True
        
        return self._process_dataframe(df, file_context)

    def _process_dataframe(self, df: pd.DataFrame, file_context: FileContext) -> bool:
        """Common dataframe processing logic for all file types."""
        # Automatically remove metadata columns if present
        df = self._drop_metadata_columns(df)

        # Handle new columns (this will update mapping and may block)
        should_block = self._handle_new_columns(df, file_context)
        if should_block:
            logger.error(f"New columns detected in {file_context.filename}. Please configure them in {file_context.mapping_filepath}.")
            return False

        # Check and add configured columns if necessary
        if not self._check_and_add_configured_columns(file_context):
            logger.error(f"Failed to add configured columns to table {file_context.target_table}")
            return False

        mapping = pd.read_csv(file_context.mapping_filepath)

        # Validate data types and separate format conflicts
        clean_df, format_conflict_df = self._validate_data_types(df, mapping, file_context.filename)

        # Export format conflicts if any
        if not format_conflict_df.empty:
            self._export_format_conflicts(format_conflict_df, file_context)
            logger.warning(f"Found {len(format_conflict_df)} format conflicts in {file_context.filename}. These rows have been exported for manual correction.")

        if clean_df.empty:
            logger.warning(f"No valid rows to process in {file_context.filename} after filtering format conflicts")
            return True

        # Drop metadata columns again on clean_df (just in case)
        clean_df = self._drop_metadata_columns(clean_df)

        # Calculate hashes - FIXED: Include sheet context for proper duplicate detection
        hash_exclude = set(file_context.hash_exclude_columns or [])
        clean_df['content_hash'] = self.file_processor.calculate_row_hashes(
            clean_df, 
            hash_exclude, 
            file_context.source_sheet  # Pass sheet name for context
        )

        # Enhanced duplicate detection with better logging
        existing_hashes = self.db_manager.get_existing_hashes(file_context.target_table, file_context.filename)
        logger.info(f"Checking {len(clean_df)} rows against {len(existing_hashes)} existing hashes for {file_context.filename}")
        
        duplicates_mask = clean_df['content_hash'].isin(existing_hashes)
        duplicates_df = clean_df[duplicates_mask]
        unique_df = clean_df[~duplicates_mask]

        if not duplicates_df.empty:
            # Enhanced duplicate analysis
            duplicate_count = len(duplicates_df)
            logger.info(f"Found {duplicate_count} potential duplicates in {file_context.filename}")
            
            # Export duplicates for manual resolution
            self._export_duplicates(duplicates_df, file_context)
            logger.warning(f"Exported {duplicate_count} duplicate rows from {file_context.filename} to {DUPLICATES_ROOT_DIR}")

        if unique_df.empty:
            logger.info(f"No new unique rows to load from {file_context.filename}")
            return True

        # Prepare for DB insert: rename columns to target names based on mapping
        mapping_df = pd.read_csv(file_context.mapping_filepath)
        col_map = {row['RawColumn']: row['TargetColumn'] for _, row in mapping_df.iterrows() if row['data_source'] == 'file'}
        insert_df = unique_df.copy()
        insert_df = insert_df.rename(columns=col_map)

        # Add system columns - FIXED: Remove milliseconds from timestamp
        insert_df['loaded_timestamp'] = datetime.now().replace(microsecond=0)  # No milliseconds
        insert_df['source_filename'] = file_context.filename  # Includes sheet name if applicable
        insert_df['operation'] = file_context.mode

        # Load into DB with enhanced error handling
        try:
            success = self._bulk_insert_to_db(insert_df, file_context.target_table)
            
            # If there were failed rows during insertion, export them
            if not success and hasattr(self, '_last_insertion_errors') and self._last_insertion_errors:
                self._export_failed_rows(unique_df, self._last_insertion_errors, file_context, file_context.target_table)
                
            return success
        except Exception as e:
            logger.error(f"Load failed for {file_context.filename}: {e}", exc_info=True)
            return False

    # ---------------------------
    # Reprocessing methods - UPDATED for consistency
    # ---------------------------
    def _process_duplicate_file(self, file_context: FileContext) -> bool:
        """Process a resolved duplicate file placed in duplicates/to_process/"""
        df = self.file_processor.load_file(
            file_context.filepath,
            file_context.start_row,
            file_context.start_col,
            file_context.sheet_config
        )
        if df is None or df.empty:
            logger.warning(f"Duplicate file is empty: {file_context.filename}")
            return True

        # Remove metadata automatically - CONSISTENT BEHAVIOR
        df = self._drop_metadata_columns(df)

        # Proceed as a regular file from here
        return self._process_dataframe(df, file_context)

    def _process_format_conflict_file(self, file_context: FileContext) -> bool:
        """Process a corrected format conflict file placed in format_conflict/to_process/"""
        df = self.file_processor.load_file(
            file_context.filepath,
            file_context.start_row,
            file_context.start_col,
            file_context.sheet_config
        )
        if df is None or df.empty:
            logger.warning(f"Format conflict file is empty: {file_context.filename}")
            return True

        # Remove metadata automatically - CONSISTENT BEHAVIOR
        df = self._drop_metadata_columns(df)

        # Proceed as regular file
        return self._process_dataframe(df, file_context)

    def _process_failed_rows_file(self, file_context: FileContext) -> bool:
        """Process a corrected failed rows file - CONSISTENT with duplicates/conflicts."""
        df = self.file_processor.load_file(
            file_context.filepath,
            file_context.start_row,
            file_context.start_col,
            file_context.sheet_config
        )
        
        if df is None or df.empty:
            logger.warning(f"Failed rows file is empty: {file_context.filename}")
            return True

        # CONSISTENT: Auto-remove metadata, just like duplicates/conflicts!
        df = self._drop_metadata_columns(df)
        
        # Process as regular file
        return self._process_dataframe(df, file_context)

    # ---------------------------
    # Utility methods - ENHANCED: Better file movement with OS logging
    # ---------------------------
    @log_os_operations
    def _move_to_processed(self, filepath: Path, category: str):
        """Move file to processed directory with consistent naming and enhanced error handling."""
        processed_dir = Path(f"{category}/processed")
        
        try:
            processed_dir.mkdir(exist_ok=True)
            logger.debug(f"Verified processed directory: {processed_dir}")
        except OSError as e:
            logger.error(f"Failed to create processed directory {processed_dir}: {e} (errno: {e.errno})")
            return
        
        try:
            # Add timestamp to avoid name conflicts
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            new_name = f"{filepath.stem}_{timestamp}{filepath.suffix}"
            new_path = processed_dir / new_name
            
            logger.info(f"Moving {category} file: {filepath} -> {new_path}")
            shutil.move(str(filepath), str(new_path))
            logger.info(f"Successfully moved {category} file to processed: {new_path}")
        except OSError as e:
            logger.error(f"Could not move file {filepath} to processed directory: {e} (errno: {e.errno})")
        except Exception as e:
            logger.error(f"Unexpected error moving file {filepath}: {type(e).__name__}: {e}")

# ===========================
# Enhanced utilities with OS logging
# ===========================
@log_os_operations
def create_sample_configs():
    """Create sample configuration files with organized directory structure and enhanced logging."""
    logger.info("Creating sample configuration files...")
    
    # Create organized directory structure
    directories = [
        "rules",
        "inputs/sales_data", 
        "inputs/inventory_data",
        "inputs/weekly_reports",
        DUPLICATES_ROOT_DIR,
        DUPLICATES_TO_PROCESS_DIR,
        DUPLICATES_PROCESSED_DIR,
        FORMAT_CONFLICT_DIR,
        FORMAT_CONFLICT_TO_PROCESS_DIR,
        FORMAT_CONFLICT_PROCESSED_DIR,
        FAILED_ROWS_DIR,
        FAILED_ROWS_TO_PROCESS_DIR,
        FAILED_ROWS_PROCESSED_DIR,
        LOG_DIR
    ]

    for directory in directories:
        try:
            os.makedirs(directory, exist_ok=True)
            logger.debug(f"Created directory: {directory}")
        except OSError as e:
            logger.error(f"Failed to create directory {directory}: {e} (errno: {e.errno})")

    if not os.path.exists(GLOBAL_CONFIG_FILE):
        global_config = {
            "dbname": "your_database_name",
            "user": "your_username",
            "password": "your_password",
            "host": "localhost",
            "port": 5432,
            "batch_size": 1000,
            "max_connections": 5,
            "min_connections": 1,
            "retry_attempts": 3,
            "enable_progress_tracking": True,
            "enable_data_validation": True,
            "timestamp_tolerance_seconds": 1.0,
            "global_hash_exclude_columns": [],
            "lock_timeout": 3600,
            "auto_add_columns": True,
            "delete_files": "N",
            "skip_empty_sheets": True,
            "warn_on_empty_sheets": True,
            "treat_empty_as_error": False,
            
            # Enhanced insertion settings
            "enable_row_level_recovery": True,
            "fail_on_partial_insert": False,
            "retry_on_deadlock": True,
            "max_retry_delay": 30,
            "enable_batch_validation": True,
            "chunk_size": 100,
            "max_chunk_failures": 5,

            # NEW: Sample file control
            "generate_sample_files": False
        }
        with open(GLOBAL_CONFIG_FILE, 'w') as f:
            yaml.dump(global_config, f)
        logger.info(f"Created sample global config: {GLOBAL_CONFIG_FILE}")
    else:
        logger.info(f"Global config already exists: {GLOBAL_CONFIG_FILE}")

    rules = {
        "sales_rule.yaml": {
            "base_name": "sales",
            "directory": "inputs/sales_data",
            "file_pattern": r"sales_\d{8}\.csv",
            "date_format": "%Y%m%d",
            "start_row": 0,
            "start_col": 0,
            "mode": "insert",
            "date_from_filename_col_name": "file_date",
            "hash_exclude_columns": [],
            "search_subdirectories": True,
            "skip_subdirectories": ["processed", "archive", "temp"],
            "skip_file_patterns": [r".*test.*", r".*backup.*"],
            "mapping_file": "rules/sales_mapping.csv"
        },
        "inventory_rule.yaml": {
            "base_name": "inventory",
            "directory": "inputs/inventory_data",
            "file_pattern": r"inventory_\d{8}\.xlsx",
            "date_format": "%Y%m%d",
            "start_row": 0,
            "start_col": 0,
            "mode": "insert",
            "date_from_filename_col_name": "file_date",
            "hash_exclude_columns": [],
            "search_subdirectories": True,
            "skip_subdirectories": ["processed", "rejected"],
            "skip_file_patterns": [r".*2022.*", r".*2023.*", r".*draft.*"],
            "sheet_config": {
                "processing_method": "multiple",
                "sheet_names": ["Sheet1", "Sheet2"]
            },
            "mapping_file": "rules/inventory_mapping.csv"
        },
        "weekly_rule.yaml": {
            "base_name": "weekly_reports",
            "directory": "inputs/weekly_reports",
            "file_pattern": r"weekly_\d{8}\.xlsx",
            "date_format": "%Y%m%d",
            "start_row": 0,
            "start_col": 0,
            "mode": "insert",
            "date_from_filename_col_name": "report_date",
            "hash_exclude_columns": [],
            "search_subdirectories": True,
            "skip_subdirectories": ["processed", "archived", "temp"],
            "skip_file_patterns": [r".*old.*", r".*template.*", r".*202[0-2].*"],
            "sheet_config": {
                "processing_method": "all"
            },
            "mapping_file": "rules/weekly_mapping.csv"
        }
    }

    for rule_file, rule_data in rules.items():
        rule_path = Path("rules") / rule_file
        if not rule_path.exists():
            with open(rule_path, 'w') as f:
                yaml.dump(rule_data, f)
            logger.info(f"Created sample rule file: {rule_path}")
        else:
            logger.info(f"Rule file already exists: {rule_path}")

    today = datetime.today()
    dates = [today - pd.Timedelta(days=7 * i) for i in range(4)]
    for dt in dates:
        date_str = dt.strftime("%Y%m%d")
        
        # Sales data (CSV)
        sample_sales_file = Path("inputs/sales_data") / f"sales_{date_str}.csv"
        if not sample_sales_file.exists():
            pd.DataFrame({
                "OrderID": [1, 2, 3],
                "Customer": ["Alice", "Bob", "Charlie"],
                "Amount": [120.5, 85.0, 42.75]
            }).to_csv(sample_sales_file, index=False)
            logger.info(f"Created sample sales file: {sample_sales_file}")

        # Inventory data (Excel with multiple sheets)
        sample_inventory_file = Path("inputs/inventory_data") / f"inventory_{date_str}.xlsx"
        if not sample_inventory_file.exists():
            with pd.ExcelWriter(sample_inventory_file, engine='openpyxl') as writer:
                pd.DataFrame({
                    "ItemID": [101, 102, 103],
                    "ItemName": ["Widget", "Gadget", "Thingy"],
                    "Stock": [50, 20, 75]
                }).to_excel(writer, sheet_name='Sheet1', index=False)
                pd.DataFrame({
                    "ItemID": [201, 202, 203],
                    "ItemName": ["Product A", "Product B", "Product C"],
                    "Stock": [30, 40, 60]
                }).to_excel(writer, sheet_name='Sheet2', index=False)
            logger.info(f"Created sample inventory file with multiple sheets: {sample_inventory_file}")

        # Weekly reports (Excel with multiple sheets)
        sample_weekly_file = Path("inputs/weekly_reports") / f"weekly_{date_str}.xlsx"
        if not sample_weekly_file.exists():
            with pd.ExcelWriter(sample_weekly_file, engine='openpyxl') as writer:
                pd.DataFrame({
                    "WeekStart": [dt.strftime("%Y-%m-%d")],
                    "TotalSales": [round(np.random.uniform(1000, 2000), 2)],
                    "TotalOrders": [int(np.random.randint(20, 50))]
                }).to_excel(writer, sheet_name='Summary', index=False)
                pd.DataFrame({
                    "Department": ["Sales", "Marketing", "Operations"],
                    "WeeklyTotal": [500, 300, 400]
                }).to_excel(writer, sheet_name='Departments', index=False)
            logger.info(f"Created sample weekly report file with multiple sheets: {sample_weekly_file}")

# ===========================
# Enhanced main entrypoint with OS error handling
# ===========================
if __name__ == "__main__":
    loader = None
    try:
        # Enhanced stale lock detection with logging
        if os.path.exists(LOCK_FILE):
            try:
                lock_time = os.path.getmtime(LOCK_FILE)
                current_time = time.time()
                lock_age = current_time - lock_time
                
                if lock_age > 3600:
                    logger.warning(f"Removing stale lock file (age: {lock_age:.1f}s)")
                    try:
                        os.remove(LOCK_FILE)
                        print("Removed stale lock file")
                    except OSError as e:
                        logger.warning(f"Could not remove stale lock file: {e}")
            except Exception as e:
                logger.warning(f"Error checking lock file: {e}")

        # Remove pattern extraction functionality completely
        # If user tries to use pattern extraction, direct them to the utility script
        if len(sys.argv) > 1 and any(arg in sys.argv for arg in ['--extract-pattern', '--pattern', '-p']):
            print("Pattern extraction has been moved to a separate utility script.")
            print("Please use: python pattern_utils.py <filename1> [filename2 ...]")
            print("Example: python pattern_utils.py ghy_20250505.xlsx")
            sys.exit(1)

        # NEW: Only create sample configs if explicitly enabled or first run
        config_exists = os.path.exists(GLOBAL_CONFIG_FILE)
        
        if not config_exists:
            # First run - create sample configs
            create_sample_configs()
        else:
            # Check if sample generation is enabled in config
            try:
                with open(GLOBAL_CONFIG_FILE, 'r') as f:
                    config_data = yaml.safe_load(f)
                    generate_samples = config_data.get('generate_sample_files', False)
                    
                if generate_samples:
                    create_sample_configs()
                else:
                    logger.info("Sample file generation is disabled in config")
            except Exception as e:
                logger.warning(f"Could not read config to check sample generation setting: {e}")

        loader = PostgresLoader(
            global_config_file=GLOBAL_CONFIG_FILE,
            rules_folder_path='rules'
        )

        result = loader.process_files()
        print(f"Processed: {result['processed']}, Failed: {result['failed']}")
        
    except KeyboardInterrupt:
        logger.info("Processing interrupted by user")
        print("Processing interrupted by user")
        if loader:
            loader.cleanup()
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Fatal error during processing: {type(e).__name__}: {e}")
        print(f"Processing failed: {e}")
        print("Check processing.log for details")
        if loader:
            loader.cleanup()
        sys.exit(1)
    finally:
        # Ensure cleanup happens even if there's an unhandled exception
        if loader:
            loader.cleanup()