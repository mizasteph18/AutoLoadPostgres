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
import requests
from pathlib import Path
from datetime import datetime
from psycopg2 import sql, pool
from psycopg2.extras import execute_values
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass, field
from contextlib import contextmanager

# Constants
DEFAULT_BATCH_SIZE = 1000
PROGRESS_FILE = "processing_progress.json"
GLOBAL_CONFIG_FILE = "global_loader_config.yaml"
RESERVED_COLUMNS = {"loaded_timestamp", "source_filename", "content_hash", "operation"}
HASH_EXCLUDE_COLS = RESERVED_COLUMNS
DUPLICATES_DIR = "duplicates"
SYSTEM_COLUMNS_ORDER = ["loaded_timestamp", "source_filename", "content_hash", "operation"]
LOCK_FILE = "loader.lock"  # Lock file for preventing concurrent runs

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("processing.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class ProcessingConfig:
    """Configuration class for processing parameters."""
    batch_size: int = DEFAULT_BATCH_SIZE
    max_connections: int = 5
    min_connections: int = 1
    retry_attempts: int = 3
    enable_progress_tracking: bool = True
    enable_data_validation: bool = True
    timestamp_tolerance_seconds: float = 1.0
    global_hash_exclude_columns: List[str] = field(default_factory=list)
    teams_webhook: str = ""  # Teams webhook URL
    lock_timeout: int = 3600  # Lock timeout in seconds (1 hour)
    
@dataclass
class FileProcessingRule:
    """Defines rules for processing specific types of files."""
    base_name: str
    directory: str
    file_pattern: str
    date_format: Optional[str] = None
    start_row: Optional[int] = None
    start_col: Optional[int] = None
    mode: Optional[str] = None
    date_from_filename_col_name: Optional[str] = None
    hash_exclude_columns: List[str] = field(default_factory=list)
    _compiled_pattern: Any = field(init=False, repr=False)

    def __post_init__(self):
        self._compiled_pattern = re.compile(self.file_pattern)

    def match(self, filename: str) -> Optional[re.Match]:
        return self._compiled_pattern.match(filename)
    
    @property
    def target_table(self) -> str:
        return self.base_name

    @property
    def mapping_file(self) -> str:
        return f"{self.base_name}_mapping.csv"
    
    def validate(self) -> Tuple[bool, List[str]]:
        errors = []
        if not self.directory:
            errors.append("Directory is required")
        if not self.file_pattern:
            errors.append("File pattern is required")
        if self.mode and self.mode not in ["cancel_and_replace", "audit", "insert"]:
            errors.append(f"Invalid mode: {self.mode}")
        if self.date_from_filename_col_name and not self.date_format:
            errors.append("date_format is required when date_from_filename_col_name is specified")
        return len(errors) == 0, errors

@dataclass
class FileContext:
    """Holds file processing context."""
    filepath: Path
    filename: str
    target_table: str
    mapping_filepath: Path
    extracted_timestamp_str: str
    file_modified_timestamp: datetime
    start_row: int
    start_col: int
    mode: str
    date_from_filename_col_name: Optional[str]
    hash_exclude_columns: List[str]
    is_duplicate: bool = False

class DatabaseManager:
    """Handles database connections and operations."""
    
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
        except Exception as e:
            logger.error(f"Failed to get existing hashes: {e}")
            return set()

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
        
        # Check blocking conditions
        # 1. File columns with LoadFlag='Y' and order=null
        condition1 = mapping[
            (mapping['data_source'] == 'file') & 
            (mapping['LoadFlag'] == 'Y') & 
            (mapping['order'].isnull())
        ]
        if not condition1.empty:
            errors.append(f"Critical: Found file columns with LoadFlag='Y' and order=null: {condition1['RawColumn'].tolist()}")
        
        # 2. Columns with LoadFlag='N' and IndexColumn='Y'
        condition2 = mapping[
            (mapping['LoadFlag'] == 'N') & 
            (mapping['IndexColumn'] == 'Y')
        ]
        if not condition2.empty:
            errors.append(f"Critical: Found columns with LoadFlag='N' and IndexColumn='Y': {condition2['RawColumn'].tolist()}")
        
        # 3. Columns with blank LoadFlag (unconfigured new columns)
        condition3 = mapping[
            (mapping['LoadFlag'] == '') |
            (mapping['LoadFlag'].isna())
        ]
        if not condition3.empty:
            errors.append(f"Critical: Found unconfigured new columns: {condition3['RawColumn'].tolist()}")
        
        # Check for missing flagged columns
        required_load_cols = mapping[mapping['LoadFlag'] == 'Y']['TargetColumn'].values
        missing_required_cols = [col for col in required_load_cols if col not in df.columns]
        if missing_required_cols:
            errors.append(f"Critical: Missing columns: {', '.join(missing_required_cols)}")
        
        if errors:
            return False, errors
        
        return True, errors

class ProgressTracker:
    """Tracks processing progress for resume capability with modification times."""
    
    def __init__(self, progress_file: str = PROGRESS_FILE):
        self.progress_file = progress_file
        self.processed_files = self._load_progress()
    
    def _load_progress(self) -> dict:
        """Load progress as {filepath: last_processed_mod_time}"""
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
                json.dump(self.processed_files, f)
        except Exception as e:
                logger.error(f"Could not save progress: {e}")
    
    def mark_processed(self, filepath: Path, mod_time: datetime) -> None:
        """Record file processing with modification time"""
        self.processed_files[str(filepath)] = mod_time.isoformat()
        self.save_progress()
    
    def needs_processing(self, filepath: Path, current_mod_time: datetime) -> bool:
        """Check if file is new or modified since last processing"""
        stored_time = self.processed_files.get(str(filepath))
        if not stored_time:
            return True  # New file
        
        try:
            stored_dt = datetime.fromisoformat(stored_time)
            # Check if modified beyond tolerance
            time_diff = abs((current_mod_time - stored_dt).total_seconds())
            return time_diff > 1.0  # 1 second tolerance
        except Exception:
            return True  # Corrupted timestamp, reprocess

class FileProcessor:
    """Handles file operations and data extraction."""
    
    def __init__(self, config: ProcessingConfig):
        self.config = config
    
    def load_file(self, file_path: Path, start_row: int = 0, start_col: int = 0) -> pd.DataFrame:
        file_ext = file_path.suffix.lower()
        
        try:
            if file_ext == '.csv':
                return pd.read_csv(file_path, skiprows=range(1, start_row + 1), header=0).iloc[:, start_col:]
            elif file_ext in ['.xlsx', '.xls']:
                return pd.read_excel(file_path, skiprows=start_row, header=0).iloc[:, start_col:]
            elif file_ext == '.parquet':
                return pd.read_parquet(file_path).iloc[:, start_col:]
            elif file_ext == '.json':
                return pd.read_json(file_path).iloc[:, start_col:]
            else:
                raise ValueError(f"Unsupported file format: {file_ext}")
        except Exception as e:
            logger.error(f"Error loading file: {e}")
            raise
    
    def calculate_row_hashes(self, df: pd.DataFrame, exclude_columns: Set[str]) -> List[str]:
        """Calculate content hashes for each row with custom exclusions."""
        hashes = []
        for _, row in df.iterrows():
            # Combine global and rule-specific exclusions
            exclude = HASH_EXCLUDE_COLS | set(exclude_columns)
            data = {k: v for k, v in row.items() if k not in exclude}
            hasher = hashlib.sha256()
            hasher.update(json.dumps(data, sort_keys=True).encode('utf-8'))
            hashes.append(hasher.hexdigest())
        return hashes

class PostgresLoader:
    """Main loader class with intelligent duplicate handling and Teams notifications."""
    
    def __init__(self, db_config: Dict[str, Any],
                 global_start_row: int = 0, global_start_col: int = 0,
                 delete_files: str = "N",
                 global_config_file: str = None,
                 rules_folder_path: str = "rules"):
        
        self.config = self._load_global_config(global_config_file)
        self.processing_rules = self._load_processing_rules(rules_folder_path)
        self.db_manager = DatabaseManager(db_config, self.config)
        self.validator = DataValidator(self.config)
        self.file_processor = FileProcessor(self.config)
        self.progress_tracker = ProgressTracker() if self.config.enable_progress_tracking else None
        
        self.global_start_row = global_start_row
        self.global_start_col = global_start_col
        self.delete_files = delete_files.upper() == "Y"
        self.lock_acquired = False
        self.run_id = datetime.now().isoformat()
        
        # Acquire lock to prevent concurrent runs
        self._acquire_lock()
        atexit.register(self._release_lock)
        
        self._validate_setup()
    
    def _acquire_lock(self):
        """Prevent multiple concurrent runs with lock file"""
        if os.path.exists(LOCK_FILE):
            lock_time = os.path.getmtime(LOCK_FILE)
            if time.time() - lock_time < self.config.lock_timeout:
                logger.error("Another instance is running. Exiting.")
                self.send_teams_notification(
                    "⛔ LOADER BLOCKED",
                    f"Another instance is already running\nLock file: {LOCK_FILE}"
                )
                sys.exit(0)
            else:
                logger.warning("Stale lock file detected. Removing...")
                os.remove(LOCK_FILE)
        
        # Create new lock file
        with open(LOCK_FILE, 'w') as f:
            f.write(str(os.getpid()))
        self.lock_acquired = True
        logger.info(f"Lock acquired for run {self.run_id}")
    
    def _release_lock(self):
        """Clean up lock file on exit"""
        if self.lock_acquired and os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
            logger.info("Lock released")
    
    def send_teams_notification(self, title: str, message: str):
        """Send notification to Microsoft Teams channel"""
        if not self.config.teams_webhook:
            return False
        
        try:
            payload = {
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "themeColor": "0076D7" if "SUCCESS" in title else "FF0000",
                "summary": "ETL Loader Notification",
                "sections": [{
                    "activityTitle": title,
                    "activitySubtitle": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "text": message,
                    "markdown": True
                }]
            }
            
            response = requests.post(
                self.config.teams_webhook,
                headers={"Content-Type": "application/json"},
                json=payload,
                timeout=5
            )
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Teams notification failed: {e}")
            return False
    
    def _load_global_config(self, config_file: Optional[str]) -> ProcessingConfig:
        if config_file and os.path.exists(config_file):
            try:
                with open(config_file, 'r') as f:
                    if config_file.endswith('.yaml') or config_file.endswith('.yml'):
                        config_data = yaml.safe_load(f)
                    else:
                        config_data = json.load(f)
                return ProcessingConfig(**config_data)
            except Exception as e:
                logger.warning(f"Could not load global config: {e}")
        
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
                    
                    rule = FileProcessingRule(
                        base_name=base_name,
                        directory=rule_data.get('directory'),
                        file_pattern=rule_data.get('file_pattern'),
                        date_format=rule_data.get('date_format'),
                        start_row=rule_data.get('start_row'),
                        start_col=rule_data.get('start_col'),
                        mode=rule_data.get('mode', 'insert'),
                        date_from_filename_col_name=rule_data.get('date_from_filename_col_name'),
                        hash_exclude_columns=rule_data.get('hash_exclude_columns', [])
                    )
                    
                    is_valid, errors = rule.validate()
                    if not is_valid:
                        logger.error(f"Invalid rule: {', '.join(errors)}")
                        continue
                    
                    rules.append(rule)
                    logger.info(f"Loaded rule '{base_name}' with hash exclusions: {rule.hash_exclude_columns}")
                except Exception as e:
                    logger.error(f"Failed to load rule: {e}")
        
        return rules
    
    def _validate_setup(self) -> None:
        logger.info("Validating setup...")
        
        for rule in self.processing_rules:
            rule_source_dir = Path(rule.directory)
            if not rule_source_dir.exists():
                rule_source_dir.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created directory: {rule_source_dir}")
            
            mapping_filepath = rule_source_dir / rule.mapping_file
            
            if not mapping_filepath.exists():
                logger.warning(f"Creating mapping file: {mapping_filepath}")
                sample_file = next(rule_source_dir.glob("*.*"), None)
                
                if sample_file:
                    self._generate_mapping_file(
                        source_filepath=sample_file,
                        mapping_filepath=mapping_filepath,
                        start_row=rule.start_row or self.global_start_row,
                        start_col=rule.start_col or self.global_start_col
                    )
                else:
                    logger.error(f"No sample file found for {rule.base_name}")

        if not self.db_manager.test_connection():
            self.send_teams_notification(
                "🚨 DATABASE CONNECTION FAILED",
                "Database connection test failed\nCheck connection settings"
            )
            raise ConnectionError("Database connection test failed")
        
        logger.info("Setup validation completed")
    
    def _generate_mapping_file(self, source_filepath: Path, mapping_filepath: Path,
                              start_row: int = 0, start_col: int = 0):
        """Generate mapping file based on sample file with LoadFlag='' for file columns."""
        try:
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
                
                if not df_sample.empty:
                    df_sample = df_sample.iloc[:, start_col:]
                columns = df_sample.columns.tolist()
                dtypes = df_sample.dtypes.apply(str).to_dict()
            else:
                columns = []
                dtypes = {}
            
            mapping_data = []
            for i, col in enumerate(columns):
                pd_type = dtypes.get(col, 'object')
                sql_type = "TEXT"
                if "int" in pd_type:
                    sql_type = "INTEGER"
                elif "float" in pd_type:
                    sql_type = "NUMERIC"
                elif "datetime" in pd_type:
                    sql_type = "TIMESTAMP"
                elif "bool" in pd_type:
                    sql_type = "BOOLEAN"
                
                mapping_data.append({
                    'RawColumn': col,
                    'TargetColumn': col.lower().replace(' ', '_'),
                    'DataType': sql_type,
                    'LoadFlag': '',  # EMPTY to require user configuration
                    'IndexColumn': 'N',
                    'data_source': 'file',
                    'definition': '',
                    'order': i
                })
            
            # Add system columns
            for col in RESERVED_COLUMNS:
                sql_type = "TEXT"
                if col == "loaded_timestamp":
                    sql_type = "TIMESTAMP"
                mapping_data.append({
                    'RawColumn': col,
                    'TargetColumn': col,
                    'DataType': sql_type,
                    'LoadFlag': 'Y',  # Always enabled for system columns
                    'IndexColumn': 'N',
                    'data_source': 'system',
                    'definition': '',
                    'order': -1
                })
            
            # Create DataFrame and sort
            mapping_df = pd.DataFrame(mapping_data)
            
            # Custom sorting: system columns first (in fixed order), then file columns by order
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
            logger.info(f"Created mapping file: {mapping_filepath}")
        except Exception as e:
            logger.error(f"Mapping generation failed: {e}")
            # Create empty mapping file as fallback
            pd.DataFrame(columns=[
                'RawColumn', 'TargetColumn', 'DataType', 'LoadFlag', 'IndexColumn',
                'data_source', 'definition', 'order'
            ]).to_csv(mapping_filepath, index=False)
    
    def _handle_new_columns(self, df: pd.DataFrame, file_context: FileContext) -> bool:
        """Detect new columns, update mapping file, and block processing."""
        mapping = pd.read_csv(file_context.mapping_filepath)
        
        # Get current columns in data file order
        current_cols = df.columns.tolist()
        existing_cols = set(mapping[mapping['data_source'] == 'file']['RawColumn'])
        new_cols = [col for col in current_cols if col not in existing_cols]
        
        if not new_cols:
            return False
        
        logger.warning(f"New columns detected: {', '.join(new_cols)}")
        
        # Build position map: {column: index}
        col_positions = {col: idx for idx, col in enumerate(current_cols)}
        
        # Update mapping with new columns at correct positions
        updated_mapping = []
        
        # Process existing columns
        for _, row in mapping.iterrows():
            row_dict = row.to_dict()
            if row_dict['data_source'] == 'file':
                # Update order to current position
                row_dict['order'] = col_positions.get(row_dict['RawColumn'], row_dict['order'])
            updated_mapping.append(row_dict)
        
        # Add new columns at their exact positions
        for col in new_cols:
            updated_mapping.append({
                'RawColumn': col,
                'TargetColumn': col.lower().replace(' ', '_'),
                'DataType': 'TEXT',
                'LoadFlag': '',  # EMPTY to require user configuration
                'IndexColumn': 'N',
                'data_source': 'file',
                'definition': '',
                'order': col_positions[col]
            })
        
        # Create DataFrame and sort by order
        updated_df = pd.DataFrame(updated_mapping)
        updated_df = updated_df.sort_values('order')
        
        # Save updated mapping
        updated_df.to_csv(file_context.mapping_filepath, index=False)
        logger.info(f"Updated mapping file with new columns at correct positions")
        
        return True
    
    def get_files_to_process(self) -> List[FileContext]:
        all_potential_file_contexts = []

        for rule in self.processing_rules:
            rule_source_dir = Path(rule.directory)
            if not rule_source_dir.exists():
                continue

            mapping_filepath = rule_source_dir / rule.mapping_file
            if not mapping_filepath.exists():
                continue

            # Process regular files
            for file_path in rule_source_dir.iterdir():
                if file_path.is_file() and file_path.suffix.lower() in ['.csv', '.xlsx', '.xls', '.parquet', '.json']:
                    filename = file_path.name
                    match = rule.match(filename)
                    
                    if match:
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
                            hash_exclude_columns=rule.hash_exclude_columns
                        )
                        all_potential_file_contexts.append(file_context)
            
            # Process duplicate files
            dup_dir = rule_source_dir / DUPLICATES_DIR
            if dup_dir.exists():
                for dup_file in dup_dir.iterdir():
                    if dup_file.is_file() and dup_file.suffix.lower() in ['.csv', '.xlsx', '.xls', '.parquet', '.json']:
                        filename = dup_file.name
                        match = rule.match(filename)
                        
                        if match:
                            extracted_timestamp = ""
                            if rule.date_format and match.groups():
                                try:
                                    date_str = "".join(match.groups())
                                    datetime.strptime(date_str, rule.date_format) 
                                    extracted_timestamp = date_str
                                except ValueError:
                                    pass
                            
                            file_modified = datetime.fromtimestamp(dup_file.stat().st_mtime)

                            file_context = FileContext(
                                filepath=dup_file,
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
                                is_duplicate=True
                            )
                            all_potential_file_contexts.append(file_context)
        
        files_to_process = []
        for fc in all_potential_file_contexts:
            # Skip unchanged files
            if (self.progress_tracker and 
                not self.progress_tracker.needs_processing(fc.filepath, fc.file_modified_timestamp)):
                logger.info(f"Skipping unchanged file: {fc.filename}")
                continue
            
            # Skip already processed files
            if self.progress_tracker and str(fc.filepath) in self.progress_tracker.processed_files:
                logger.info(f"Skipping {fc.filename}: Already processed")
                continue
            
            # Skip unchanged files in audit mode
            if fc.mode == "audit" and not fc.is_duplicate:
                try:
                    if self.db_manager.file_exists_in_db(fc.target_table, fc.file_modified_timestamp, fc.filename):
                        logger.info(f"Skipping {fc.filename}: Content unchanged")
                        if self.progress_tracker:
                            self.progress_tracker.mark_processed(fc.filepath, fc.file_modified_timestamp)
                        continue
                except Exception as e:
                    logger.error(f"Audit check failed: {e}")
            
            files_to_process.append(fc)

        logger.info(f"Files to process: {len(files_to_process)}")
        return files_to_process
    
    def process_files(self) -> Dict[str, int]:
        start_time = time.time()
        logger.info(f"=== STARTING RUN {self.run_id} ===")
        self.send_teams_notification(
            "🟢 LOADER STARTED",
            f"Run ID: {self.run_id}\nFiles to process: Checking..."
        )
        
        file_contexts = self.get_files_to_process()
        processed, failed = 0, 0
        
        for fc in file_contexts:
            if self.process_file(fc):
                processed += 1
            else:
                failed += 1
        
        # Final status report
        duration = time.time() - start_time
        status = f"Processed: {processed}, Failed: {failed}, Duration: {duration:.1f}s"
        logger.info(f"Run completed. {status}")
        
        # Send Teams notification
        if processed + failed > 0:
            self.send_teams_notification(
                "✅ LOADER COMPLETED" if failed == 0 else "⚠️ LOADER COMPLETED WITH ERRORS",
                f"Run ID: {self.run_id}\n{status}"
            )
                
        return {"processed": processed, "failed": failed}
    
    def process_file(self, file_context: FileContext) -> bool:
        try:
            if file_context.is_duplicate:
                logger.info(f"Processing duplicate file: {file_context.filename}")
                success = self._process_duplicate_file(file_context)
            else:
                logger.info(f"Processing: {file_context.filename} -> Table: {file_context.target_table}")
                success = self._process_regular_file(file_context)
            
            # Update progress tracking
            if success and self.progress_tracker:
                self.progress_tracker.mark_processed(file_context.filepath, file_context.file_modified_timestamp)
            
            return success
            
        except Exception as e:
            error_msg = f"Error processing {file_context.filename}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            self.send_teams_notification("❗ PROCESSING ERROR", error_msg)
            return False
    
    def _process_regular_file(self, file_context: FileContext) -> bool:
        """Process a regular (non-duplicate) file"""
        # Load file data
        df = self.file_processor.load_file(
            file_context.filepath,
            file_context.start_row,
            file_context.start_col
        )
        
        if df.empty:
            logger.warning(f"File is empty: {file_context.filename}")
            return True
        
        # Handle new columns
        if self._handle_new_columns(df, file_context):
            error_msg = f"New columns detected in {file_context.filename}. Processing blocked."
            logger.error(error_msg)
            self.send_teams_notification("⚠️ NEW COLUMNS DETECTED", error_msg)
            return False
        
        # Load mapping for validation
        mapping = pd.read_csv(file_context.mapping_filepath)
        
        # Validate BEFORE processing duplicates to catch configuration issues early
        is_valid, errors = self.validator.validate_dataframe(df, mapping, file_context.filename)
        if errors:
            for error in errors:
                logger.error(error)
            self.send_teams_notification(
                "❌ VALIDATION FAILED",
                f"File: {file_context.filename}\nErrors:\n- " + "\n- ".join(errors)
            )
        if not is_valid:
            return False
        
        # Calculate row hashes with custom exclusions
        hash_exclude = set(file_context.hash_exclude_columns)
        df['content_hash'] = self.file_processor.calculate_row_hashes(df, hash_exclude)
        
        # Identify and isolate duplicates
        unique_df, duplicates_df = self._identify_conflicts(df, file_context)
        
        # Export duplicates if found
        if not duplicates_df.empty:
            self._export_duplicates(duplicates_df, file_context)
            logger.warning(f"Found {len(duplicates_df)} duplicates in {file_context.filename}")
            self.send_teams_notification(
                "⚠️ DUPLICATES FOUND",
                f"File: {file_context.filename}\nDuplicates: {len(duplicates_df)}"
            )
        
        # Process only unique rows
        if unique_df.empty:
            logger.info(f"No unique rows to process in {file_context.filename}")
            return True
            
        return self._process_unique_data(unique_df, file_context)

    def _identify_conflicts(self, df: pd.DataFrame, file_context: FileContext) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Identify all conflicting rows using mapping file's IndexColumn"""
        # Create conflict markers
        df['_conflict_type'] = None
        df['_business_key'] = ""
        
        # Get index columns from mapping file
        mapping = pd.read_csv(file_context.mapping_filepath)
        business_keys = mapping[
            (mapping['IndexColumn'] == 'Y') & 
            (mapping['LoadFlag'] == 'Y')
        ]['RawColumn'].tolist()
        
        target_key_cols = []
        
        # Map business keys to target columns
        if business_keys:
            for raw_col in business_keys:
                map_row = mapping[(mapping['RawColumn'] == raw_col) & 
                                 (mapping['LoadFlag'] == 'Y')]
                if not map_row.empty:
                    target_col = map_row.iloc[0]['TargetColumn']
                    if target_col in df.columns:
                        target_key_cols.append(target_col)
        
        # Create business key string for sorting
        if target_key_cols:
            df['_business_key'] = df[target_key_cols].astype(str).agg('|'.join, axis=1)
        
        # Find exact duplicates (mark ALL occurrences)
        hash_dupes = df.duplicated(subset=['content_hash'], keep=False)
        df.loc[hash_dupes, '_conflict_type'] = 'EXACT_DUPLICATE'
        
        # Find business key conflicts
        if target_key_cols:
            bk_dupes = df.duplicated(subset=target_key_cols, keep=False)
            # Only mark if not already marked
            new_conflicts = bk_dupes & (df['_conflict_type'].isna())
            df.loc[new_conflicts, '_conflict_type'] = 'BUSINESS_KEY_CONFLICT'
            
            # Upgrade exact duplicates to combo type if also in business conflict
            combo_mask = hash_dupes & bk_dupes
            df.loc[combo_mask, '_conflict_type'] = 'EXACT_AND_BUSINESS_CONFLICT'
        
        # Split into clean and conflict sets
        clean_df = df[df['_conflict_type'].isna()].copy()
        conflict_df = df[~df['_conflict_type'].isna()].copy()
        
        # Clean temporary columns from clean_df
        clean_df = clean_df[[col for col in clean_df.columns if not col.startswith('_')]]
        
        return clean_df, conflict_df

    def _export_duplicates(self, conflict_df: pd.DataFrame, file_context: FileContext):
        """Export conflicts to duplicates folder with original filename"""
        # Create duplicates directory
        dup_dir = file_context.filepath.parent / DUPLICATES_DIR
        dup_dir.mkdir(parents=True, exist_ok=True)
        
        # Use original filename
        export_path = dup_dir / file_context.filename
        
        # Add detailed guidance
        conflict_df['_GUIDANCE'] = conflict_df['_conflict_type'].apply(
            self._get_conflict_guidance
        )
        
        # Sort by business key and conflict type
        if '_business_key' in conflict_df.columns:
            conflict_df = conflict_df.sort_values(by=['_business_key', '_conflict_type'])
        
        # Save with guidance
        conflict_df.to_csv(export_path, index=False)
        
        logger.warning(f"Exported {len(conflict_df)} conflicts to {export_path}")
        logger.info(f"Review file: {export_path}")

    def _get_conflict_guidance(self, conflict_type: str) -> str:
        """Generate specific guidance per conflict type"""
        guidance = {
            'EXACT_DUPLICATE': 
                "ACTION: Delete all but one identical row. These are 100% identical duplicates.",
                
            'BUSINESS_KEY_CONFLICT': 
                "DECISION REQUIRED: Different data for same business key. "
                "Keep only one version per key. Delete others or merge data.",
                
            'EXACT_AND_BUSINESS_CONFLICT':
                "CRITICAL: Both exact and business key duplicates exist. "
                "First resolve exact duplicates, then business key conflicts."
        }
        return guidance.get(conflict_type, "Manual review required")

    def _process_duplicate_file(self, file_context: FileContext) -> bool:
        """Process a cleaned duplicate file"""
        try:
            logger.info(f"Processing cleaned duplicates: {file_context.filename}")
            
            # Load duplicate file
            df = self.file_processor.load_file(
                file_context.filepath,
                file_context.start_row,
                file_context.start_col
            )
            
            if df.empty:
                logger.warning(f"Cleaned duplicate file is empty: {file_context.filename}")
                return True
                
            # Remove conflict resolution columns
            conflict_cols = ['_conflict_type', '_GUIDANCE', '_business_key']
            df = df.drop(columns=conflict_cols, errors='ignore')
            
            # Process as unique data
            return self._process_unique_data(df, file_context)
            
        except Exception as e:
            logger.error(f"Error processing duplicate file: {e}")
            return False

    def _process_unique_data(self, clean_df: pd.DataFrame, file_context: FileContext) -> bool:
        """Process only conflict-free rows"""
        # Load mapping and validate
        mapping = pd.read_csv(file_context.mapping_filepath)
        is_valid, errors = self.validator.validate_dataframe(clean_df, mapping, file_context.filename)
        
        for error in errors:
            logger.error(error)
        if not is_valid:
            return False
        
        # Apply transformations
        clean_df = self._apply_transformations(clean_df, mapping)
        
        # Add metadata columns
        clean_df['loaded_timestamp'] = datetime.now()
        clean_df['source_filename'] = file_context.filename
        clean_df['operation'] = 'I'
        
        # Calculate hashes with custom exclusions
        hash_exclude = set(file_context.hash_exclude_columns)
        clean_df['content_hash'] = self.file_processor.calculate_row_hashes(clean_df, hash_exclude)
        
        # Add extracted date if specified and column doesn't exist
        if (file_context.date_from_filename_col_name and 
            file_context.extracted_timestamp_str and
            file_context.date_from_filename_col_name not in clean_df.columns):
            try:
                extracted_dt = datetime.strptime(
                    file_context.extracted_timestamp_str, 
                    next(rule.date_format for rule in self.processing_rules 
                         if rule.base_name == file_context.target_table)
                )
                clean_df[file_context.date_from_filename_col_name] = extracted_dt
            except Exception:
                clean_df[file_context.date_from_filename_col_name] = None
        
        # Apply mode-specific processing
        if file_context.mode == "audit":
            return self._process_audit_mode(clean_df, file_context)
        elif file_context.mode == "cancel_and_replace":
            return self._process_cancel_replace_mode(clean_df, file_context)
        else:
            return self._load_data_to_db(clean_df, file_context.target_table)
    
    def _apply_transformations(self, df: pd.DataFrame, mapping: pd.DataFrame) -> pd.DataFrame:
        """Apply column mapping and transformations."""
        # Create mapping dictionary for columns to include
        column_mapping = {}
        for _, row in mapping.iterrows():
            if row['LoadFlag'] == 'Y' and row['RawColumn'] in df.columns:
                column_mapping[row['RawColumn']] = row['TargetColumn']
        
        # Rename columns
        df = df.rename(columns=column_mapping)
        
        # Select only columns that are marked for loading
        columns_to_keep = [col for col in mapping[mapping['LoadFlag'] == 'Y']['TargetColumn'] if col in df.columns]
        return df[columns_to_keep]

    def _process_audit_mode(self, df: pd.DataFrame, file_context: FileContext) -> bool:
        """Process file in audit mode with change detection"""
        # Get existing hashes from DB
        existing_hashes = self.db_manager.get_existing_hashes(
            file_context.target_table, 
            file_context.filename
        )
        
        # Filter to only new/changed rows
        new_rows = df[~df['content_hash'].isin(existing_hashes)]
        
        if new_rows.empty:
            logger.info(f"No changes detected: {file_context.filename}")
            return True
            
        logger.info(f"Loading {len(new_rows)} changed rows")
        return self._load_data_to_db(new_rows, file_context.target_table)

    def _process_cancel_replace_mode(self, df: pd.DataFrame, file_context: FileContext) -> bool:
        """Process file in cancel-and-replace mode with change detection"""
        try:
            with self.db_manager.get_connection() as conn:
                # Delete all existing records for this filename
                deleted_count = self.db_manager.delete_by_source_filename(
                    conn,
                    file_context.target_table,
                    file_context.filename
                )
                logger.info(f"Deleted {deleted_count} previous records for {file_context.filename}")
                
                # Insert all new records
                return self._load_data_to_db(df, file_context.target_table, conn)
        except Exception as e:
            logger.error(f"Cancel/replace failed: {e}")
            return False
    
    def _load_data_to_db(self, df: pd.DataFrame, target_table: str, conn=None) -> bool:
        """Load DataFrame to database"""
        try:
            use_external_conn = bool(conn)
            if not conn:
                conn = self.db_manager.get_connection().__enter__()
            
            with conn.cursor() as cursor:
                columns = [c for c in df.columns if c not in RESERVED_COLUMNS]
                placeholders = ", ".join(["%s"] * len(columns))
                
                # Prepare insert query
                query = sql.SQL("""
                    INSERT INTO {} ({})
                    VALUES ({})
                """).format(
                    sql.Identifier(target_table),
                    sql.SQL(", ").join(map(sql.Identifier, columns)),
                    sql.SQL(placeholders)
                )
                
                # Insert in batches
                values = [tuple(row) for row in df[columns].itertuples(index=False)]
                for i in range(0, len(values), self.config.batch_size):
                    batch = values[i:i+self.config.batch_size]
                    execute_values(cursor, query, batch)
                
                conn.commit()
                logger.info(f"Loaded {len(df)} rows to {target_table}")
                return True
        except Exception as e:
            logger.error(f"Database load failed: {e}")
            return False
        finally:
            if not use_external_conn and conn:
                conn.close()

def create_sample_configs():
    """Create sample configuration files."""
    os.makedirs("rules", exist_ok=True)
    os.makedirs("sales_data", exist_ok=True)
    os.makedirs("inventory_data", exist_ok=True)
    os.makedirs("weekly_data", exist_ok=True)
    
    # Global Config
    global_config = {
        "batch_size": 1000,
        "max_connections": 5,
        "min_connections": 1,
        "retry_attempts": 3,
        "enable_progress_tracking": True,
        "enable_data_validation": True,
        "timestamp_tolerance_seconds": 1.0,
        "global_hash_exclude_columns": [],
        "teams_webhook": "https://yourcompany.webhook.office.com/...",  # REPLACE WITH YOUR WEBHOOK
        "lock_timeout": 3600
    }
    with open(GLOBAL_CONFIG_FILE, 'w') as f:
        yaml.dump(global_config, f)
    
    # Sales Rule
    sales_rule = {
        "directory": "./sales_data",
        "file_pattern": "^sales_report_(\\d{8})\\.csv$",
        "date_format": "%Y%m%d",
        "date_from_filename_col_name": "report_date",
        "start_row": 0,
        "start_col": 0,
        "mode": "cancel_and_replace",
        "hash_exclude_columns": []
    }
    with open("rules/sales_rule.yaml", 'w') as f:
        yaml.dump(sales_rule, f)
    
    # Create sample sales file
    sales_data = pd.DataFrame({
        'Date': ['2023-01-01', '2023-01-01', '2023-01-01', '2023-01-01'],
        'Product': ['Laptop', 'Laptop', 'Laptop', 'Mouse'],
        'Amount': [1200.50, 1200.50, 1100.00, 25.00]
    })
    sales_data.to_csv("sales_data/sales_report_20230101.csv", index=False)
    
    # Inventory Rule
    inventory_rule = {
        "directory": "./inventory_data",
        "file_pattern": "^inventory_(\\d{4}-\\d{2}-\\d{2})\\.xlsx$",
        "date_format": "%Y-%m-%d",
        "start_row": 0,
        "start_col": 0,
        "mode": "audit",
        "hash_exclude_columns": []
    }
    with open("rules/inventory_rule.yaml", 'w') as f:
        yaml.dump(inventory_rule, f)
    
    # Create sample inventory file
    inventory_data = pd.DataFrame({
        'Item_ID': [101, 102],
        'Item_Name': ['Widget', 'Gadget'],
        'Stock': [500, 750]
    })
    inventory_data.to_excel("inventory_data/inventory_2023-01-01.xlsx", index=False)
    
    # Weekly Data Rule
    weekly_rule = {
        "directory": "./weekly_data",
        "file_pattern": "^weekly_report_(\\d{8})\\.csv$",
        "date_format": "%Y%m%d",
        "date_from_filename_col_name": "extract_date",
        "start_row": 0,
        "start_col": 0,
        "mode": "cancel_and_replace",
        "hash_exclude_columns": ["extract_date"]
    }
    with open("rules/weekly_rule.yaml", 'w') as f:
        yaml.dump(weekly_rule, f)
    
    # Create sample weekly files
    week1_data = pd.DataFrame({
        'ID': [1, 2, 3],
        'Name': ['ItemA', 'ItemB', 'ItemC'],
        'Value': [100, 200, 300]
    })
    week1_data.to_csv("weekly_data/weekly_report_20230604.csv", index=False)
    
    week2_data = pd.DataFrame({
        'ID': [1, 2, 3],
        'Name': ['ItemA', 'ItemB', 'ItemC'],
        'Value': [100, 200, 350]
    })
    week2_data.to_csv("weekly_data/weekly_report_20230611.csv", index=False)
    
    print("Sample configuration files and data created")

if __name__ == "__main__":
    # Clean up stale lock file if exists
    if os.path.exists(LOCK_FILE):
        lock_age = time.time() - os.path.getmtime(LOCK_FILE)
        if lock_age > 3600:  # 1 hour threshold
            os.remove(LOCK_FILE)
            print("Removed stale lock file")
    
    create_sample_configs()
    
    # Database configuration - REPLACE WITH YOUR ACTUAL CREDENTIALS
    db_config = {
        "dbname": "your_db",
        "user": "your_user",
        "password": "your_password",
        "host": "localhost",
        "port": 5432
    }
    
    try:
        loader = PostgresLoader(
            db_config=db_config,
            global_config_file=GLOBAL_CONFIG_FILE,
            rules_folder_path='rules'
        )
        
        result = loader.process_files()
        print(f"Processed: {result['processed']}, Failed: {result['failed']}")
        
    except Exception as e:
        print(f"Processing failed: {e}")
        print("Check processing.log for details")