<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PostgreSQL Data Loader - Complete User Guide</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        }
        
        body {
            background-color: #f5f7fa;
            color: #333;
            line-height: 1.6;
            padding: 20px;
            max-width: 1200px;
            margin: 0 auto;
        }
        
        .container {
            background-color: white;
            border-radius: 10px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.1);
            padding: 30px;
        }
        
        h1 {
            color: #2c3e50;
            border-bottom: 3px solid #3498db;
            padding-bottom: 15px;
            margin-bottom: 30px;
        }
        
        h2 {
            color: #2980b9;
            margin-top: 40px;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 2px solid #ecf0f1;
        }
        
        h3 {
            color: #34495e;
            margin-top: 25px;
            margin-bottom: 15px;
        }
        
        .toc {
            background-color: #f8f9fa;
            border-left: 4px solid #3498db;
            padding: 20px;
            margin: 25px 0;
            border-radius: 5px;
        }
        
        .toc ul {
            list-style-type: none;
            padding-left: 10px;
        }
        
        .toc li {
            margin: 8px 0;
        }
        
        .toc a {
            color: #2980b9;
            text-decoration: none;
            transition: color 0.3s;
        }
        
        .toc a:hover {
            color: #1a5276;
            text-decoration: underline;
        }
        
        .parameter-table {
            width: 100%;
            border-collapse: collapse;
            margin: 25px 0;
            box-shadow: 0 0 10px rgba(0,0,0,0.05);
        }
        
        .parameter-table thead tr {
            background-color: #3498db;
            color: white;
            text-align: left;
        }
        
        .parameter-table th,
        .parameter-table td {
            padding: 12px 15px;
            border: 1px solid #ddd;
        }
        
        .parameter-table tbody tr {
            border-bottom: 1px solid #ddd;
        }
        
        .parameter-table tbody tr:nth-of-type(even) {
            background-color: #f8f9fa;
        }
        
        .parameter-table tbody tr:hover {
            background-color: #e8f4fc;
        }
        
        .code-block {
            background-color: #2c3e50;
            color: #ecf0f1;
            padding: 15px;
            border-radius: 5px;
            margin: 20px 0;
            font-family: 'Courier New', monospace;
            overflow-x: auto;
        }
        
        .mode-comparison {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin: 25px 0;
        }
        
        .mode-card {
            background-color: white;
            border: 1px solid #e1e8ed;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 3px 10px rgba(0,0,0,0.08);
            transition: transform 0.3s;
        }
        
        .mode-card:hover {
            transform: translateY(-5px);
            box-shadow: 0 5px 15px rgba(0,0,0,0.1);
        }
        
        .mode-header {
            display: flex;
            align-items: center;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 2px solid #ecf0f1;
        }
        
        .mode-icon {
            width: 40px;
            height: 40px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            margin-right: 15px;
            color: white;
            font-weight: bold;
        }
        
        .mode-insert .mode-icon { background-color: #27ae60; }
        .mode-cancel .mode-icon { background-color: #e74c3c; }
        .mode-audit .mode-icon { background-color: #f39c12; }
        .mode-smart .mode-icon { background-color: #8e44ad; }
        
        .mode-title {
            font-size: 18px;
            font-weight: bold;
            color: #2c3e50;
        }
        
        .mode-badge {
            display: inline-block;
            padding: 4px 8px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: bold;
            margin: 5px;
        }
        
        .badge-green { background-color: #d5f4e6; color: #27ae60; }
        .badge-red { background-color: #fadbd8; color: #e74c3c; }
        .badge-yellow { background-color: #fef9e7; color: #f39c12; }
        .badge-blue { background-color: #ebf5fb; color: #3498db; }
        .badge-purple { background-color: #f4ecf7; color: #8e44ad; }
        
        .step-list {
            list-style-type: none;
            padding-left: 0;
        }
        
        .step-list li {
            counter-increment: step-counter;
            margin-bottom: 15px;
            padding-left: 40px;
            position: relative;
        }
        
        .step-list li:before {
            content: counter(step-counter);
            background-color: #3498db;
            color: white;
            border-radius: 50%;
            width: 25px;
            height: 25px;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            position: absolute;
            left: 0;
            top: 0;
            font-size: 14px;
            font-weight: bold;
        }
        
        .color-box {
            display: inline-block;
            width: 20px;
            height: 20px;
            border-radius: 3px;
            margin-right: 10px;
            vertical-align: middle;
            border: 1px solid #ddd;
        }
        
        .footer {
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid #ecf0f1;
            color: #7f8c8d;
            font-size: 14px;
        }
        
        @media (max-width: 768px) {
            body {
                padding: 10px;
            }
            
            .container {
                padding: 15px;
            }
            
            .parameter-table {
                font-size: 14px;
            }
            
            .parameter-table th,
            .parameter-table td {
                padding: 8px 10px;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>PostgreSQL Data Loader - Complete User Guide</h1>
        
        <div class="toc">
            <h3>Table of Contents</h3>
            <ul>
                <li><a href="#installation">1. Installation & Setup</a></li>
                <li><a href="#structure">2. Directory Structure</a></li>
                <li><a href="#configuration">3. Configuration</a></li>
                <li><a href="#excel">4. Multi-Sheet Excel</a></li>
                <li><a href="#modes">5. Processing Modes</a></li>
                <li><a href="#logging">6. Enhanced Logging with Colors</a></li>
                <li><a href="#errors">7. Error Recovery</a></li>
                <li><a href="#reference">8. Error Reference</a></li>
                <li><a href="#troubleshooting">9. Troubleshooting</a></li>
                <li><a href="#best-practices">10. Best Practices</a></li>
                <li><a href="#commands">11. Command Reference</a></li>
            </ul>
        </div>
        
        <h2 id="installation">1. Installation & Setup</h2>
        
        <h3>Quick Installation</h3>
        <div class="code-block">
pip install pandas psycopg2-binary pyyaml numpy openpyxl<br><br>
mkdir -p rules inputs/sales_data inputs/inventory_data<br>
mkdir -p duplicates/to_process duplicates/processed<br>
mkdir -p format_conflict/to_process format_conflict/processed<br>
mkdir -p failed_rows/to_process failed_rows/processed<br>
mkdir -p logs<br><br>
python loader_script.py
        </div>
        
        <h2 id="structure">2. Directory Structure</h2>
        <p>Organized structure for easy management:</p>
        <div class="code-block">
data_loader/<br>
├── loader_script.py<br>
├── global_loader_config.yaml<br>
├── processing_progress.json<br>
├── loader.lock<br>
├── rules/<br>
├── inputs/<br>
├── duplicates/<br>
├── format_conflict/<br>
├── failed_rows/<br>
└── logs/
        </div>
        
        <h2 id="configuration">3. Configuration</h2>
        
        <h3>Global Configuration Parameters</h3>
        <table class="parameter-table">
            <thead>
                <tr>
                    <th>Parameter</th>
                    <th>Description</th>
                    <th>Default</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><strong>dbname</strong></td>
                    <td>PostgreSQL database name</td>
                    <td>"your_database_name"</td>
                </tr>
                <tr>
                    <td><strong>user</strong></td>
                    <td>PostgreSQL username</td>
                    <td>"your_username"</td>
                </tr>
                <tr>
                    <td><strong>password</strong></td>
                    <td>PostgreSQL password</td>
                    <td>"your_password"</td>
                </tr>
                <tr>
                    <td><strong>host</strong></td>
                    <td>PostgreSQL server host</td>
                    <td>"localhost"</td>
                </tr>
                <tr>
                    <td><strong>port</strong></td>
                    <td>PostgreSQL server port</td>
                    <td>5432</td>
                </tr>
                <tr>
                    <td><strong>batch_size</strong></td>
                    <td>Rows processed per batch</td>
                    <td>1000</td>
                </tr>
                <tr>
                    <td><strong>enable_progress_tracking</strong></td>
                    <td>Track processed files</td>
                    <td>true</td>
                </tr>
                <tr>
                    <td><strong>delete_files</strong></td>
                    <td>Delete source after processing</td>
                    <td>"N"</td>
                </tr>
                <tr>
                    <td><strong>lock_timeout</strong></td>
                    <td>Lock timeout in seconds</td>
                    <td>3600</td>
                </tr>
                <tr>
                    <td><strong>retry_on_deadlock</strong></td>
                    <td>Retry on database deadlock</td>
                    <td>true</td>
                </tr>
            </tbody>
        </table>
        
        <h3>Processing Rule Parameters</h3>
        <table class="parameter-table">
            <thead>
                <tr>
                    <th>Parameter</th>
                    <th>Description</th>
                    <th>Example</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><strong>base_name</strong></td>
                    <td>Target table name</td>
                    <td>"sales"</td>
                </tr>
                <tr>
                    <td><strong>directory</strong></td>
                    <td>Source directory</td>
                    <td>"inputs/sales_data"</td>
                </tr>
                <tr>
                    <td><strong>file_pattern</strong></td>
                    <td>Regex pattern for files</td>
                    <td>"sales_\d{8}\.csv"</td>
                </tr>
                <tr>
                    <td><strong>mode</strong></td>
                    <td>Processing mode</td>
                    <td>"insert"</td>
                </tr>
                <tr>
                    <td><strong>search_subdirectories</strong></td>
                    <td>Search in subdirectories</td>
                    <td>true</td>
                </tr>
                <tr>
                    <td><strong>skip_subdirectories</strong></td>
                    <td>Subdirectories to skip</td>
                    <td>["archive", "temp"]</td>
                </tr>
                <tr>
                    <td><strong>skip_file_patterns</strong></td>
                    <td>File patterns to skip</td>
                    <td>[".*test.*"]</td>
                </tr>
                <tr>
                    <td><strong>mapping_file</strong></td>
                    <td>Mapping file path</td>
                    <td>"rules/sales_mapping.csv"</td>
                </tr>
            </tbody>
        </table>
        
        <h2 id="modes">5. Processing Modes</h2>
        
        <div class="mode-comparison">
            <div class="mode-card mode-insert">
                <div class="mode-header">
                    <div class="mode-icon">I</div>
                    <div class="mode-title">INSERT Mode</div>
                </div>
                <p><span class="mode-badge badge-green">Duplicates: EXPORTED</span></p>
                <p><span class="mode-badge badge-yellow">Performance: STANDARD</span></p>
                <p><strong>Use Case:</strong> Initial load, new data</p>
                <h4>Process:</h4>
                <ol class="step-list">
                    <li>Scan inputs/ directory</li>
                    <li>Check progress tracking</li>
                    <li>Load file data</li>
                    <li>Detect duplicates</li>
                    <li>Export duplicates to duplicates/</li>
                    <li>Insert unique rows</li>
                </ol>
            </div>
            
            <div class="mode-card mode-cancel">
                <div class="mode-header">
                    <div class="mode-icon">C</div>
                    <div class="mode-title">CANCEL & REPLACE</div>
                </div>
                <p><span class="mode-badge badge-red">Duplicates: N/A</span></p>
                <p><span class="mode-badge badge-red">Performance: HEAVY</span></p>
                <p><strong>Use Case:</strong> Corrections, full replacement</p>
                <h4>Process:</h4>
                <ol class="step-list">
                    <li>Find existing rows with same filename</li>
                    <li>DELETE all existing rows</li>
                    <li>Load new data</li>
                    <li>Insert all new rows</li>
                    <li>Clean up source file (optional)</li>
                </ol>
            </div>
            
            <div class="mode-card mode-audit">
                <div class="mode-header">
                    <div class="mode-icon">A</div>
                    <div class="mode-title">AUDIT Mode</div>
                </div>
                <p><span class="mode-badge badge-green">Duplicates: EXPORTED</span></p>
                <p><span class="mode-badge badge-yellow">Performance: GOOD</span></p>
                <p><strong>Use Case:</strong> Rarely modified files</p>
                <h4>Process:</h4>
                <ol class="step-list">
                    <li>Check file timestamp vs database</li>
                    <li>If similar → SKIP file completely</li>
                    <li>If different → Process like INSERT</li>
                    <li>Log skip/process decision</li>
                </ol>
            </div>
            
            <div class="mode-card mode-smart">
                <div class="mode-header">
                    <div class="mode-icon">S</div>
                    <div class="mode-title">SMART AUDIT</div>
                </div>
                <p><span class="mode-badge badge-red">Duplicates: SILENT</span></p>
                <p><span class="mode-badge badge-green">Performance: EXCELLENT</span></p>
                <p><strong>Use Case:</strong> Daily loads, stable data</p>
                <h4>Process:</h4>
                <ol class="step-list">
                    <li>Check file hash vs progress</li>
                    <li>If unchanged → SKIP completely</li>
                    <li>Calculate row-level hashes</li>
                    <li>Compare with all existing hashes</li>
                    <li>Insert only NEW rows</li>
                    <li>Silently ignore duplicates</li>
                </ol>
            </div>
        </div>
        
        <h2 id="logging">6. Enhanced Logging with Colors</h2>
        
        <table class="parameter-table">
            <thead>
                <tr>
                    <th>Log Level</th>
                    <th>Color</th>
                    <th>ANSI Code</th>
                    <th>Purpose</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><strong>DEBUG</strong></td>
                    <td><span class="color-box" style="background-color: #00cccc;"></span> Cyan</td>
                    <td>\033[36m</td>
                    <td>Detailed debugging information</td>
                </tr>
                <tr>
                    <td><strong>INFO</strong></td>
                    <td><span class="color-box" style="background-color: #ffffff; border: 1px solid #ccc;"></span> White</td>
                    <td>\033[37m</td>
                    <td>Normal informational messages</td>
                </tr>
                <tr>
                    <td><strong>WARNING</strong></td>
                    <td><span class="color-box" style="background-color: #ffcc00;"></span> Yellow</td>
                    <td>\033[33m</td>
                    <td>Warnings that need review</td>
                </tr>
                <tr>
                    <td><strong>ERROR</strong></td>
                    <td><span class="color-box" style="background-color: #ff3333;"></span> Red</td>
                    <td>\033[31m</td>
                    <td>Errors requiring immediate action</td>
                </tr>
                <tr>
                    <td><strong>CRITICAL</strong></td>
                    <td><span class="color-box" style="background-color: #990000; color: white;">C</span> Red bg/White text</td>
                    <td>\033[41m\033[37m</td>
                    <td>Critical errors blocking processing</td>
                </tr>
            </tbody>
        </table>
        
        <h3>Example Log Output</h3>
        <div class="code-block" style="background-color: #1a1a1a;">
<span style="color: #ffffff;">2024-01-15 10:30:45 - INFO - Database connection successful</span><br>
<span style="color: #ffcc00;">2024-01-15 10:30:46 - WARNING - Found 5 duplicate rows</span><br>
<span style="color: #ff3333;">2024-01-15 10:30:47 - ERROR - Database connection lost</span><br>
<span style="background-color: #990000; color: white;">2024-01-15 10:30:48 - CRITICAL - Cannot create database table</span>
        </div>
        
        <h2 id="commands">11. Command Reference</h2>
        
        <h3>Basic Usage</h3>
        <div class="code-block">
# Standard processing<br>
python loader_script.py<br><br>
# Test database connection<br>
python loader_script.py --test-connection<br><br>
# Run with file deletion<br>
python loader_script.py --delete-files Y<br><br>
# Create sample configuration<br>
python loader_script.py --setup
        </div>
        
        <h3>Utility Commands</h3>
        <div class="code-block">
# View logs in real-time<br>
tail -f logs/processing_latest.log<br><br>
# Check directory structure<br>
find . -type d -name "inputs" -o -name "rules" | sort<br><br>
# Remove stale lock file<br>
rm -f loader.lock<br><br>
# Monitor SMART AUDIT statistics<br>
grep -r "SMART AUDIT:" logs/
        </div>
        
        <div class="footer">
            <p><strong>PostgreSQL Data Loader - Complete User Guide</strong></p>
            <p>Version 2.0 | Last Updated: January 2024</p>
            <p>This guide covers all features including SMART AUDIT mode, colored logging, and comprehensive error handling.</p>
        </div>
    </div>
</body>
</html>