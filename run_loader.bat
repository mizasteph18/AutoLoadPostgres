@echo off
set PYTHON=C:\Python39\python.exe
set SCRIPT=C:\ETL_Loader\postgres_loader.py
set CONFIG=C:\ETL_Loader\global_config.yaml

%PYTHON% %SCRIPT% --config %CONFIG%