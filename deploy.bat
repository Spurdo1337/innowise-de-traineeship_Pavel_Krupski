@echo off
setlocal

rem Put the source files here before running (not included in the repository):
rem   datasets\source\taxi\tripdata.parquet
rem   datasets\source\users\users.avro

if not exist "datasets\source\taxi\tripdata.parquet" (
    echo Missing datasets\source\taxi\tripdata.parquet
    exit /b 1
)
if not exist "datasets\source\users\users.avro" (
    echo Missing datasets\source\users\users.avro
    exit /b 1
)

databricks bundle validate
if errorlevel 1 exit /b 1

rem Bind objects that already exist, so that deploy updates them instead of creating them again.
call :bind_schema raw_files || exit /b 1
call :bind_schema bronze || exit /b 1
call :bind_schema silver || exit /b 1
call :bind_schema gold || exit /b 1
call :bind_schema dlt_bronze || exit /b 1
call :bind_schema dlt_silver || exit /b 1
call :bind_schema dlt_gold || exit /b 1
call :bind_volume || exit /b 1
call :bind_pipeline || exit /b 1
call :bind_job || exit /b 1

databricks bundle deploy
if errorlevel 1 exit /b 1

for /f "usebackq delims=" %%V in (`powershell -NoProfile -Command "(databricks bundle summary -o json | ConvertFrom-Json).resources.volumes.landing.volume_path"`) do set VOLUME_PATH=%%V

if "%VOLUME_PATH%"=="" (
    echo Could not read the volume path from the bundle summary.
    exit /b 1
)

echo Uploading datasets to %VOLUME_PATH%
databricks fs cp -r --overwrite .\datasets\source dbfs:%VOLUME_PATH%/source
if errorlevel 1 exit /b 1

echo Done. Start the job "medallion-pipeline" from Workflows.
exit /b 0

:bind_schema
rem %1 = schema name, also the bundle resource key
databricks schemas get workspace.%1 >nul 2>&1
if errorlevel 1 exit /b 0
echo Binding existing schema workspace.%1
databricks bundle deployment bind %1 workspace.%1
if errorlevel 1 exit /b 1
exit /b 0

:bind_volume
databricks volumes read workspace.raw_files.landing >nul 2>&1
if errorlevel 1 exit /b 0
echo Binding existing volume workspace.raw_files.landing
databricks bundle deployment bind landing workspace.raw_files.landing
if errorlevel 1 exit /b 1
exit /b 0

:bind_pipeline
set PIPE_ID=
for /f "usebackq delims=" %%I in (`powershell -NoProfile -Command "(databricks pipelines list-pipelines -o json | ConvertFrom-Json | Where-Object { $_.name -eq 'medallion_dlt' } | Select-Object -First 1).pipeline_id"`) do set PIPE_ID=%%I
if not defined PIPE_ID exit /b 0
echo Binding existing pipeline medallion_dlt (%PIPE_ID%)
databricks bundle deployment bind medallion_dlt %PIPE_ID%
if errorlevel 1 exit /b 1
exit /b 0

:bind_job
set JOB_ID=
for /f "usebackq delims=" %%I in (`powershell -NoProfile -Command "(databricks jobs list -o json | ConvertFrom-Json | Where-Object { $_.settings.name -eq 'medallion-pipeline' } | Select-Object -First 1).job_id"`) do set JOB_ID=%%I
if not defined JOB_ID exit /b 0
echo Binding existing job medallion-pipeline (%JOB_ID%)
databricks bundle deployment bind medallion_pipeline %JOB_ID%
if errorlevel 1 exit /b 1
exit /b 0
