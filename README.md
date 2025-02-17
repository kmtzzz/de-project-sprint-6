# Sprint 6 project

### Description
This repository is intended for source code of Sprint 6 project.  

***Technologies used in implementation:***
1. S3
2. Vertica
3. Python
4. Airflow
5. Docker

### Repository structure
Inside `src` next folders exist:
- `/src/dags` - DAGs which extract data from S3 and load to Vertica STAGING data layer
- `/src/sql` - DDLs and DMLs to upload datamarts.

### Data model for DWH implementation
- Data vault
