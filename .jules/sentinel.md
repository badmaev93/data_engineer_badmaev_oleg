## 2025-02-14 - Path Traversal via Unvalidated Airflow Params
**Vulnerability:** Path traversal vulnerability in Airflow parameters (`date_from`, `date_to`). The parameters were formatted directly into the filename string without validation (`f"aggregation_{date_from}_to_{date_to}.csv"`), allowing a malicious user to overwrite files elsewhere.
**Learning:** Airflow params are external user input. Never trust parameters and use them without validation when interacting with the file system.
**Prevention:** Always cast date inputs to valid Date objects or run `datetime.strptime` checks to validate them, and use `os.path.basename` when using parameters in filenames.
