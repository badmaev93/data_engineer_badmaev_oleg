## 2025-02-14 - Path Traversal via Unvalidated Airflow Params
**Vulnerability:** Path traversal vulnerability in Airflow parameters (`date_from`, `date_to`). The parameters were formatted directly into the filename string without validation (`f"aggregation_{date_from}_to_{date_to}.csv"`), allowing a malicious user to overwrite files elsewhere.
**Learning:** Airflow params are external user input. Never trust parameters and use them without validation when interacting with the file system.
**Prevention:** Always cast date inputs to valid Date objects or run `datetime.strptime` checks to validate them, and use `os.path.basename` when using parameters in filenames.
## 2025-02-21 - Remove Hardcoded Database Password in Init Script
**Vulnerability:** Hardcoded database password in `init-db/01-init-forum-db.sh` and Airflow connections in `docker-compose.yml`.
**Learning:** Hardcoding credentials in source files (even for initialization) exposes secrets to version control and anyone with read access to the repository.
**Prevention:** Use environment variables (e.g., `${FORUM_DB_PASSWORD:-default_value}`) in bash scripts and Docker Compose definitions. When updating credentials in one place (database init), ensure client configurations (like Airflow connections) are also updated to match.
