## 2025-02-14 - Path Traversal via Unvalidated Airflow Params
**Vulnerability:** Path traversal vulnerability in Airflow parameters (`date_from`, `date_to`). The parameters were formatted directly into the filename string without validation (`f"aggregation_{date_from}_to_{date_to}.csv"`), allowing a malicious user to overwrite files elsewhere.
**Learning:** Airflow params are external user input. Never trust parameters and use them without validation when interacting with the file system.
**Prevention:** Always cast date inputs to valid Date objects or run `datetime.strptime` checks to validate them, and use `os.path.basename` when using parameters in filenames.

## 2026-10-24 - Hardcoded Secrets in Docker Compose
**Vulnerability:** Hardcoded Postgres passwords in docker-compose.yml.
**Learning:** Default configuration parameters in docker-compose often contain hardcoded secrets which poses a security risk if committed to a public repository or deployed.
**Prevention:** Replace hardcoded secrets with environment variable substitutions (e.g., ${POSTGRES_PASSWORD:-default_value}) to allow overrides via .env files while maintaining functionality.
