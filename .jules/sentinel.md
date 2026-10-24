## 2025-02-14 - Hardcoded Airflow Connection Credentials
**Vulnerability:** Hardcoded credentials (`forum`/`forum`) were passed via command line (`airflow connections add`) in the `docker-compose.yml` for initializing database connections.
**Learning:** Command line arguments in `docker-compose` can easily expose sensitive data and make credential rotation difficult. Airflow allows configuring connections via environment variables, which is a safer pattern.
**Prevention:** Use environment variables (e.g. `AIRFLOW_CONN_FORUM_DB`) and `.env` files rather than hardcoding passwords in init scripts or entrypoints. Ensure variables are properly passed through `docker-compose.yml` to internal services like Postgres so that they stay in sync.
