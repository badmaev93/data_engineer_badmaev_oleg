## 2024-10-24 - Hardcoded Secrets in docker-compose.yml
**Vulnerability:** Hardcoded database and admin credentials in `docker-compose.yml` (e.g. `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`, `postgres` service environment variables, and `airflow users create`).
**Learning:** Checking credentials directly into version control creates immediate risk, especially in publicly exposed codebases. `docker-compose.yml` configs should use environment variables via `.env` files.
**Prevention:** Parameterize configuration files (like `docker-compose.yml`) using `${VAR_NAME}` and provide a `.env.example` file so that sensitive values are provided strictly through the environment.
