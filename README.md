Environment Configuration GuideThis project uses a dual-layer configuration system to manage database connections across QA and Production environments while keeping sensitive credentials secure.1. PrerequisitesBefore running any scripts, ensure you have the following R packages installed:Rinstall.packages(c("DBI", "RPostgres", "RMariaDB", "config", "here"))
2. Local Secrets (.Renviron)The .Renviron file stores your actual passwords. This file is git-ignored and should never be committed.Create a file named .Renviron in the project root.Add your credentials using the following format (no spaces around =):Plaintext# PostgreSQL Credentials
QA_PSQL_PASS="your_qa_password_here"
PROD_PSQL_PASS="your_prod_password_here"

# MySQL Credentials
QA_MYSQL_PASS="your_mysql_password_here"

# Important: Ensure there is a blank line at the end of this file.
Note: You must restart R (Ctrl+Shift+F10) after editing this file for changes to take effect.3. Environment Metadata (config.yml)The config.yml defines the non-sensitive connection details. This file is committed to the repository.KeyDescriptiondb_typeEither postgres or mysqlhostThe server addresspass_env_varThe name of the variable in .Renviron to pull the password fromExample Entry:YAMLqa_postgres:
  db_type: "postgres"
  host: "qa-db.company.com"
  dbname: "analytics_qa"
  user: "admin_user"
  port: 5432
  pass_env_var: "QA_PSQL_PASS"
4. Usage in RTo connect to an environment, use the connect_to_db() function included in the project library:Rlibrary(here)
source(here("R/connections.R"))

# Connect to QA
con <- connect_to_db("qa_postgres")

# Run a test query
dbGetQuery(con, "SELECT count(*) FROM users")
5. Security Checklist[ ] Verify .Renviron is listed in .gitignore.[ ] Verify config.yml does not contain plain-text passwords.[ ] Ensure R_CONFIG_ACTIVE is set if you want to default to a specific environment.
