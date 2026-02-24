
# Environment Configuration Guide

This project uses a dual-layer configuration system to manage database connections across **QA** and **Production** environments while keeping sensitive credentials secure.

## 1. Prerequisites

Before running any scripts, ensure you have the following R packages installed:

```r
install.packages(c("DBI", "RPostgres", "RMariaDB", "config", "here"))

```

---

## 2. Local Secrets (`.Renviron`)

The `.Renviron` file stores your actual passwords. **This file is git-ignored and should never be committed to the repository.**

1. Create a file named `.Renviron` in the project root.
2. Add your credentials using the following format (**Note:** Do not use spaces around the `=` sign):

```text
# PostgreSQL Credentials
QA_PSQL_PASS="your_qa_password_here"
PROD_PSQL_PASS="your_prod_password_here"

# MySQL Credentials
QA_MYSQL_PASS="your_mysql_password_here"

# Important: Ensure there is a blank line at the end of this file.

```

> [!IMPORTANT]
> You must **restart R** (`Ctrl + Shift + F10` in RStudio) after editing this file for changes to take effect.

---

## 3. Environment Metadata (`config.yml`)

The `config.yml` defines the non-sensitive connection details. This file **is** committed to the repository to help teammates sync their settings.

| Key | Description |
| --- | --- |
| `db_type` | The database engine: `postgres` or `mysql`. |
| `host` | The server address (e.g., `localhost` or an IP). |
| `pass_env_var` | The **name** of the variable in `.Renviron` that holds the password. |

### Example Entry:

```yaml
qa_postgres:
  db_type: "postgres"
  host: "qa-db.company.com"
  dbname: "analytics_qa"
  user: "admin_user"
  port: 5432
  pass_env_var: "QA_PSQL_PASS"

```

---

## 4. Usage in R

To connect to an environment, use the `connect_to_db()` function. This function dynamically pulls the metadata from the YAML and the secret from your environment.

```r
library(here)
source(here("R/connections.R"))

# Connect to the QA environment defined in config.yml
con <- connect_to_db("qa_postgres")

# Run a test query
result <- dbGetQuery(con, "SELECT count(*) FROM users")
print(result)

```

---

## 5. Security Checklist

* [ ] **Verify `.Renviron` is listed in `.gitignore`.** (Run `git check-ignore .Renviron` to verify).
* [ ] **Verify `config.yml` does not contain plain-text passwords.**
* [ ] **Check for Trailing Newlines:** Ensure `.Renviron` ends with an empty line.

---

### Troubleshooting

* **Error: `the condition has length > 1**`: Check your `.Renviron` for duplicate variable names.
* **Error: `Environment variable ... not found**`: Restart your R session and verify the file name is exactly `.Renviron`.

