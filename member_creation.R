# Load the library
library(dplyr)
library(RMariaDB)
library(DBI)
library(RPostgres)
library(here)
library(DBUtils)

# Build DB Connection Function
connect_to_db <- function(env_name) {
  # Load the specific block from config.yml
  conf <- config::get(config = env_name, file = here("config.yml"))
  
  # Pull the actual password value using the key provided in the YAML
  db_password <- Sys.getenv(conf$pass)
  
  if (db_password == "") {
    stop(paste("Error: Environment variable", conf$pass, "not found in .Renviron"))
  }
  
  if (conf$db_type == "postgres") {
    library(RPostgres)
    con <- dbConnect(
      RPostgres::Postgres(),
      host = conf$host,
      dbname = conf$dbname,
      user = conf$user,
      password = db_password, # Uses the secret pulled above
      port = conf$port
    )
  } else if (conf$db_type == "mysql") {
    library(RMariaDB)
    con <- dbConnect(
      RMariaDB::MariaDB(),
      host = conf$host,
      dbname = conf$dbname,
      user = conf$user,
      password = db_password, # Uses the secret pulled above
      port = conf$port
    )
  } else {
    stop("Unsupported db_type. Use 'postgres' or 'mysql'.")
  }
  
  return(con)
}

# 1. Establish the connections
con <- connect_to_db("qa_mysql")

# 2. Define your queries
query1 <- "SELECT DISTINCT t.customer_email
FROM transactions t 
LEFT JOIN members m 
ON m.email = t.customer_email 
WHERE t.member_id IS NULL AND t.customer_email IS NOT NULL AND YEAR(t.`date`) >= 2025 AND m.id IS NULL"

query2 <- "SELECT m.id, m.email FROM members m"


df_members <- dbGetQuery(con, query2)
df_orphans <- dbGetQuery(con, query1)

#sanitize email text
df_members$email <- trimws(tolower(df_members$email))
df_members <- df_members %>%
  rename(member_id = id)
df_orphans$customer_email <- trimws(tolower(df_orphans$customer_email))

#check for duplicate members after sanitizing
email_duplicates_members <- df_members %>%
  group_by(email) %>%
  filter(n() > 1) %>%
  arrange(email)

# View the results
print(email_duplicates_members)

#check for duplicate orphans after sanitizing
email_duplicates_orphans <- df_orphans %>%
  group_by(customer_email) %>%
  filter(n() > 1) %>%
  arrange(customer_email)

# View the results
print(email_duplicates_orphans)

# Returns cleaned emails from transactions that do not match to members
unmatched_emails <- df_orphans %>%
  anti_join(df_members, by = c("customer_email" = "email"))

# View the results
print(unmatched_members)

# returns member emails that match an orphan after sanitizing
cleaned_matches <- df_members %>%
  inner_join(df_orphans, by = c("email" = "customer_email"))

# View the results
print(cleaned_matches)
