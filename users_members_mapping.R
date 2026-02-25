# Install these if you haven't: 
#install.packages(c("DBI", "RPostgres"))
#install.packages(c("DBI", "RMariaDB"))

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
con <- connect_to_db("prod_postgres")
con2 <- connect_to_db("prod_mysql")


# 2. Define marketplace query
query1 <- "SELECT u.id, u.email FROM users u"

# 3. Execute the query and fetch into a DataFrame
df_users <- dbGetQuery(con, query1)

# 4. Define BOE query
# MySQL uses backticks (`) for identifiers if they are reserved words, 
# but standard SQL usually works fine.
query2 <- "SELECT m.id, m.email FROM members m"

# 5. Execute and fetch into a DataFrame
df_members <- dbGetQuery(con2, query2)

# 6. Clean up: Close the BOE connection
dbDisconnect(con2)

# 7. Clean the email columns (Optional but recommended)
# This handles casing issues or accidental trailing spaces
df_users <- df_users %>%
  mutate(email = as.character(email))
df_members$email <- trimws(tolower(df_members$email))
df_users$email <- trimws(tolower(df_users$email))
df_members <- df_members %>%
  rename(member_id = id)

# check for duplicate emails after sanitizing text
email_duplicates_hn <- df_members %>%
  group_by(email) %>%
  filter(n() > 1) %>%
  arrange(email)

email_duplicates_ws <- df_users %>%
  group_by(email) %>%
  filter(n() > 1) %>%
  arrange(email)

# 8. Perform a Left Join
mapped_data <- df_users %>%
  left_join(df_members %>% select(email, member_id), by = "email")

#check for duplicates in mapped data
email_duplicates <- mapped_data %>%
  group_by(email) %>%
  filter(n() > 1) %>%
  arrange(email)

# 9. Write your final R dataframe to a temporary table in Postgres
# This table will vanish automatically when you disconnect
dbWriteTable(con, "temp_mapping", mapped_data, temporary = TRUE, overwrite = TRUE)

# 10. Run a single SQL UPDATE statement joining the two tables
# This maps 'member_id' from the temp table to 'hn_member_id' in your users table
update_query <- "
  UPDATE users u
  SET hn_member_id = t.member_id
  FROM temp_mapping t
  WHERE u.id = t.id AND t.member_id is not null and u.hn_member_id != t.member_id;
"

# 11. Execute the update
dbExecute(con, update_query)

# 12. Pull a summary of the update results
validation_query <- "
  SELECT 
    CAST(COUNT(*) AS BIGINT) AS total_users,
    CAST(COUNT(hn_member_id) AS BIGINT) AS matched_users,
    CAST(COUNT(*) - COUNT(hn_member_id) AS BIGINT) AS missing_ids
  FROM users;
"
# 6. Clean up: Close the PSQL connection
dbDisconnect(con)