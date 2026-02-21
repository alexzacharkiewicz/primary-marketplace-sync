# Install these if you haven't: 
#install.packages(c("DBI", "RPostgres"))
#install.packages(c("DBI", "RMariaDB"))

# Load the library
library(dplyr)
library(RMariaDB)
library(DBI)
library(RPostgres)
library(here)

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
con <- connect_to_db("uat_postgres")
con2 <- connect_to_db("uat_mysql")


# 2. Define your query
query1 <- "SELECT u.id, u.email FROM users u"

# 3. Execute the query and fetch into a DataFrame
df_users <- dbGetQuery(con, query1)

# 2. Define your query
# MySQL uses backticks (`) for identifiers if they are reserved words, 
# but standard SQL usually works fine.
query2 <- "SELECT m.id, m.email FROM members m"

# 3. Execute and fetch into a DataFrame
df_members <- dbGetQuery(con2, query2)

# 4. Clean up: Close the connection
dbDisconnect(con2)

# 2. Clean the email columns (Optional but recommended)
# This handles casing issues or accidental trailing spaces
df_users <- df_users %>%
  mutate(email = as.character(email))
df_members$email <- trimws(tolower(df_members$email))
df_users$email <- trimws(tolower(df_users$email))
df_members <- df_members %>%
  rename(member_id = id)

# 3. Perform a Left Join
# This keeps everything in the member list and adds user_ids where the email matches
mapped_data <- df_users %>%
  left_join(df_members %>% select(email, member_id), by = "email")

# 4. Write your final R dataframe to a temporary table in Postgres
# This table will vanish automatically when you disconnect
dbWriteTable(con, "temp_mapping", mapped_data, temporary = TRUE, overwrite = TRUE)

# 5. Run a single SQL UPDATE statement joining the two tables
# This maps 'member_id' from the temp table to 'hn_member_id' in your users table
update_query <- "
  UPDATE users u
  SET hn_member_id = t.member_id
  FROM temp_mapping t
  WHERE u.id = t.id;
"

# 6. Execute the update
dbExecute(con, update_query)

# 7. Pull a summary of the update results
validation_query <- "
  SELECT 
    COUNT(*) AS total_users,
    COUNT(hn_member_id) AS matched_users,
    COUNT(*) - COUNT(hn_member_id) AS missing_ids
  FROM users;
"

stats <- dbGetQuery(con, validation_query)

# 8. Print a friendly summary
cat("--- Update Results ---\n")
cat("Total Users in DB:  ", stats$total_users, "\n")
cat("Successfully Mapped:", stats$matched_users, "\n")
cat("Still Missing ID:  ", stats$missing_ids, "\n")

# 9. Preview the newly mapped data
preview <- dbGetQuery(con, "SELECT id, email, hn_member_id FROM users WHERE hn_member_id IS NOT NULL LIMIT 10")
print(preview)



