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
con <- connect_to_db("prod_mysql")
con2 <- connect_to_db("prod_postgres")

# 2. Define your queries
query1 <- "SELECT DISTINCT t.user_id, u.email, u.fname, u.lname, u.created_at  
FROM boxoffice.tickets t
LEFT JOIN users u 
ON u.id = t.user_id
WHERE u.hn_member_id IS NULL AND (t.boe_event_id IS NOT NULL OR t.boe_ticket_id IS NOT NULL)"

query2 <- "SELECT m.id, m.email FROM members m"


df_members <- dbGetQuery(con, query2)
df_orphans <- dbGetQuery(con2, query1)

#sanitize email text
df_members$email <- trimws(tolower(df_members$email))
df_members <- df_members %>%
  rename(member_id = id)
df_orphans$email <- trimws(tolower(df_orphans$email))
df_orphans <- df_orphans %>%
  mutate(email = as.character(email))

#check for duplicate members after sanitizing
email_duplicates_members <- df_members %>%
  group_by(email) %>%
  filter(n() > 1) %>%
  arrange(email)

#check for duplicate orphans after sanitizing
email_duplicates_orphans <- df_orphans %>%
  group_by(email) %>%
  filter(n() > 1) %>%
  arrange(email)

# Returns cleaned emails from transactions that do not match to members
unmatched_emails <- df_orphans %>%
  anti_join(df_members, by = c("email" = "email"))

# returns member emails that match an orphan after sanitizing
cleaned_matches <- df_members %>%
  inner_join(df_orphans, by = c("email" = "email"))

#rename columns to match DB
unmatched_emails <- unmatched_emails %>% 
  rename(external_id = user_id,
         member_since = created_at)

#combine first and last name for member name
unmatched_emails$name <- paste(unmatched_emails$fname, unmatched_emails$lname)

# Delete multiple columns
unmatched_emails <- unmatched_emails %>% select(-c(fname, lname))

# 1. Upload results to a temp table
dbWriteTable(con, "temp_members_load", unmatched_emails, temporary = TRUE, overwrite = TRUE)

# 2. Execute the INSERT statement using SQL functions
insert_sql <- "
INSERT INTO dbmaster.members (
    id, email, name, phone_number, address, 
    email_verified, phone_verified, created_at, 
    external_id, updated_at, member_since
)
SELECT 
    UUID(), email, name, NULL, NULL, 
    0, 0, CURRENT_TIMESTAMP, 
    external_id, CURRENT_TIMESTAMP, member_since
FROM temp_members_load;"

dbExecute(con, insert_sql)

#close DB connections
dbDisconnect(con)
dbDisconnect(con2)