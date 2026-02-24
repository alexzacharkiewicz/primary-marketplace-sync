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
query1 <- "SELECT m.id, m.email FROM members m"
query2 <- "SELECT t.id, t.member_id, t.customer_email FROM transactions t"
query3 <- "SELECT r.id, r.member_id, r.customer_email FROM reservations r"
query4 <- "SELECT i.id, i.reservation_id, i.customer_id FROM invoices i"

# 3. Execute the queries and fetch into DataFrames
df_members <- dbGetQuery(con, query1)
df_trans <- dbGetQuery(con, query2)
df_resv <- dbGetQuery(con, query3)
df_inv <- dbGetQuery(con, query4)

# 4. Clean the email columns and rename id columns for clarity
# This handles casing issues or accidental trailing spaces
df_members$email <- trimws(tolower(df_members$email))
df_members <- df_members %>%
  rename(member_id = id)
df_trans$customer_email <- trimws(tolower(df_trans$customer_email))
df_trans <- df_trans %>%
  rename(transaction_id = id)
df_resv$customer_email <- trimws(tolower(df_resv$customer_email))
df_resv <- df_resv %>%
  rename(reservation_id = id)
df_inv <- df_inv %>%
  rename(invoice_id = id)
df_inv <- df_inv %>%
  rename(member_id = customer_id)

# Check for duplicate member emails
df_members_dupes %>% count(email) %>% filter(n > 1)

# 5. Update Transactions member_id based on email
df_trans_new <- df_trans %>%
  left_join(df_members %>% select(email, correct_id = member_id), 
            by = c("customer_email" = "email")) %>%
  mutate(member_id = coalesce(correct_id, member_id)) %>%
  select(-correct_id)

# 6. Update Reservations member_id based on email
df_resv_new <- df_resv %>%
  left_join(df_members %>% select(email, correct_id = member_id), 
            by = c("customer_email" = "email")) %>%
  mutate(member_id = coalesce(correct_id, member_id)) %>%
  select(-correct_id)

# 7. Update Invoices member_id based on reservation_id
# Note: This assumes df_resv now has the updated member_ids 
df_inv_new <- df_inv %>%
  left_join(df_resv %>% select(reservation_id, correct_id = member_id), 
            by = "reservation_id") %>%
  mutate(member_id = coalesce(correct_id, member_id)) %>%
  select(-correct_id)

# Upload your cleaned DataFrames to the DB as temporary staging tables
dbWriteTable(con, "stg_trans", df_trans_new, overwrite = TRUE, temporary = TRUE)
dbWriteTable(con, "stg_resv", df_resv_new, overwrite = TRUE, temporary = TRUE)
dbWriteTable(con, "stg_inv", df_inv_new, overwrite = TRUE, temporary = TRUE)

# Update Transactions
sql_trans <- "
UPDATE transactions t
INNER JOIN stg_trans s ON t.id = s.transaction_id
SET t.member_id = s.member_id
WHERE t.member_id <=> s.member_id = 0;" # Only update if they are actually different

dbExecute(con, sql_trans)

# Update Reservations
sql_resv <- "
UPDATE reservations r
INNER JOIN stg_resv s ON r.id = s.reservation_id
SET r.member_id = s.member_id
WHERE r.member_id <=> s.member_id = 0;"

dbExecute(con, sql_resv)

# Update Invoices
sql_inv <- "
UPDATE invoices i
INNER JOIN stg_inv s ON i.id = s.invoice_id
SET i.customer_id = s.member_id
WHERE i.customer_id <=> s.member_id = 0;"

dbExecute(con, sql_inv)

# 7. Pull a summary of the update results. STILL IN PROGRESS
#validation_query <- ""

#stats <- dbGetQuery(con, validation_query)

# 8. Print a friendly summary
#cat("--- Update Results ---\n")
#cat("Total Users in DB:  ", stats$total_users, "\n")
#cat("Successfully Mapped:", stats$matched_users, "\n")
#cat("Still Missing ID:  ", stats$missing_ids, "\n")

