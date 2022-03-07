# Databricks notebook source
# MAGIC %md
# MAGIC ## Setup ODBC and install brickster
# MAGIC brickster will allow us to create/manage the dbsql endpoint programmatically within R

# COMMAND ----------

# MAGIC %sh
# MAGIC sh init-script.sh

# COMMAND ----------

install.packages(c("httr2", "odbc"))
install.packages("../brickster_0.1.0.tar.gz")

# COMMAND ----------

library(brickster)
library(tidyverse)

# token is always present in notebook env
token <- "XXXXXXXXXX"
host <- "https://e2-demo-tokyo.cloud.databricks.com/"

# COMMAND ----------

endpoint <- db_sql_endpoint_create(
      name = "r-odbc",
      cluster_size = "2X-Small", 
      host = host,
      token = token, 
      enable_serverless_compute = FALSE
    )

# COMMAND ----------

tryCatch(
  {
    endpoint <- db_sql_endpoint_create(
      name = "r-odbc",
      cluster_size = "2X-Small", 
      host = host,
      token = token
    )
  },
  error = function(cond) {
    message("endpoint likely exists aleady")
  }
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Connect via ODBC

# COMMAND ----------

# get endpoint ID
endpoints <- brickster::db_sql_endpoint_list(host = host, token = token)$endpoints
endpoint_id <- map_dfr(endpoints, ~data.frame(name = .x$name, id = .x$id)) %>%
  filter(name == "r-odbc") %>%
  pluck("id")

# COMMAND ----------

# ensure endpoint is active
# for cluster you can use brickster::get_and_start_cluster
endpoint <- db_sql_endpoint_get(id = endpoint_id, host = host, token = token)

if (endpoint$state != "RUNNING") {
  db_sql_endpoint_start(id = endpoint$id, host = host, token = token)
}

endpoint_state <- db_sql_endpoint_get(id = endpoint$id, host = host, token = token)$state
while (endpoint_state != "RUNNING") {
  Sys.sleep(5)
}

# COMMAND ----------

library(DBI)
conn <- DBI::dbConnect(
  odbc::odbc(),
  dsn = "Databricks",
  Host = endpoint$odbc_params$hostname, 
  Port = endpoint$odbc_params$port,
  SparkServerType = 3,
  Schema = "default",
  ThriftTransport = 2, 
  SSL = 1,
  AuthMech = 3,
  UID = "token",
  PWD = "XXXXXXXXXXXXXXXX", 
  HTTPPath = endpoint$odbc_params$path
)

# COMMAND ----------

dbListTables(conn)

# COMMAND ----------

tbl(conn, "diamonds")

# COMMAND ----------


