# Databricks notebook source
# MAGIC %md
# MAGIC ## Install Brickster
# MAGIC brickster will allow us to create/manage the dbsql endpoint programmatically within R

# COMMAND ----------

install.packages(c("httr2", "odbc"))
install.packages("../brickster_0.1.0.tar.gz")

# COMMAND ----------

library(brickster)

# token is always present in notebook env
token <- spark.databricks.token
host <- "https://e2-demo-field-eng.cloud.databricks.com/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a DBSQL endpoint

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
# MAGIC ## Install ODBC

# COMMAND ----------

# get endpoint ID
library(tidyverse)
endpoints <- brickster::db_sql_endpoint_list(host = host, token = token)$endpoints
endpoint_id <- map_dfr(endpoints, ~data.frame(name = .x$name, id = .x$id)) %>%
  filter(name == "r-odbc") %>%
  pluck("id")

# COMMAND ----------

library(DBI)
conn <- DBI::dbConnect(
  odbc::odbc(),
  dsn = "Databricks",
  Host = gsub("^https://(.*)/$", "\\1", host), 
  Port = 443,
  SparkServerType = 3,
  Schema = "default",
  ThriftTransport = 2, 
  SSL = 1,
  AuthMech = 3,
  UID = "token",
  PWD = token, 
  HTTPPath = paste0("/sql/1.0/endpoints/", endpoint_id)
)
