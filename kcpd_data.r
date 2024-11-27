# Databricks notebook source
suppressMessages(library(tidyverse))
suppressMessages(library(sparklyr))
suppressMessages(library(RSocrata))
suppressMessages(library(DBI))
suppressMessages(library(glue))
suppressMessages(library(arrow))

# COMMAND ----------

# Connect sparklyr to your spark cluster
sc <- spark_connect(method = "databricks")

# COMMAND ----------

# Read CSV from API and manipulate before saving
message("Starting data read")
results_df <- read.socrata("https://data.kcmo.org/resource/isbe-v4d8.json")
message("Data read completed")

# COMMAND ----------

# Transforming coordinate list to individual long and lat columns and binding back to original table (dropping nulls ~ 2.2%)
results_df <- results_df %>%
select(location.coordinates) %>%
filter(location.coordinates != "NULL") %>%
mutate(location.coordinates = map(location.coordinates, set_names, c("long","lat"))) %>%
unnest_wider(col = c("location.coordinates")) %>%
cbind(results_df %>% filter(location.coordinates != "NULL") %>% select(!c(location.coordinates,location.type)))

# COMMAND ----------

# Read CSV from API and write parquet to volume
file_name = glue("/Volumes/datakc/volumes/isbe-v4d8/isbe-v4d8_{Sys.Date()}")
message("Writing parquet file")
write_parquet(results_df, file_name)
message("Parquet file written")

# COMMAND ----------

# Reading parquet to spark and cleaning data
isbev4d8_spark <- spark_read_parquet(sc, file_name)
isbev4d8_spark <- isbev4d8_spark %>%
filter(!is.na(to_date)) %>%
mutate(duration = to_date - from_date)

# COMMAND ----------

# Creating table
isbev4d8_spark %>% spark_write_table('datakc.tables.kcpd_crime_data', mode = 'overwrite')

# COMMAND ----------


