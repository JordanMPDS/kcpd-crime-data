# Databricks notebook source
import csv
import pyspark.pandas as ps
from datetime import datetime
from sodapy import Socrata

# COMMAND ----------

client = Socrata("data.kcmo.org", None)
results = client.get_all("isbe-v4d8")
results_df = ps.DataFrame.from_records(results)

# COMMAND ----------

file_name = "isbe-v4d8" + datetime.today().strftime("%Y-%m-%d")
results_df.to_parquet(
    "/Volumes/datakc/volumes/isbe-v4d8/" + file_name, compression="snappy"
)

# COMMAND ----------

#TODO finish getting lat/long FOR EACH row
results_df['latitude'] = results_df[["location"]].iloc[1].iloc[0]["coordinates"][1]
results_df['longitude'] = results_df[["location"]].iloc[1].iloc[0]["coordinates"][0]
results_df = results_df.drop(['location',':@computed_region_kk66_ngf4',':@computed_region_k7in_q28t',':@computed_region_w4hf_t6bp',':@computed_region_qizh_zmq5',':@computed_region_vrdq_ghvi'], axis=1)

# COMMAND ----------


