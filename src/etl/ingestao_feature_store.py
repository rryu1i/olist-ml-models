# Databricks notebook source
# MAGIC %pip install tqdm
# MAGIC 
# MAGIC import datetime
# MAGIC from tqdm import tqdm
# MAGIC 
# MAGIC def import_query(path):
# MAGIC     with open(path, 'r') as open_file:
# MAGIC         return open_file.read()
# MAGIC 
# MAGIC 
# MAGIC def table_exists(database, table):
# MAGIC     count = (spark.sql(f"SHOW TABLES FROM {database}")
# MAGIC                   .filter(f"tableName = '{table}'")
# MAGIC                   .count())
# MAGIC     return count > 0
# MAGIC 
# MAGIC 
# MAGIC def date_range(dt_start, dt_stop, period='daily'):
# MAGIC     datetime_start = datetime.datetime.strptime(dt_start, '%Y-%m-%d')
# MAGIC     datetime_stop = datetime.datetime.strptime(dt_stop, '%Y-%m-%d')
# MAGIC     dates = []
# MAGIC     while datetime_start <= datetime_stop:
# MAGIC         dates.append(datetime_start.strftime("%Y-%m-%d"))
# MAGIC         datetime_start += datetime.timedelta(days=1)
# MAGIC     if period == 'daily':
# MAGIC         return dates
# MAGIC     elif period == 'monthly':
# MAGIC         return [i for i in dates if i.endswith("01")]
# MAGIC 
# MAGIC table = dbutils.widgets.get("table")
# MAGIC table_name = f"fs_vendedor_{table}"
# MAGIC database = "silver.analytics"
# MAGIC period = dbutils.widgets.get("period")
# MAGIC 
# MAGIC query = import_query(f"{table}.sql")
# MAGIC 
# MAGIC date_start = dbutils.widgets.get("date_start")
# MAGIC date_stop = dbutils.widgets.get("date_stop")
# MAGIC 
# MAGIC dates = date_range(date_start, date_stop, period)
# MAGIC 
# MAGIC print(table_name, table_exists(database, table_name))
# MAGIC print(date_start, ' ~ ', date_stop)

# COMMAND ----------

if not table_exists(database, table_name):
    print("Criando a tabela...")
    (spark.sql(query.format(date=dates.pop(0)))
          .coalesce(1)
          .write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .partitionBy("dtReference")
          .saveAsTable(f"{database}.{table_name}")
    )
    print("ok")

print("Realizando update")
for i in tqdm(dates):
    spark.sql(f"DELETE FROM {database}.{table_name} WHERE dtReference = '{i}'")
    (spark.sql(query.format(date=i))
          .coalesce(1)
          .write
          .format("delta")
          .mode("append")
          .saveAsTable(f"{database}.{table_name}"))
print("ok")
                                                      
