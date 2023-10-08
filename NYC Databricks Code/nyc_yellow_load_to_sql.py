# Databricks notebook source
jdbc_url = f"jdbc:sqlserver://dev-sql-server-01.database.windows.net:1433;databaseName=nyc-taxi"
connection_properties = {
        "user": "username",
        "password": "password",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

def load_to_adw(df, df_table):
    df.write.jdbc(url=jdbc_url, table=df_table, mode="append", properties=connection_properties)

# COMMAND ----------

def truncate_load_to_adw(df, df_table):
    df.write.jdbc(url=jdbc_url, table=df_table, mode="overwrite", properties=connection_properties)