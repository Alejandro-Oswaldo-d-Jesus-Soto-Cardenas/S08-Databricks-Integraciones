
# Databricks notebook source
# S08 | AP4 | Databricks e Integraciones
# Autor: Alejandro Oswaldo de Jesús Soto Cárdenas

# COMMAND ----------

from pyspark.sql import functions as F

print("S08 AP4 - Databricks e Integraciones")
print("Proyecto sincronizado desde GitHub")

# COMMAND ----------

# Lectura de usuarios desde el repositorio
# La ruta se ajustará si el workspace usa otra ubicación.

ruta_csv = "../data/users_dirty.csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(ruta_csv)
)

display(df.limit(10))

# COMMAND ----------

# Cantidad de registros
print("Total de registros:", df.count())

# Distribución por país
display(
    df.groupBy("country")
      .count()
      .orderBy(F.desc("count"))
)

# COMMAND ----------

# Revisión de registros duplicados
display(
    df.groupBy("user_id")
      .count()
      .filter(F.col("count") > 1)
)