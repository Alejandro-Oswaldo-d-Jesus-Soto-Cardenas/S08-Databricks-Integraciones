# Databricks notebook source
# S08 | AP4 | Databricks e Integraciones
# Autor: Alejandro Oswaldo de Jesús Soto Cárdenas

# COMMAND ----------

from pyspark.sql import functions as F

print("S08 AP4 - Databricks e Integraciones")
print("Proyecto sincronizado desde GitHub")

# COMMAND ----------

# Lectura de usuarios desde el Git Folder

ruta_csv = "/Workspace/Users/alejandro.soto.cardenas@vallegrande.edu.pe/S08-Databricks-Integraciones/data/users_dirty.csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(ruta_csv)
)

display(df.limit(10))

# COMMAND ----------

# Cantidad de registros

total_registros = df.count()

print("Total de registros:", total_registros)

# COMMAND ----------

# Distribución por país

display(
    df.groupBy("country")
      .count()
      .orderBy(F.desc("count"))
)

# COMMAND ----------

# Revisión de registros duplicados

duplicados = (
    df.groupBy("user_id")
      .count()
      .filter(F.col("count") > 1)
)

display(duplicados)

# COMMAND ----------

# Resumen de calidad de datos

print("=== RESUMEN DE CALIDAD ===")

print("Total de registros:", df.count())
print("Total de columnas:", len(df.columns))
print("Registros duplicados:", duplicados.count())

# Valores nulos por columna

nulls = df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df.columns
])

display(nulls)
