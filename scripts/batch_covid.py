from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Crear sesión
spark = SparkSession.builder \
    .appName("Analisis COVID Colombia") \
    .getOrCreate()

# Cargar CSV
df = spark.read.csv(
    "/home/vboxuser/tarea3_spark/data/covid19.csv",
    header=True,
    inferSchema=True
)

print("=== COLUMNAS ===")
df.printSchema()

print("=== DATOS ===")
df.show(5)

# Limpiar datos
df_clean = df.select("Nombre departamento", "Estado") \
             .dropna(subset=["Nombre departamento", "Estado"])

# =========================
# ANÁLISIS
# =========================

# Casos por departamento
df_dept = df_clean.groupBy("Nombre departamento").count()

# Casos por estado
df_estado = df_clean.groupBy("Estado").count()

# Mostrar resultados
print("=== POR DEPARTAMENTO ===")
df_dept.show()

print("=== POR ESTADO ===")
df_estado.show()

# Guardar resultados
df_dept.coalesce(1).write.csv(
    "/home/vboxuser/tarea3_spark/output/departamentos",
    header=True,
    mode="overwrite"
)

df_estado.coalesce(1).write.csv(
    "/home/vboxuser/tarea3_spark/output/estado",
    header=True,
    mode="overwrite"
)

print("Proceso finalizado correctamente")

spark.stop()
