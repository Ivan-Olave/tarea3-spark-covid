from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Creamos sesión Spark
spark = SparkSession.builder \
    .appName("COVID desde HDFS") \
    .getOrCreate()

# Ruta en HDFS 
file_path = "hdfs://localhost:9000/Tarea31/covid19.csv"

# Leemos archivo
df = spark.read.csv(file_path, header=True, inferSchema=True)

print("=== ESTRUCTURA ===")
df.printSchema()

print("=== DATOS ===")
df.show(5)

# Seleccionamos las columnas necesarias
df_clean = df.select("Nombre departamento", "Estado") \
             .dropna(subset=["Nombre departamento", "Estado"])

# Análisis
print("=== CASOS POR DEPARTAMENTO ===")
df_dept = df_clean.groupBy("Nombre departamento").count()
df_dept.show()

print("=== CASOS POR ESTADO ===")
df_estado = df_clean.groupBy("Estado").count()
df_estado.show()

# Guardamos resultados (opcional)
df_dept.coalesce(1).write.csv(
    "/home/vboxuser/tarea3_spark/output/hdfs_departamentos",
    header=True,
    mode="overwrite"
)

print("Proceso finalizado correctamente")

spark.stop()
