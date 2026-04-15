# Análisis de Datos COVID-19 con Apache Spark

## Descripción

Este proyecto realiza el procesamiento de un dataset de COVID-19 en Colombia utilizando Apache Spark en modo batch.

Se implementan procesos de limpieza, transformación y análisis exploratorio de datos (EDA), permitiendo obtener información relevante como el número de casos por departamento y estado.

## Tecnologías utilizadas

- Apache Spark
- Python (PySpark)
- Hadoop HDFS
- Linux (Ubuntu)

## Estructura del proyecto

1. tarea3_spark/
1.1.  data/
	1.1.1.  covid19.csv
1.2. scripts/
        1.2.2.  batch_covid.py
1.3.  output/

## Requisitos

- Apache Spark instalado
- Python 3
- Hadoop

## Ejecución

1. Ubicar el archivo covid19.csv en la ruta:

/home/vboxuser/tarea3_spark/data/

2. Ejecutar el script:

spark-submit scripts/batch_covid.py

3. Ver resultados en:

/home/vboxuser/tarea3_spark/output/

## Resultados

- Conteo de casos por departamento
- Conteo de casos por estado

## Autor

Iván Andrés Olave Ramírez
