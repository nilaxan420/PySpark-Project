# Databricks notebook source
# DBTITLE 1,Import Library
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Create dataframe

df = spark.read.load('/FileStore/tables/googleplaystore.csv', format='csv', sep=',', header='true', escape='"', inferschema='true')

# COMMAND ----------

df.count()

# COMMAND ----------

df.show(3)

# COMMAND ----------

# DBTITLE 1,Check Schema
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Data Cleaning 
df=df.drop("Size", "Content Rating", "Last Updated","Current Ver", "Android Ver")

# COMMAND ----------

# DBTITLE 1,Check data after cleaning
df.show(3)

# COMMAND ----------

# DBTITLE 1,Check data schema after cleaning
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Correct Data Format
from pyspark.sql.functions import regexp_replace, col 

df=df.withColumn("Reviews", col("Reviews").cast(IntegerType()))\
.withColumn("Installs", regexp_replace(col("Installs"), "[^0-9]", ""))\
.withColumn("Installs", col("Installs").cast(IntegerType()))\
.withColumn("Price", regexp_replace(col("Price"), "[$]",""))\
.withColumn("Price", col("Price").cast(IntegerType()))

# COMMAND ----------

# DBTITLE 1,Print Schema after Data Format Transformation
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Display Data after format change
df.show(5)

# COMMAND ----------

# DBTITLE 1,Creating a view based on our data
df.createOrReplaceTempView("apps")

# COMMAND ----------

# DBTITLE 1,Selecting our data from created view
# MAGIC %sql select * from apps

# COMMAND ----------

# DBTITLE 1,Top Reviews
# MAGIC %sql select App, sum(Reviews) from apps
# MAGIC group by 1
# MAGIC order by 2 desc

# COMMAND ----------

# DBTITLE 1,Top 10 Installed Apps
# MAGIC %sql select App, Type, sum(Installs) from apps
# MAGIC group by 1, 2
# MAGIC order by 3 desc

# COMMAND ----------

# DBTITLE 1,Category Distribution
# MAGIC %sql select Category, sum(Installs) from apps
# MAGIC group by 1
# MAGIC order by 2 desc

# COMMAND ----------

# DBTITLE 1,Top Paid App
# MAGIC %sql select App, sum(Price) from apps
# MAGIC where Type='Paid'
# MAGIC group by 1
# MAGIC order by 2 desc

# COMMAND ----------


