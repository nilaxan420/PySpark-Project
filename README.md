# PySpark Project Tutorial

This tutorial provides a step-by-step guide on how to perform a PySpark project using a Google Play Store dataset. The goal is to perform data analysis and develop business insights from the dataset.

## Project Overview

The project involves working with a Google Play Store dataset. The goal is to perform data analysis and develop business insights from the dataset. The approach follows a real-time work scenario, which includes having an agenda and insights to find out.

## Data Overview

The dataset includes information about different apps, their ratings, reviews, installs, type (free or paid), and more. The dataset is not in a proper format and needs cleaning.

## Data Cleaning

PySpark is used to clean the dataset. The necessary libraries are imported and a data frame is created to read the data. Unnecessary columns are dropped and the data is converted into a proper format using PySpark functions.

Here's a sample code snippet for data cleaning:

```python
# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create SparkSession
spark = SparkSession.builder.getOrCreate()

# Read data
df = spark.read.format('csv').option('header', 'true').load('GooglePlayStore.csv')

# Drop unnecessary columns
df = df.drop('Size', 'Content Rating', 'Last Updated', 'Current Ver', 'Android Ver')

# Convert data types
df = df.withColumn('Reviews', col('Reviews').cast('integer'))
df = df.withColumn('Installs', regexp_replace('Installs', '\\D+', '').cast('integer'))
df = df.withColumn('Price', regexp_replace('Price', '\\D+', '').cast('double'))
```

## Data Analysis

After cleaning the data, various analyses are performed. They include finding out the top 10 reviewed apps, the top 10 installed apps, and the distribution of apps by category. The top paid apps are also identified. SQL queries are used to perform these analyses.

Here's a sample code snippet for data analysis:

```python
# Register DataFrame as SQL temporary view
df.createOrReplaceTempView("apps")

# Perform SQL queries
top_reviewed_apps = spark.sql("SELECT App, SUM(Reviews) FROM apps GROUP BY App ORDER BY 2 DESC")
top_installed_apps = spark.sql("SELECT App, SUM(Installs) FROM apps GROUP BY App ORDER BY 2 DESC")
app_category_distribution = spark.sql("SELECT Category, SUM(Installs) FROM apps GROUP BY Category ORDER BY 2 DESC")
top_paid_apps = spark.sql("SELECT App, SUM(Price) FROM apps WHERE Type = 'Paid' GROUP BY App ORDER BY 2 DESC")
```

 
