# Databricks notebook source
from pyspark.sql.functions import col,lower,trim,split,explode,regexp_replace

df = spark.read.format("text").load("abfss://training@trainingspark.dfs.core.windows.net/README.md")
#display(df)


df_count = df.select(explode(split(col('value'), r'\W+')).alias('word')) \
             .withColumn('word_clean', lower(trim(col('word')))) \
             .filter(col('word_clean').contains('spark')) \
             .count()
              
              
display(df_count)
              

# COMMAND ----------

from pyspark.sql.functions import col, lower, size, split, sum
 
df_count = df.withColumn("spark_count", size(split(lower(col("value")), "spark")) - 1)
 
total_spark = df_count.agg(sum("spark_count").alias("total_spark"))
 
total_spark.show()

# COMMAND ----------

from pyspark.sql.functions import col, lit, length, lower, replace

word = "spark"
word_length = len(word)

df_spark = df.withColumn(
    "spark_count",
    (length(lower(col("value"))) - length(replace(lower(col("value")), lit(word), lit("")))) / word_length
)

display(df_spark)
df_spark_count=df_spark.agg(sum("spark_count").alias("total_spark"))
display(df_spark_count)

# COMMAND ----------

from pyspark.sql.functions import col, lower, length, replace, lit
 
word = "spark"
word_length = len(word)
 
df_spark = df.withColumn(
    "spark_count",
    (length(lower(col("value"))) - length(replace(lower(col("value")), lit("spark"), lit("")))) / word_length
)
df_spark.show()
 
df_spark.agg(sum("spark_count").alias("total_spark")).show()