# Databricks notebook source
#Importing Required Libaries
 
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg,count,desc,length,when,col

# COMMAND ----------

#Creating DF1 By my Amazon.csv

df1 = spark.read.format("csv").option("header", "true").load("/Volumes/workspace/default/amazon/amazon.csv")

# COMMAND ----------

#Display in Spark
df1.display()

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

#To select only the product name

df1.select('product_name').display()

# COMMAND ----------

from pyspark.sql.functions import col, avg, desc, regexp_extract

# Keep only rows where 'rating' is a valid number (integer or decimal)
df1_clean = df1.filter(
    regexp_extract(col('rating'), r'^(\d+(\.\d+)?|\.\d+)$', 0) != ""
)

top_rated_product = (
    df1_clean.groupBy('product_id', 'product_name')
    .agg(avg(col('rating').cast('double')).alias('avg_rating'))
    .orderBy(desc('avg_rating'))
    .limit(10)
)

display(top_rated_product)

# COMMAND ----------

#Most Reviewed Product

most_reviewed_product=df1.groupBy('product_id','product_name').count().orderBy(desc('count')).limit(10)
most_reviewed_product.display()

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace, avg, expr

discount_analysis = (
    df1.withColumn(
        "discount_percentage_clean",
        expr("try_cast(regexp_replace(discount_percentage, '%', '') as double)")
    )
    .groupBy("category")
    .agg(avg("discount_percentage_clean").alias("avg_discount"))
)

display(discount_analysis)

# COMMAND ----------

from pyspark.sql.functions import col, regexp_extract, avg, count

# Filter rows where rating is a valid number (integer or decimal)
df1_clean = df1.filter(
    regexp_extract(col('rating'), r'^(\d+(\.\d+)?|\.\d+)$', 0) != ""
)

user_engagement = (
    df1_clean.groupBy('product_id')
    .agg(
        avg(col('rating').cast('double')).alias('avg_rating'),
        count('rating').alias('rating_count')
    )
)

display(user_engagement)

# COMMAND ----------

#creating Temp Table from DF1

df1.createOrReplaceTempView('amazon_sales_table')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from amazon_sales_table order by product_id desc limit 10

# COMMAND ----------

