# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # From R or Python to R *and* Python

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's start by creating an R dataframe called **my_data**

# COMMAND ----------

my_data <- data.frame(
  Col1 = c(1:10), 
  Col2 = c(rep("A", 3), rep("B", 3), rep("C", 4))
)

my_data

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Check out Databricks' native visualization capabilities

# COMMAND ----------

display(my_data)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Converting R dataframe to Spark DataFrame

# COMMAND ----------

library(SparkR)

my_data_sp <- SparkR::as.DataFrame(my_data)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Write Spark DataFrame to temp table that can be queried via SQL

# COMMAND ----------

SparkR::createOrReplaceTempView(my_data_sp, "my_data_tmp")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Query Temp Table using SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM my_data_tmp
# MAGIC WHERE Col1 > 5

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Execute another Databricks workbook from within this workbook

# COMMAND ----------

# MAGIC %run ./workbook2
